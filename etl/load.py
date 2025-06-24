#!/usr/bin/env python3
"""etl.load
-------------------
Highly‑optimised loader that streams harmonised batches into the live
MySQL database defined by *db_schema.sql*.

Goals
-----
* **Transactions** – every batch is an atomic `INSERT`; rolls back on errors.
* **Batch / Parameterised** – `executemany()` with one round‑trip per batch.
* **Parallelism** – optional thread‑pool to saturate the network / I/O.
* **Schema‑aware** – fetches `INFORMATION_SCHEMA` once per table and
  auto‑aligns rows → columns (extra keys are dropped, absent → NULL).
* **Verbose logging** – DEBUG traces for every major step, concise INFO summary.

Typical use from the ETL pipeline::

    loader = MySQLLoader(**cfg.mysql)
    for table, batch in harmonizer.apply(...):
        loader.enqueue(table, batch)      # non‑blocking
    loader.flush()                        # wait + final stats

A CLI wrapper is provided for ad‑hoc testing::

    python -m etl.load \
        --host localhost --database gutbrain_dw \
        --user dw_user --password secret \
        harmonised.jsonl
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import queue
import threading
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

import mysql.connector
from mysql.connector import pooling

LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s %(name)s: %(message)s",
)

# ---------------------------------------------------------------------------
# Helper – resolve ENV/CLI credentials
# ---------------------------------------------------------------------------

def _env_default(key: str, default: str | None = None) -> str | None:
    return os.getenv(key, default)


# ---------------------------------------------------------------------------
# Core loader
# ---------------------------------------------------------------------------
class MySQLLoader:
    """Thread‑safe batch loader with connection pooling."""

    #: sanity cap – never try to insert >this rows at once (to avoid OOM)
    _MAX_BATCH = 50_000

    def __init__(
        self,
        host: str = "localhost",
        port: int = 3306,
        database: str = "gutbrain_dw",
        user: str = "root",
        password: str | None = None,
        pool_size: int = 8,
        batch_size: int = 1_000,
        parallel_workers: int = 4,
        autocommit: bool = False,
    ) -> None:
        self.batch_size = min(batch_size, self._MAX_BATCH)
        self.parallel_workers = max(1, parallel_workers)
        self._column_cache: Dict[str, List[str]] = {}
        self._stats: Dict[str, int] = defaultdict(int)

        # connection pool
        self._pool = pooling.MySQLConnectionPool(
            pool_name="etl_pool",
            pool_size=pool_size,
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            charset="utf8mb4",
            autocommit=autocommit,  # we manage commit() ourselves
        )

        # Work‑queue & threads
        self._q: queue.Queue[Tuple[str, List[Dict]]] = queue.Queue()
        self._threads: List[threading.Thread] = []
        for idx in range(self.parallel_workers):
            t = threading.Thread(target=self._worker, name=f"loader‑{idx+1}")
            t.daemon = True
            t.start()
            self._threads.append(t)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def enqueue(self, table: str, rows: List[Dict]) -> None:
        """Put a `(table, rows)` job into the queue (returns immediately)."""
        if not rows:
            return
        LOGGER.debug("Queued %d rows → %s", len(rows), table)
        # Chop huge payloads to respect _MAX_BATCH
        for i in range(0, len(rows), self.batch_size):
            self._q.put((table, rows[i : i + self.batch_size]))

    def flush(self) -> Dict[str, int]:
        """Block until the queue empties, then return insert statistics."""
        self._q.join()  # wait for tasks
        return dict(self._stats)

    # ------------------------------------------------------------------
    # Worker loop
    # ------------------------------------------------------------------
    def _worker(self) -> None:
        while True:
            table, rows = self._q.get()
            try:
                inserted = self._insert_batch(table, rows)
                self._stats[table] += inserted
            except Exception as exc:
                LOGGER.exception("‼️  Failed batch (%s rows) → %s: %s", len(rows), table, exc)
            finally:
                self._q.task_done()

    # ------------------------------------------------------------------
    # SQL helpers
    # ------------------------------------------------------------------
    def _insert_batch(self, table: str, rows: List[Dict]) -> int:
        """Insert a batch; returns affected row‑count (committed)."""
        cols = self._table_columns(table)
        keys = sorted({k for row in rows for k in row.keys() if k in cols})
        if not keys:
            raise ValueError(f"None of the provided columns match target table '{table}'.")

        placeholders = ", ".join(["%s"] * len(keys))
        sql = f"INSERT INTO {table} ({', '.join(keys)}) VALUES ({placeholders})"
        values = [tuple(row.get(k) for k in keys) for row in rows]

        conn = self._pool.get_connection()
        try:
            with self._tx(conn):
                cur = conn.cursor()
                cur.executemany(sql, values)
                affected = cur.rowcount
            LOGGER.debug("✅  %s ← %d rows (cols=%d)", table, affected, len(keys))
            return affected
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Caching
    # ------------------------------------------------------------------
    def _table_columns(self, table: str) -> List[str]:
        if table in self._column_cache:
            return self._column_cache[table]

        conn = self._pool.get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                   WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s""",
                (conn.database, table),
            )
            cols = [r[0] for r in cur.fetchall()]
            if not cols:
                raise ValueError(f"Unknown table '{table}' – no columns found.")
            self._column_cache[table] = cols
            LOGGER.debug("Discovered %d columns for %s: %s", len(cols), table, cols)
            return cols
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Tx helper
    # ------------------------------------------------------------------
    @contextmanager
    def _tx(self, conn):
        try:
            conn.start_transaction()
            yield
            conn.commit()
        except Exception:
            conn.rollback()
            raise


# ---------------------------------------------------------------------------
# CLI – accepts newline‑delimited JSON records: {"table": "Genes", "row": {...}}
# ---------------------------------------------------------------------------

def _cli() -> None:
    p = argparse.ArgumentParser(description="Stream harmonised JSON → MySQL")
    p.add_argument("input", type=Path, help="JSONL file with {table, row} objects")
    p.add_argument("--host", default=_env_default("MYSQL_HOST", "localhost"))
    p.add_argument("--port", type=int, default=int(_env_default("MYSQL_PORT", "3306")))
    p.add_argument("--database", default=_env_default("MYSQL_DB", "gutbrain_dw"))
    p.add_argument("--user", default=_env_default("MYSQL_USER", "root"))
    p.add_argument("--password", default=_env_default("MYSQL_PWD", ""))
    p.add_argument("--batch-size", type=int, default=1_000)
    p.add_argument("--workers", type=int, default=4)
    args = p.parse_args()

    loader = MySQLLoader(
        host=args.host,
        port=args.port,
        database=args.database,
        user=args.user,
        password=args.password,
        batch_size=args.batch_size,
        parallel_workers=args.workers,
    )

    # Stream file → batches per table (local grouping for efficiency)
    current_table = None
    buffer: List[Dict] = []

    def _flush():
        nonlocal buffer, current_table
        if buffer:
            loader.enqueue(current_table, buffer)
            buffer = []

    with args.input.open() as fh:
        for line in fh:
            if not line.strip():
                continue
            obj = json.loads(line)
            tbl = obj["table"]
            row = obj["row"]
            if current_table is None:
                current_table = tbl
            if tbl != current_table or len(buffer) >= loader.batch_size:
                _flush()
                current_table = tbl
            buffer.append(row)
    _flush()

    stats = loader.flush()
    LOGGER.info("📈  Insert summary: %s", stats)


if __name__ == "__main__":
    _cli()
