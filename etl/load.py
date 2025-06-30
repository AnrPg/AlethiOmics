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
from concurrent.futures import ThreadPoolExecutor
from errno import errorcode
import json
import logging
import os
import queue
import re
from sqlite3 import IntegrityError
import threading
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
import time
from typing import Dict, Iterable, List, Sequence, Tuple

import concurrent
import mysql.connector
from mysql.connector import pooling

CONNECTION_TIMEOUT=10 # TODO: get this value from public config file
LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s %(name)s: %(message)s",
)
_samples_lock = threading.Lock()
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
        # pool_size: int = 4,
        batch_size: int = 1_000,
        parallel_workers: int = 4,
        autocommit: bool = False,
    ) -> None:
        
        LOGGER.debug(
            "Initializing MySQLLoader(host=%s, port=%s, db=%s, user=%s, batch_size=%d, workers=%d, autocommit=%s)",
            host, port, database, user, batch_size, parallel_workers, autocommit
        )

        self.batch_size = min(batch_size, self._MAX_BATCH)
        self.parallel_workers = max(1, parallel_workers)
        self._column_cache: Dict[str, List[str]] = {}
        self._stats: Dict[str, int] = defaultdict(int)

        self._db_config = dict(
            host=host,
            port=3306, # TODO: check why this fixed the problems with the database and parametrize it!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            database=database,
            user=user,
            password=password,
            charset="utf8mb4",
            connection_timeout=CONNECTION_TIMEOUT, # seconds to establish TCP + handshake
            autocommit=autocommit,
        )
        # connection pool
        # self._pool_args = dict( #mysql.connector.pooling.MySQLConnectionPool
        #     pool_name="etl_pool",
        #     pool_size=1,
        #     host=host,
        #     port=port,
        #     database=database,
        #     user=user,
        #     password=password,
        #     charset="utf8mb4",
        #     connection_timeout=CONNECTION_TIMEOUT,
        #     autocommit=autocommit,
        # )
        # self._pool = None
        LOGGER.debug("Database config set: %s", self._db_config)
        
        # Work‑queue & threads
        self._q: queue.Queue[Tuple[str, List[Dict]]] = queue.Queue()
        self._threads: List[threading.Thread] = []
        for idx in range(self.parallel_workers):
            t = threading.Thread(target=self._worker, name=f"loader‑{idx+1}")
            t.daemon = True
            t.start()
            self._threads.append(t)
            LOGGER.debug("Started worker thread: %s", t.name)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    
    # def _get_pool(self):
    #     if self._pool is None:
    #         self._pool = pooling.MySQLConnectionPool(**self._pool_args)
    #     return self._pool

    def enqueue(self, table: str, rows: List[Dict]) -> None:
        """Put a `(table, rows)` job into the queue (returns immediately)."""
        if not rows:
            return
        LOGGER.debug("Queued %d rows → %s", len(rows), table)
        # Chop huge payloads to respect _MAX_BATCH
        for i in range(0, len(rows), self.batch_size):
            batch = rows[i : i + self.batch_size]
            LOGGER.debug(f" enqueue: putting batch of {len(batch)} rows → {table}\n\t{batch}")
            self._q.put((table, batch))

    def flush(self) -> Dict[str, int]:
        """Block until the queue empties, then return insert statistics."""
        LOGGER.debug(" flush: waiting for all enqueued batches to finish…")
        LOGGER.debug(f">>> flush: unfinished tasks -> {self._q.unfinished_tasks}")
        self._q.join()  # wait for tasks
        LOGGER.debug(f" flush: done. insert statistics: {dict(self._stats)}")
        return dict(self._stats)

    # ------------------------------------------------------------------
    # Worker loop
    # ------------------------------------------------------------------
    def _worker(self) -> None:
        while True:
            LOGGER.debug("Queue size before get(): %d", self._q.qsize())
            table, rows = self._q.get()
            LOGGER.debug(f" worker: [{threading.current_thread().name}] picked up {len(rows)} rows → {table}")
            try:
                # run the batch insert with a hard timeout
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    fut = pool.submit(self._insert_batch, table, rows)
                    inserted = fut.result(timeout=30)   # bail out after 30s
                self._stats[table] += inserted

                LOGGER.debug(f" worker: [{threading.current_thread().name}] inserted {inserted} rows into {table}")
            except TimeoutError:
                LOGGER.error("⏱  Insert batch timed out: %s rows → %s", len(rows), table)
            except Exception as exc:
                LOGGER.exception("‼️  Failed batch (%s rows) → %s: %s", len(rows), table, exc)
            finally:
                self._q.task_done()
                LOGGER.debug("Queue size after task_done(): %d", self._q.qsize())

    # ------------------------------------------------------------------
    # SQL helpers
    # ------------------------------------------------------------------

    def _insert_batch(self, table: str, rows: List[Dict]) -> int:
        """Insert a batch; returns affected row-count (committed)."""
        LOGGER.debug(f" _insert_batch: [{threading.current_thread().name}] _insert_batch START → {len(rows)} rows into {table}")
        start = time.time()

        cols = self._table_columns(table)
        keys = sorted({k for row in rows for k in row.keys() if k in cols})
        if not keys:
            raise ValueError(f"None of the provided columns match target table '{table}'.")

        placeholders = ", ".join(["%s"] * len(keys))
        sql = f"INSERT INTO {table} ({', '.join(keys)}) VALUES ({placeholders})"
        values = [tuple(row.get(k) for k in keys) for row in rows]

        LOGGER.debug("  samples keys to insert: %r", keys)
        LOGGER.debug("  samples values: %r", values)


        affected = 0
        max_retries = 3

        def _retry_loop():
            nonlocal affected
            for attempt in range(1, max_retries + 1):
                conn = mysql.connector.connect(**self._db_config)
                try:
                    with self._tx(conn):
                        cur = conn.cursor()
                        # Insert per-row to catch duplicates
                        inserted_this_tx = 0
                        for vals in values:
                            try:
                                cur.execute(sql, vals)
                                # count only actual inserts
                                if cur.rowcount:
                                    inserted_this_tx += 1
                            except IntegrityError as e:
                                # existing duplicate logic
                                if e.errno == errorcode.ER_DUP_ENTRY:
                                    m = re.search(r"Duplicate entry '(.+)' for key '(.+)'", e.msg)
                                    if m:
                                        dup_val, dup_key = m.groups()
                                        LOGGER.warning(
                                            "[%s] duplicate key %r=%r on row %s — skipping",
                                            table, dup_key, dup_val, vals
                                        )
                                    else:
                                        LOGGER.warning(
                                            "[%s] duplicate entry error on row %s: %s — skipping",
                                            table, vals, e.msg
                                        )
                                    continue
                                else:
                                    raise
                        affected = inserted_this_tx
                    LOGGER.debug(" %s ← %d rows (cols=%d)", table, affected, len(keys))
                    LOGGER.debug(f" _insert_batch: [{threading.current_thread().name}] _insert_batch COMMIT for {table} ({len(rows)} rows) in {time.time()-start:.2f}s")
                    # only break if we inserted at least one row or this was last attempt
                    # for Studies we don’t expect 100% new rows every batch,
                    # so treat zero-rows as success and stop retrying
                    if table == "Studies":
                        break
                    # for all other tables, break on real progress or final attempt
                    if inserted_this_tx > 0 or attempt == max_retries:
                        break  # success or give up after last retry
                    # otherwise, treat as deadlock-like and retry
                    raise mysql.connector.Error(
                        f"Deadlock-like: 0 rows on attempt {attempt}, retrying",
                        errno=1213
                    )
                except mysql.connector.Error as err:
                    # Deadlock: try again
                    if err.errno == 1213 and attempt < max_retries:
                        wait = 0.1 * attempt
                        LOGGER.warning(
                            "Deadlock on %s (attempt %d/%d), retrying after %.2fs",
                            table, attempt, max_retries, wait
                        )
                        time.sleep(wait)
                        continue
                    # other errors or max retries reached
                    LOGGER.error(f"DB error inserting into {table}:\nrows attempted to be inserted -> {values}\n for columns -> {', '.join(keys)}\nError:\t{err}")
                    break
                finally:
                    conn.close()
        if table == "Samples":
            with _samples_lock:
                _retry_loop()
        else:
            _retry_loop()
            
        # # ── STUBBED INSERT FOR DEBUG ───────────────────────────────
        # import time
        # LOGGER.debug(f" \t\t!!!!_insert_batch: [{threading.current_thread().name}] sleeping instead of inserting {len(rows)} rows into {table}")
        # time.sleep(0.01)
        # return len(rows)
        # # ── END STUB ───────────────────────────────────────────────


    # ------------------------------------------------------------------
    # Caching
    # ------------------------------------------------------------------
    
    def _table_columns(self, table: str) -> List[str]:
        if table in self._column_cache:
            return self._column_cache[table]

        conn = mysql.connector.connect(**self._db_config)
        # pool = self._get_pool()
        # conn = pool.get_connection() # or conn = self._pool.get_connection()
        
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
