#!/usr/bin/env python3

"""
Orchestrates the full ETL workflow for the Gut–Brain Organoid DW:

  1. (optional) Generate synthetic data with synthetic_data_generator.py
  2. Discover + extract raw files
  3. Harmonise rows according to mapping.yml
  4. Stream them into the live MySQL DB via an SSH tunnel

Logs go both to screen and to  logs/pipeline_<timestamp>.log

example of CLI run:
`
python main.py\
    --batch-size 16\
    --use-synthetic --synthetic-params --foo bar --baz quux \
    --num_experiments 8 --seed 20250624 --out_dir raw_data/synthetic_runs
`
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import List

from sshtunnel import SSHTunnelForwarder

from etl.extract import Extractor
from etl.harmonize import Harmonizer
from etl.load import MySQLLoader
from etl.utils.log import configure_logging, get_logger

# ───────────────────────────────────────────────────────────────────────────
#  Helpers
# ───────────────────────────────────────────────────────────────────────────

def _run_synthetic_generator(
    data_dir: Path,
    synthetic_params: List[str],
    num_experiments: int,
    seed: int,
    out_dir: str
) -> None:
    """
    Launch the synthetic-data generator as a module,
    so we don’t depend on the cwd or stray .py files.
    """
    cmd = [
        sys.executable,
        "-m", "etl.utils.synthetic_data_generator",
        "--num_experiments", str(num_experiments),
        "--seed", str(seed),
        "--out_dir", str(out_dir),
    ]
    if synthetic_params:
        cmd += synthetic_params

    logger = get_logger(__name__)
    # Debug: where are we, and what does Python see?
    logger.debug("cwd: %s", os.getcwd())
    logger.debug("sys.path: %s", sys.path)
    logger.info("🧪  Running synthetic generator: %s", " ".join(cmd))
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        # Log the stderr from the subprocess to diagnose import/path issues
        logger.error("Synthetic generator failed (exit %d)", e.returncode)
        logger.error("STDOUT:\n%s", e.stdout)
        logger.error("STDERR:\n%s", e.stderr)
        raise

# ───────────────────────────────────────────────────────────────────────────
#  Main driver
# ───────────────────────────────────────────────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser(description="Gut–Brain DW – full ETL pipeline")
    
    ap.add_argument(
        "--data-dir",
        default="raw_data",
        help="Directory with raw *.tsv/*.zarr files (created if --use-synthetic)",
    )
    ap.add_argument(
        "--batch-size",
        type=int,
        default=1_000,
        help="Rows per batch for both Extractor and Loader.",
    )
    ap.add_argument(
        "--mode",
        choices=("all", "metadata", "raw_counts"),
        default="all",
        help="Extractor mode (pass-through to etl.extract).",
    )
    # Synthetic data generation ------------------------------------------------
    ap.add_argument(
        "--use-synthetic",
        action="store_true",
        help="Call synthetic_data_generator before the ETL run.",
    )
    ap.add_argument(
        "--synthetic-params",
        nargs="*",
        metavar="PARAM",
        help="Extra flags forwarded verbatim to the synthetic-data generator.",
    )
    ap.add_argument(
        "--num_experiments",
        type=int,
        default=1,
        help="Number of synthetic experiments to generate.",
    )
    ap.add_argument(
        "--seed",
        type=int,
        default=int(datetime.now().strftime("%Y%m%d")),
        help="Random seed for synthetic-data generation.",
    )
    ap.add_argument(
        "--out_dir",
        default="raw_data/synthetic_runs",
        help="Output directory for synthetic runs.",
    )
    # MySQL / SSH credentials --------------------------------------------------
    ap.add_argument("--ssh-host", default="devanr.tenant-a9.svc.cluster.local")
    ap.add_argument("--ssh-user", default="anr")
    ap.add_argument(
        "--ssh-key-path",
        default="/home/rodochrousbisbiki/.ssh/id_ed25519",
        help="Private key for the bastion/edge node.",
    )
    ap.add_argument("--remote-host", default="127.0.0.1")
    ap.add_argument("--remote-port", type=int, default=3306)

    ap.add_argument("--mysql-user", default=os.getenv("MYSQL_USER", "MasterLogariasmos"))
    ap.add_argument("--mysql-password", default=os.getenv("MYSQL_PASS", "1234"))
    ap.add_argument("--mysql-db", default=os.getenv("MYSQL_DB", "alethiomics_live"))

    ap.add_argument(
        "--mapping-yaml", default="mapping.yml",
        help="Column-mapping file for the Harmonizer.",
    )
    args = ap.parse_args()

    # 1️⃣  Logging: console + rotating file
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = Path("logs") / f"pipeline_{ts}.log"
    log_path.parent.mkdir(exist_ok=True)
    configure_logging(log_path)
    logger = get_logger(__name__)
    logger.info("🚀  ETL pipeline started")

    data_dir = Path(args.data_dir).expanduser().resolve()

    # 2️⃣  Synthetic data (optional)
    if args.use_synthetic:
        if data_dir.exists():
            logger.warning("Overwriting existing synthetic folder %s", data_dir)
        # Debug: show how args are being passed through
        logger.debug("synthetic-params: %r", args.synthetic_params)
        logger.debug("num_experiments: %d, seed: %d, out_dir: %s",
                     args.num_experiments, args.seed, args.out_dir)
        _run_synthetic_generator(
            data_dir,
            args.synthetic_params or [],
            args.num_experiments,
            args.seed,
            args.out_dir,
        )


    if not data_dir.exists():
        logger.error("Data directory %s does not exist – aborting.", data_dir)
        sys.exit(1)

    # 3️⃣  SSH tunnel → cluster MySQL
    logger.info("🔐  Opening SSH tunnel %s → %s:%s",
                args.ssh_host, args.remote_host, args.remote_port)
    tunnel = SSHTunnelForwarder(
        (args.ssh_host, 22),
        ssh_username=args.ssh_user,
        ssh_pkey=args.ssh_key_path,
        remote_bind_address=(args.remote_host, args.remote_port),
        local_bind_address=("127.0.0.1",),
    )
    tunnel.start()
    local_port = tunnel.local_bind_port
    logger.info("🛡️   Tunnel established on localhost:%s", local_port)

    # 4️⃣  Instantiate ETL stages
    extractor = Extractor(data_dir, mode=args.mode, batch_size=args.batch_size)
    harmonizer = Harmonizer(args.mapping_yaml)
    loader = MySQLLoader(
        host="127.0.0.1",
        port=local_port,
        database=args.mysql_db,
        user=args.mysql_user,
        password=args.mysql_password,
        batch_size=args.batch_size,
        parallel_workers=4,
    )

    # 5️⃣  Stream pipeline
    row_total = 0
    try:
        for table, batch in extractor.iter_batches():
            harmonised = harmonizer.apply(table, batch)
            loader.enqueue(table, harmonised)
            row_total += len(harmonised)
        stats = loader.flush()
        logger.info("🎉  ETL finished – %d rows processed.", row_total)
        logger.info("📊  Insert summary: %s", stats)
    finally:
        tunnel.stop()
        logger.info("🔒  SSH tunnel closed")


if __name__ == "__main__":
    main()
