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
import yaml

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
    out_dir: str,
    tz: str,
    ts_format: str,
    base_uri: str,
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
        "--tz", tz,
        "--ts-format", ts_format,
        "--base-uri", base_uri,
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

def load_config(path: Path) -> dict:
    """
    If `path` ends in .age, run `age --decrypt` (using $AGE_IDENTITY or default).
    Otherwise load plaintext YAML.
    """
    data = None
    if path.suffix == ".age":
        # determine identity file
        identity = os.environ.get("AGE_IDENTITY")  # e.g. /home/user/key.txt
        cmd = ["age", "--decrypt", str(path)]
        if identity:
            cmd += ["--identity", identity]
        # decrypt into memory
        proc = subprocess.run(cmd, check=True, stdout=subprocess.PIPE)
        data = proc.stdout.decode()
    else:
        data = path.read_text()
    return yaml.safe_load(data) or {}


# ───────────────────────────────────────────────────────────────────────────
#  Main driver
# ───────────────────────────────────────────────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser(description="Gut–Brain DW – full ETL pipeline")
    
    # allow pointing at a YAML config
    ap.add_argument(
        "--config", type=Path, default=Path("config.yml"),
        help="YAML config file (keys: base_uri, tz, ts_format, log_datefmt)"
    )
    ap.add_argument(
        "--sensitive-config", type=Path,
        default=Path(".config/sensitive_config.yml.age"),
        help="Path to encrypted sensitive YAML (age‐encrypted)"
    )


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
    ap.add_argument("--tz", "--timezone", dest="tz",
                    default="Europe/Athens",
                    help="IANA time-zone for both synthetic folder names and "
                         "log timestamps (default: %(default)s)")
    ap.add_argument("--ts-format",
                    default="%Y%m%d-%H%M%S",
                    help="strftime() pattern for experiment folder timestamp "
                         "(default: %(default)s)")
    ap.add_argument("--log-datefmt",
                    default="%Y-%m-%d %H:%M:%S",
                    help="strftime() pattern for timestamps inside the log "
                         "file (default: %(default)s)")
    ap.add_argument("--base-uri", dest="base_uri", default=None,
                    help="Base URI for all outputs (file://, s3://, gs://, etc.)")

    
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
    
    ap.add_argument("--ssh-host", default=None, help="Bastion SSH host (overrides sensitive-config)")
    ap.add_argument("--ssh-user", default=None, help="Bastion SSH user")
    ap.add_argument("--ssh-key-path", default=None, help="Private key for the bastion/edge node")
    ap.add_argument("--remote-host", default=None, help="Remote DB host (via bastion)")
    ap.add_argument("--remote-port", type=int, default=None, help="Remote DB port")
    ap.add_argument("--mysql-user", default=None, help="MySQL user")
    ap.add_argument("--mysql-password", default=None, help="MySQL password")
    ap.add_argument("--mysql-db", default=None, help="MySQL database name")
    ap.add_argument("--mapping-yaml", default=None, help="Column-mapping file (overrides public config db_mapping)")

    args = ap.parse_args()

    # ─────────── load public config ──────────────────────────────────────
    
    # --- Load YAML config and fill in any CLI‐unspecified values ---
    public_cfg = {}
    if args.config.exists():
        public_cfg = yaml.safe_load(args.config.read_text()) or {}
    
    # Fallback: pull every value from CLI args, else via YAML i.e. public_cfg.get(...), otherwise set to default values if possible
    args.tz            = args.tz            or public_cfg.get("tz")           or "Europe/Athens"
    args.ts_format     = args.ts_format     or public_cfg.get("ts_format")    or "%Y%m%d-%H%M%S"
    args.log_datefmt   = args.log_datefmt   or public_cfg.get("log_datefmt")  or "%Y-%m-%d at %H:%M:%S"
    args.base_uri      = args.base_uri      or public_cfg.get("base_uri")     or "file://./raw_data/synthetic_runs"
    args.mapping_yaml  = args.mapping_yaml  or public_cfg.get("db_mapping")   or "./.config/mapping_catalogue.yml"

    # ─────────── decrypt & load sensitive config ─────────────────────────
    
    sensitive_cfg = {}
    sc_path = args.sensitive_config
    if sc_path.suffix == ".age":
        cmd = ["age", "--decrypt", str(sc_path)]
        # allow pointing at your private key
        if env_id := os.environ.get("AGE_IDENTITY"):
            cmd += ["--identity", env_id]
        out = subprocess.run(cmd, check=True, stdout=subprocess.PIPE)
        sensitive_cfg = yaml.safe_load(out.stdout.decode()) or {}
    elif sc_path.exists():
        sensitive_cfg = yaml.safe_load(sc_path.read_text()) or {}

    # ─────────── populate args from CLI > sensitive ──────────────
    # SSH
    args.ssh_host      = args.ssh_host      or sensitive_cfg.get("ssh_host")
    args.ssh_user      = args.ssh_user      or sensitive_cfg.get("ssh_user")
    args.ssh_key_path  = args.ssh_key_path  or sensitive_cfg.get("ssh_key_path")
    args.remote_host   = args.remote_host   or sensitive_cfg.get("remote_host")
    args.remote_port   = args.remote_port   or sensitive_cfg.get("remote_port")

    # MySQL
    args.mysql_user     = args.mysql_user     or sensitive_cfg.get("mysql_user")
    args.mysql_password = args.mysql_password or sensitive_cfg.get("mysql_password")
    args.mysql_db       = args.mysql_db       or sensitive_cfg.get("mysql_db")

    # ----------------------------------------------------------------------------------------

    # 1️⃣  Logging: console + rotating file
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = Path("logs") / f"pipeline_{ts}.log"
    log_path.parent.mkdir(exist_ok=True)
    configure_logging(log_path, datefmt=args.log_datefmt, tz=args.tz)
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
        args.tz,
        args.ts_format,
        args.base_uri,
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
