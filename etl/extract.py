#!/usr/bin/env python3
"""Streaming extractor – **v3** (2025-06-24)

Now reads a YAML mapping and only yields the mapped columns,
and can include raw count URIs based on `mode`.
API changed: extract(Path, mapping, mode) -> iterable of {column, value}.
"""
from __future__ import annotations
import re
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, Set

from etl.utils.misc import create_timestamped_filename, print_and_log

def _yield_dicts(column: str, values: Iterable[Any]) -> Generator[Dict[str, Any], None, None]:
    """Yield the minimal dict shape expected by harmonizer."""
    for v in values:
        yield {"column": column, "value": v}


def extract(path: Path,
            mapping: dict,
            skip_zarr_datasets: Set[str] = None,
            skip_tsv_columns: Set[str] = None,
            mode: str = "metadata"  # one of 'metadata', 'counts', 'both'
           ) -> Generator[Dict[str, Any], None, None]:
    """
    Streams rows from .zarr or tab-separated files as {column, value},
    but only for columns defined in mapping['columns'].
    If mode includes 'counts', yields a zarr_uri (raw_counts_uri) entry pointing to the file.
    """
    skip_zarr_datasets = skip_zarr_datasets or {"X", "counts"}
    skip_tsv_columns   = skip_tsv_columns   or set()

    suffix = path.suffix.lower()

    logfile = create_timestamped_filename("../extract_logs")
    
    # ───────────────────────────── ZARR ──────────────────────────────
    if suffix == ".zarr":
        import zarr
        root = zarr.open(path, mode="r")

        # metadata only or both
        if mode in ("metadata", "both"):
            # ---- 1. Variable/feature table ----
            var_group = root["var"]
            for var_key in var_group.array_keys():
                map_key = f"var.{var_key}"
                print_and_log(f"\t\t{path}: {map_key}", add_timestamp=False, logfile_path=logfile, collapse_size=0)
                if map_key not in mapping["columns"]:
                    continue
                col = var_group[var_key][:]
                print_and_log(f"[extract] {path.name} var → {var_key}", logfile_path=logfile)
                yield from _yield_dicts(var_key, col)

            # ---- 2. Observation/sample metadata ----
            obs_group = root["obs"]
            for obs_key in obs_group.array_keys():
                if obs_key in skip_zarr_datasets:
                    continue
                map_key = f"obs.{obs_key}"
                if map_key not in mapping["columns"]:
                    continue
                col = obs_group[obs_key][:]
                print_and_log(f"[extract] {path.name} obs → {obs_key}", logfile_path=logfile)
                yield from _yield_dicts(obs_key, col)

                # counts only or both
        if mode in ("counts", "both"):
            # yield URI for each sample's raw counts
            obs_group = root["obs"]
            if "sample_id" in obs_group.array_keys():
                sample_ids = obs_group["sample_id"][:]
                for sid in sample_ids:
                    uri = f"{path}#obs/{sid}"
                    yield {"column": "zarr_uri", "value": uri} # TODO rename zarr_uri to raw_counts_uri

    # ───────────────────────────── TSV / TXT ────────────────────────────── / TXT ──────────────────────────────
    elif suffix in {".tsv", ".txt"}:
        import pandas as pd

        # skip raw/counts files
        if any(k in path.name.lower() for k in ("raw", "count", "idf", "raw_counts")):
            return

        # metadata only or both
        if mode in ("metadata", "both"):
            df = pd.read_csv(path, sep="\t", dtype=str, engine="python", on_bad_lines="skip")
            for col in df.columns:
                if col in skip_tsv_columns:
                    continue
                map_key = f"{path.stem}.{col}"
                if map_key not in mapping["columns"]:
                    continue
                print_and_log(f"[extract] {path.name} tsv → {col}", logfile_path=logfile)
                yield from _yield_dicts(col, df[col].values)
        # raw count not supported

    # ───────────────────────────── Unsupported ──────────────────────────────
    else:
        return
