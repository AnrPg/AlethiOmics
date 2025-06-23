#!/usr/bin/env python3
"""Streaming extractor – **v2** (2025-06-23)

Now reads a YAML mapping and only yields the mapped columns.
API unchanged: extract(Path, mapping) -> iterable of {column, value}.
"""
from __future__ import annotations
import re
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, Set

import yaml


def load_mapping(path: Path) -> dict:
    """Load the mapping.yml that defines which file/column pairs to extract."""
    with open(path, "r") as fh:
        return yaml.safe_load(fh)


def print_and_log(*args, **kwargs):
    """Stubbed logger; replace or extend as you like."""
    print(*args, **kwargs)


def _yield_dicts(column: str, values: Iterable[Any]) -> Generator[Dict[str, Any], None, None]:
    """Yield the minimal dict shape expected by harmonizer."""
    for v in values:
        yield {"column": column, "value": v}


def extract(path: Path,
            mapping: dict,
            skip_zarr_datasets: Set[str] = None,
            skip_tsv_columns: Set[str] = None
           ) -> Generator[Dict[str, Any], None, None]:
    """
    Streams rows from .zarr or tab-separated files as {"column":…, "value":…},
    but only for columns defined in mapping['columns'].
    """
    skip_zarr_datasets = skip_zarr_datasets or {"X", "counts"}
    skip_tsv_columns   = skip_tsv_columns   or set()

    suffix = path.suffix.lower()

    # ───────────────────────────── ZARR ──────────────────────────────
    if suffix == ".zarr":
        import zarr
        root = zarr.open(path, mode="r")

        # ---- 1. Variable/feature table ----
        var_group = root["var"]
        for var_key in var_group.array_keys():
            map_key = f"var.{var_key}"
            if map_key not in mapping["columns"]:
                continue
            col = var_group[var_key][:]
            print_and_log(f"[extract] {path.name} var→ {var_key}")
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
            print_and_log(f"[extract] {path.name} obs→ {obs_key}")
            yield from _yield_dicts(obs_key, col)

    # ───────────────────────────── TSV / TXT ──────────────────────────────
    elif suffix in {".tsv", ".txt"}:
        import pandas as pd

        # skip raw/counts files
        if any(k in path.name.lower() for k in ("raw", "count", "idf", "raw_counts")):
            return

        df = pd.read_csv(path, sep="\t", dtype=str, engine="python", on_bad_lines="skip")
        for col in df.columns:
            if col in skip_tsv_columns:
                continue
            map_key = f"{path.stem}.{col}"
            if map_key not in mapping["columns"]:
                continue
            print_and_log(f"[extract] {path.name} tsv→ {col}")
            yield from _yield_dicts(col, df[col].values)

    # ───────────────────────────── Unsupported ──────────────────────────────
    else:
        # silently ignore everything else
        return


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Usage: extract.py <mapping.yml> <path1> [<path2> …]")
        sys.exit(1)

    mapping_path = Path(sys.argv[1])
    mapping = load_mapping(mapping_path)

    for file_path in sys.argv[2:]:
        for item in extract(Path(file_path), mapping):
            # e.g. forward to harmonizer, or simply print
            print(item)
