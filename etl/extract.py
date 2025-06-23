#!/usr/bin/env python3
"""Streaming extractor – **v2** (2025‑06‑23)

Changes
=======
* Robust TSV/SDRF/IDF reader that loads **only mapped columns** with the
  *Python* engine + ``on_bad_lines='skip'`` – avoids Pandas ``ParserError``
  when rows have variable field counts.
* Uses ``usecols`` so off‑schema columns are never even parsed.
* Retains early‑exit for internal ``.zarr`` paths.
* API unchanged: ``extract(Path, mapping) -> iterable of {column, value}``.
"""
from __future__ import annotations

from pathlib import Path
import re
from typing import Dict, Generator, Any, Iterable, Tuple, Set, Union
import yaml

from etl.utils.misc import create_timestamped_filename, print_and_log  # PyYAML

CHUNK = 10_000  # rows per chunk when iterating arrays

# ───────────────────────────── helpers ─────────────────────────────

def _yield_dicts(column: str, values: Iterable[Any]) -> Generator[Dict[str, Any], None, None]:
    """Yield successive ``{"column": column, "value": v}`` dicts."""
    for v in values:
        yield {"column": column, "value": v}


def _parse_mapping_columns(mapping: Union[Path, str, Dict[str, Any]]) -> Tuple[Set[str], Set[str], Set[str]]:
    if isinstance(mapping, (str, Path)):
        with Path(mapping).open("r", encoding="utf-8") as fh:
            mapping_dict = yaml.safe_load(fh)
    elif isinstance(mapping, dict):
        mapping_dict = mapping
    else:
        raise TypeError("mapping must be Path | str | dict")

    raw_keys = set(mapping_dict.get("columns", {}))
    obs, var, tsv = set(), set(), set()
    TSV_PREFIXES = ("counts.", "sdrf.", "idf.", "uns.")

    for k in raw_keys:
        k = k.split("(")[0].strip()
        if "." in k:
            prefix, name = k.split(".", 1)
            if prefix == "obs":
                obs.add(name)
            elif prefix == "var":
                var.add(name)
            elif k.startswith(TSV_PREFIXES):
                tsv.add(name)
        else:
            tsv.add(k)
    return obs, var, tsv

# ───────────────────────────── main extractor ─────────────────────────────

def extract(
    path: Path,
    mapping: Union[Path, str, Dict[str, Any]],
    *,
    want: str = "all",                             # <— new: "all"|"meta"|"genes"
    skip_zarr_datasets: Set[str] | None = None,
    skip_tsv_columns: Set[str] | None = None,
) -> Generator[Dict[str, Any], None, None]:
    """Stream mapping‑relevant records from *path*.

    Handles `.zarr`, `.tsv`, `.txt`.  All other files are silently ignored.
    """
    logfile = create_timestamped_filename("./debug_logs")
    # Abort if we're inside a .zarr store (internal file)
    parts = path.parts
    if any(p.endswith(".zarr") for p in parts[:-1]):
        return  # skip silently

    obs_allowed, var_allowed, tsv_allowed = _parse_mapping_columns(mapping)

    skip_zarr = set(skip_zarr_datasets or {"X", "counts"})
    skip_tsv = set(skip_tsv_columns or set())

    suffix = path.suffix.lower()

    # ───────────────────────── Zarr ────────────────────────────
    if suffix == ".zarr":
        import zarr
        root = zarr.open(path, mode="r")

        # var (feature metadata)
        if want in ("all", "genes"):
            var_grp = root["var"]
            for key in var_grp.array_keys():
                print_and_log(f"Processing var key: {key}", add_timestamp=False, logfile_path=logfile, collapse_size=0)
                if key not in var_allowed:
                    print_and_log(f"\tSkipping var key: {key} (not in mapping)", add_timestamp=False, logfile_path=logfile, collapse_size=0)
                    continue
                arr = var_grp[key]
                for start in range(0, len(arr), CHUNK):
                    print_and_log(f"\tProcessing var chunk: {start} to {start + CHUNK}: {key} --> {arr[start : start + 10]}", add_timestamp=False, logfile_path=logfile, collapse_size=0)
                    yield from _yield_dicts(key, arr[start : start + CHUNK])

        # obs (sample metadata)
        if want in ("all", "meta"):
            obs_grp = root["obs"]
            if want in ("meta",) and re.search(r"/X/|counts|raw/", str(path)):
                return

            for key in obs_grp.array_keys():
                print_and_log(f"Processing obs key: {key}", add_timestamp=False, logfile_path=logfile, collapse_size=0)
                if key in skip_zarr or key not in obs_allowed:
                    print_and_log(f"\tSkipping obs key: {key} (not in mapping or skipped)", add_timestamp=False, logfile_path=logfile, collapse_size=0)
                    continue
                col = obs_grp[key][:]
                print_and_log(f"\tProcessing obs column: {key} with {len(col)} values: {col[:10]}", add_timestamp=False, logfile_path=logfile, collapse_size=0)
                yield from _yield_dicts(key, col)

    # ───────────────────────── TSV / TXT ───────────────────────
    elif suffix in {".tsv", ".txt"}:
        import pandas as pd

        # Prepare list of columns to load (intersection + honour skip list)
        allowed_cols = [c for c in tsv_allowed if c not in skip_tsv]
        if not allowed_cols:
            return  # nothing mapped from this file

        try:
            df = pd.read_csv(
                path,
                sep="\t",
                engine="python",          # tolerant to ragged rows
                on_bad_lines="skip",       # skip malformed rows silently
                usecols=lambda c: c in allowed_cols,
                comment="#",               # ignore metadata comment lines
                dtype=str,                 # keep as strings for harmoniser
            )
        except Exception as exc:
            # Log & skip unreadable file instead of crashing whole ETL
            print_and_log(f"[extract] WARN: could not parse {path}: {exc}; skipping file.", add_timestamp=False, logfile_path=logfile, collapse_size=0)
            return

        for col in df.columns:
            print_and_log(f"Processing column: {col} with {len(df[col])} values: {df[col].values[:10]}", add_timestamp=False, logfile_path=logfile, collapse_size=0)
            yield from _yield_dicts(col, df[col].values)

    # ───────────────────────── Other – ignore ──────────────────
    else:
        return  # silently skip unsupported file types
