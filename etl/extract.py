#!/usr/bin/env python3
"""Streaming extractor that emits only the features required by the
MySQL transactional schema, as declared in *feature_mapping.yml* (or an
in‑memory dict).  Any Zarr dataset or TSV column **not** referenced in the
mapping is silently skipped.

Returned rows are simple ``{"column": <str>, "value": <Any>}`` dicts that
feed the downstream harmoniser.
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Generator, Any, Iterable, Tuple, Set, Union
import types
import yaml  # PyYAML – already a dependency elsewhere in the repo

CHUNK = 10_000  # tune to available memory

# ───────────────────────────── helpers ─────────────────────────────

def _yield_dicts(column: str, values: Iterable[Any]) -> Generator[Dict[str, Any], None, None]:
    """Yield the (column, value) pairs one‑by‑one as tiny dicts."""
    for v in values:
        yield {"column": column, "value": v}


def _parse_mapping_columns(mapping: Union[Path, str, Dict[str, Any]]) -> Tuple[Set[str], Set[str], Set[str]]:
    """Return three *allowed* sets: (obs_keys, var_keys, tsv_columns).

    * ``obs_keys`` – keys inside ``root['obs']`` datasets of AnnData Zarr.
    * ``var_keys`` – keys inside ``root['var']`` (feature metadata) datasets.
    * ``tsv_columns`` – DataFrame columns allowed when reading .tsv/.txt
      (the raw column names **after** any explicit prefix, e.g. ``Gene ID``).
    """

    # -------- 1) Load mapping YAML to dict if necessary --------
    if isinstance(mapping, (str, Path)):
        mapping_path = Path(mapping)
        with mapping_path.open("r", encoding="utf-8") as fh:
            mapping_dict = yaml.safe_load(fh)
    elif isinstance(mapping, dict):
        mapping_dict = mapping
    else:
        raise TypeError("'mapping' must be a path, str, or dict")

    raw_keys = set(mapping_dict.get("columns", {}).keys())

    # -------- 2) Derive *unqualified* dataset/column names --------
    obs_keys: Set[str] = set()
    var_keys: Set[str] = set()
    tsv_keys: Set[str] = set()

    TSV_PREFIXES = ("counts.", "sdrf.", "idf.", "uns.")

    for k in raw_keys:
        # strip optional inline comments like " (distinct)"
        k = k.split("(")[0].strip()  # e.g. "sdrf.stimulus_iri (distinct)" -> "sdrf.stimulus_iri"

        if "." in k:
            prefix, name = k.split(".", 1)
            if prefix == "obs":
                obs_keys.add(name)
            elif prefix == "var":
                var_keys.add(name)
            elif k.startswith(TSV_PREFIXES):
                tsv_keys.add(name)
        else:
            # plain key (rare) – treat as TSV column name
            tsv_keys.add(k)

    return obs_keys, var_keys, tsv_keys

# ───────────────────────────── main extractor ─────────────────────────────

def extract(
    path: Path,
    mapping: Union[Path, str, Dict[str, Any]],
    *,
    skip_zarr_datasets: Set[str] | None = None,
    skip_tsv_columns: Set[str] | None = None,
) -> Generator[Dict[str, Any], None, None]:
    """Stream records from *path* while ignoring everything not present in
    the mapping catalogue.

    Parameters
    ----------
    path
        Input file (.zarr, .tsv, .txt).
    mapping
        Either (a) a *dict* already parsed from YAML **or** (b) a *Path/str*
        to a ``feature_mapping.yml`` file.
    skip_zarr_datasets
        Extra dataset names (within ``root['obs']``) that should *always* be
        skipped (defaults to {"X", "counts"}).
    skip_tsv_columns
        Extra DataFrame column names that should *always* be skipped.
    """

    obs_allowed, var_allowed, tsv_allowed = _parse_mapping_columns(mapping)

    # merge caller‑provided skip masks
    skip_zarr = set(skip_zarr_datasets or {"X", "counts"})
    skip_tsv = set(skip_tsv_columns or set())

    suffix = path.suffix.lower()

    # ───────────────────────────── Zarr ──────────────────────────────
    if suffix == ".zarr":
        import zarr

        root = zarr.open(path, mode="r")

        # ---- 1. Gene / feature metadata (root['var']) ----
        var_group = root["var"]
        for key in var_group.array_keys():
            if key not in var_allowed:
                continue  # skip non‑mapped var datasets
            array = var_group[key]
            for start in range(0, len(array), CHUNK):
                for row in _yield_dicts(key, array[start : start + CHUNK]):
                    yield row

        # ---- 2. Sample‑level metadata (root['obs']) ----
        obs_table = root["obs"]
        for obs_key in obs_table.array_keys():
            if obs_key in skip_zarr or obs_key not in obs_allowed:
                continue
            col = obs_table[obs_key][:]
            for row in _yield_dicts(obs_key, col):
                yield row

        # (Root['X'] and other numeric matrices are intentionally ignored.)

    # ───────────────────────────── TSV / TXT ──────────────────────────
    elif suffix in {".tsv", ".txt"}:
        import pandas as pd

        # When the file name suggests raw read counts, leave decision to mapping
        df = pd.read_csv(path, sep="\t")

        for col in df.columns:
            if col in skip_tsv or col not in tsv_allowed:
                continue
            for row in _yield_dicts(col, df[col].values):
                yield row

    # ───────────────────────────── Unsupported ────────────────────────
    else:
        raise ValueError(f"Unsupported file type: {suffix!r} for {path!r}")
