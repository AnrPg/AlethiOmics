#!/usr/bin/env python3

from pathlib import Path
from typing import Dict, Generator, Any, Iterable

CHUNK = 10000               # adjust to the largest batch the RAM will tolerate

def _yield_dicts(column: str, values: Iterable[Any]) -> Generator[Dict[str, Any], None, None]:
    """Tiny helper so we don’t repeat the yield block two dozen times."""
    for v in values:
        yield {"column": column, "value": v}

def extract(path: Path,
            mapping,
            skip_zarr_datasets: set = None,
            skip_tsv_columns: set = None
           ) -> Generator[Dict[str, Any], None, None]:
    """
    Streams rows from .zarr or tab-separated files as {"column": …, "value": …} dicts,
    but skips any raw-counts arrays or columns.
    """
    suffix = path.suffix.lower()
    skip_zarr_datasets = skip_zarr_datasets or {"X", "counts"}
    skip_tsv_columns   = skip_tsv_columns   or set()
    
    # ───────────────────────────── Zarr ──────────────────────────────
    if suffix == ".zarr":
        import zarr
        root = zarr.open(path, mode="r")

        # ---- 1. Gene IDs  (root['var'] is the variable/feature table) ----
        var_group = root["var"]
        keys = list(var_group.array_keys())
        # auto-detect object-dtype arrays for gene IDs
        obj_keys = [k for k in keys if var_group[k].dtype.kind in ("U", "O")]
        if len(obj_keys) == 1:
            gene_ds_name = obj_keys[0]
        elif "feature_name" in keys:
            gene_ds_name = "feature_name"
        elif "ensembl_id" in keys:
            gene_ds_name = "ensembl_id"
        else:
            gene_ds_name = keys[0]
            print(f"[warning] using '{gene_ds_name}' for gene IDs")

        gene_ids = var_group[gene_ds_name]
        for start in range(0, gene_ids.shape[0], CHUNK):
            for row in _yield_dicts("gene_id", gene_ids[start:start + CHUNK]):
                yield row

        # 2. Sample-level metadata (obs)
        obs_table = root["obs"]
        for obs_key in obs_table.array_keys():
            if obs_key in skip_zarr_datasets:
                continue
            col = obs_table[obs_key][:]
            for row in _yield_dicts(obs_key, col):
                yield row

        # (we do *not* touch root["X"] or any other datasets in the Zarr)

    # ───────────────────────────── TSV / TXT ──────────────────────────────
    elif suffix in {".tsv", ".txt"}:
        import pandas as pd

        # if the filename suggests raw/counts, skip the entire file
        if any(k in path.name.lower() for k in ("raw", "count", "idf", "raw_counts")):
            return

        df = pd.read_csv(path, sep="\t")

        for col in df:
            if col in skip_tsv_columns:
                continue
            for row in _yield_dicts(col, df[col].values):
                yield row
    # ───────────────────────────── Unsupported ──────────────────────────────
    else:
        # raise ValueError(f"Unsupported file type: {suffix!r} for {path!r}")
        pass
