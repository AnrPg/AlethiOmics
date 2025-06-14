from pathlib import Path
from typing import Dict, Generator, Any, Iterable

CHUNK = 10000               # adjust to the largest batch the RAM will tolerate

def _yield_dicts(column: str, values: Iterable[Any]) -> Generator[Dict[str, Any], None, None]:
    """Tiny helper so we don’t repeat the yield block two dozen times."""
    for v in values:
        yield {"column": column, "value": v}

def extract(path: Path, mapping) -> Generator[Dict[str, Any], None, None]:
    """
    Streams rows from .zarr or tab-separated files as {"column": …, "value": …} dicts.

    Parameters
    ----------
    path : Path
        File or directory to read.
    mapping : Any
        Still unused here; left in the signature so callers don’t break.
    """
    suffix = path.suffix.lower()

    # ───────────────────────────── Zarr ──────────────────────────────
    
    if suffix == ".zarr":
        import zarr

        root = zarr.open(path, mode="r")

        # ---- 1. Gene IDs  (root['var'] is the variable/feature table) ----
        # Many Scanpy-written Zarrs store the IDs in 'gene_id' or '_index'.
        gene_ds_name = "gene_id" if "gene_id" in root["var"] else "_index"
        gene_ids = root["var"][gene_ds_name]          # zarr.Array

        # Stream in CHUNK-sized slabs; zarr slices are lazy (no RAM spike).
        for start in range(0, gene_ids.shape[0], CHUNK):
            for row in _yield_dicts("gene_id", gene_ids[start:start + CHUNK]):
                yield row

        # ---- 2. Sample-level metadata (obs) ----
        obs_table = root["obs"]                      # zarr.legacy.core.Group
        for obs_key in obs_table.array_keys():
            col = obs_table[obs_key][:]
            for row in _yield_dicts(obs_key, col):
                yield row

    # ───────────────────────────── TSV / TXT ──────────────────────────────
    elif suffix in {".tsv", ".txt"}:
        import pandas as pd

        df = pd.read_csv(path, sep="\t")
        for col in df:
            for row in _yield_dicts(col, df[col].values):
                yield row
