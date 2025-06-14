#!/usr/bin/env python3

def extract(path, mapping):
    if path.suffix == ".zarr":
        import zarr
        annotated_data = zarr.open(path, mode="r")
        # Yield rows as dicts; keys will be matched by regex later
        for key, col in annotated_data.var.items():               # genes
            yield {"column": "gene_id", "value": key}
        for obs_key in annotated_data.obs.columns:                # sample meta
            for val in annotated_data.obs[obs_key]:
                yield {"column": obs_key, "value": val}
    elif path.suffix in {".tsv", ".txt"}:
        import pandas as pd
        df = pd.read_csv(path, sep="\t")
        for col in df:
            for val in df[col]:
                yield {"column": col, "value": val}
