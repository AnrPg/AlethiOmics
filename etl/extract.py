#!/usr/bin/env python3
from pathlib import Path
from typing import Dict, Generator, Any, Optional
import zarr
import yaml

# adjust to the largest batch the RAM will tolerate
CHUNK = 10000

# Load feature configuration
CONFIG_PATH = Path("config/features.yml")
config = yaml.safe_load(CONFIG_PATH.read_text())
FEATURE_META = set(config.get('meta', []))
FEATURE_GENES = set(config.get('genes', []))

# Define which obs fields map to Samples columns (extendable via config)
OBS_TO_SAMPLE = {
    "barcodekey": "sample_id",
    "cell_type_ontology_term_id": "cell_type_iri",
    "cell_type": "cell_type_label",
    "tissue_ontology_term_id": "tissue_iri",
    "tissue": "tissue_label",
    "organism_ontology_term_id": "organism_iri",
    "organism": "organism_label",
    "growth_condition": "growth_condition",
    # 'stimulus' and 'microbe' handled separately
}


def extract(path: Path,
            mapping: Dict[str, Dict[str, Any]],
            skip_zarr: Optional[set] = None
           ) -> Generator[Dict[str, Any], None, None]:
    """
    Stream structured rows for each table based on config/features.yml.
    mapping provides foreign-key values for stimuli, microbes, studies, etc.
    """
    skip_zarr = skip_zarr or {"X", "counts"}
    root = zarr.open(path, mode="r")

    # 1. Extract Genes
    var_group = root["var"]
    # Identify gene ID array from var (e.g. gene_id, feature_name)
    gene_keys = [k for k in var_group.array_keys() if k in FEATURE_GENES or 'id' in k]
    if not gene_keys:
        raise ValueError("No gene identifier found in var group.")
    gene_ds = var_group[gene_keys[0]]

    for start in range(0, gene_ds.shape[0], CHUNK):
        chunk = gene_ds[start:start + CHUNK]
        for gene in chunk:
            row = {"table": "Genes", "gene_id": str(gene)}
            # filter by config
            yield {k: v for k, v in row.items() if k in FEATURE_GENES or k == 'table'}

    # 2. Extract Samples
    obs = root["obs"]
    sample_ids = obs["barcodekey"][:]
    num = sample_ids.shape[0]
    study_id = mapping.get("study_id")

    for i in range(num):
        row: Dict[str, Any] = {"table": "Samples"}
        # static fields
        row["sample_id"] = str(sample_ids[i])
        row["study_id"] = study_id
        row["zarr_uri"] = str(path)
        # dynamic obs fields
        for obs_key, col in OBS_TO_SAMPLE.items():
            if obs_key in obs.array_keys():
                val = obs[obs_key][:][i]
                row[col] = str(val)
        # stimulus mapping
        if "stimulus" in obs.array_keys():
            raw = str(obs["stimulus"][i])
            row["stimulus_id"] = mapping.get("stimuli", {}).get(raw)
        # microbe mapping
        if "microbe" in obs.array_keys():
            raw = str(obs["microbe"][i])
            row["microbe_id"] = mapping.get("microbes", {}).get(raw)
        # filter by config
        yield {k: v for k, v in row.items() if k in FEATURE_META or k == 'table'}

    # 3. Extract ExpressionStats (if present arrays)
    # expects arrays 'log2_fc', 'p_value', 'significance' aligned to var x obs
    if all(name in root for name in ['obs', 'var', 'layers']):
        log2 = root['layers']['log2_fc']
        pval = root['layers']['p_value']
        sig = root['layers']['significance']
        for i in range(log2.shape[0]):
            for j in range(log2.shape[1]):
                row = {
                    "table": "ExpressionStats",
                    "sample_id": str(sample_ids[j]),
                    "gene_id": str(var_group[gene_keys[0]][i]),
                    "log2_fc": float(log2[i, j]),
                    "p_value": float(pval[i, j]),
                    "significance": str(sig[i, j]),
                }
                yield {k: v for k, v in row.items() if k in FEATURE_META or k in ('table',)}
