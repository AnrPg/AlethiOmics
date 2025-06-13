#!/usr/bin/env python3

import re
import requests
from typing import Optional, Dict

# --- Mapping from prefix to IRI base ---
PREFIX_TO_IRI = {
    "CL": "http://purl.obolibrary.org/obo/CL_",
    "EFO": "http://www.ebi.ac.uk/efo/EFO_",
    "NCBITaxon": "http://purl.obolibrary.org/obo/NCBITaxon_",
    "UBERON": "http://purl.obolibrary.org/obo/UBERON_",
    "PATO": "http://purl.obolibrary.org/obo/PATO_",
    "CHEBI": "http://purl.obolibrary.org/obo/CHEBI_",
    "HANCESTRO": "http://purl.obolibrary.org/obo/HANCESTRO_",
    "MONDO": "http://purl.obolibrary.org/obo/MONDO_",
    "HsapDv": "http://purl.obolibrary.org/obo/HsapDv_"
}

# --- Convert CURIE to IRI ---
def curie_to_iri(curie: str) -> Optional[str]:
    if ":" not in curie:
        return None
    prefix, local_id = curie.split(":")
    base = PREFIX_TO_IRI.get(prefix)
    return base + local_id if base else None

# --- Convert IRI to CURIE ---
def iri_to_curie(iri: str) -> Optional[str]:
    for prefix, base in PREFIX_TO_IRI.items():
        if iri.startswith(base):
            return f"{prefix}:{iri[len(base):]}"
    return None

# --- Normalize any ontology ID (CURIE or IRI) ---
def normalize_ontology_id(id_str: str) -> Optional[Dict[str, str]]:
    if id_str.startswith("http"):
        iri = id_str
        curie = iri_to_curie(iri)
    elif ":" in id_str:
        curie = id_str
        iri = curie_to_iri(curie)
    else:
        return None

    if not iri or not curie:
        return None

    prefix = curie.split(":")[0]
    
    # --- DEBUG logs onto screen ------------------------------------------------------------------------------
    print(f"[INFO] Normalizing ontology ID: {id_str} (IRI: {iri}, CURIE: {curie}, Prefix: {prefix})")
    # -----------------------------------------------------------------------------------------------------------
    
    return {"iri": iri, "curie": curie, "ontology_prefix": prefix}

# --- Enrichment via OLS API (OntoBee alternative possible too) ---
def enrich_ontology_term(id_str: str) -> Optional[Dict[str, Optional[str]]]:
    norm = normalize_ontology_id(id_str)
    if norm is None:
        return None

    prefix = norm["ontology_prefix"].lower()
    local_id = norm["curie"].split(":")[1].replace("_", ":")
    
    # --- DEBUG logs onto screen ---------------------------------------------------------------
    print(f"[INFO] Enriching ontology term: {id_str} (prefix: {prefix}, local_id: {local_id})")
    # -------------------------------------------------------------------------------------------
    
    # OLS uses lowercase prefixes and colons
    url = f"https://www.ebi.ac.uk/ols/api/ontologies/{prefix}/terms?obo_id={norm['curie']}"
    
    # --- DEBUG logs onto screen ---------------------------
    print(f"[DEBUG] OLS API URL: {url}")
    # ------------------------------------------------------
    
    try:
        r = requests.get(url, timeout=5)

        # --- DEBUG logs onto screen ------------------------------------------------------
        print(f"[DEBUG] OLS API response status: {r.status_code}")
        if r.status_code == 404:
            print(f"[WARN] Term not found: {id_str}")
            return norm | {"name": None, "definition": None}
        elif r.status_code != 200:
            print(f"[ERROR] OLS API error for {id_str}: {r.status_code} - {r.text}")
            return norm | {"name": None, "definition": None}
        # ---------------------------------------------------------------------------------
        r.raise_for_status()
        results = r.json()["_embedded"]["terms"]
        if not results:
            return norm | {"name": None, "definition": None}

        term = results[0]
        name = term.get("label")
        definition = term.get("obo_definition_citation")
        if isinstance(definition, list):
            definition = definition[0]

        return norm | {"name": name, "definition": definition}

    except Exception as e:
        print(f"[WARN] Failed to enrich {id_str}: {e}")
        return norm | {"name": None, "definition": None}


# def curie_to_iri(curie: str) -> str:
#     prefix_map = {
#         "CL": "http://purl.obolibrary.org/obo/CL_",
#         "EFO": "http://www.ebi.ac.uk/efo/EFO_",
#         "NCBITaxon": "http://purl.obolibrary.org/obo/NCBITaxon_",
#         # Add more as needed
#     }
#     if ":" not in curie:
#         return curie  # already an IRI or malformed
#     prefix, local_id = curie.split(":")
#     return prefix_map.get(prefix, prefix + ":") + local_id

# # Example with AnnData object
# import scanpy as sc

# adata = sc.read_h5ad("your_data.h5ad")

# # Transform the CURIEs in place
# adata.obs["cell_type_ontology_term_iri"] = adata.obs["cell_type_ontology_term_id"].apply(curie_to_iri)

# # Now ready to insert into MySQL live DB
