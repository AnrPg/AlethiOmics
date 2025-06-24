#!/usr/bin/env python3
"""
Harmonizer v2.2 – mapping.yml–driven, with fallback APIs for ontology and taxonomy lookups.
2025-06-23
"""

import re
import yaml
import requests
import xmltodict
from pathlib import Path
from typing import Any, Dict, Optional

# ─── Core ontology & taxonomy helpers ───────────────────────────────────────────

PREFIX_TO_IRI = {
    "CL":        "http://purl.obolibrary.org/obo/CL_",
    "EFO":       "http://www.ebi.ac.uk/efo/EFO_",
    "NCBITaxon": "http://purl.obolibrary.org/obo/NCBITaxon_",
    "UBERON":    "http://purl.obolibrary.org/obo/UBERON_",
    "PATO":      "http://purl.obolibrary.org/obo/PATO_",
    "CHEBI":     "http://purl.obolibrary.org/obo/CHEBI_",
    "HANCESTRO": "http://purl.obolibrary.org/obo/HANCESTRO_",
    "MONDO":     "http://purl.obolibrary.org/obo/MONDO_",
    "HsapDv":    "http://purl.obolibrary.org/obo/HsapDv_",
}

TIMEOUT = 5  # HTTP timeout


def normalize_ontology_id(id_str: str) -> Optional[Dict[str,str]]:
    """Given a CURIE or full IRI, return standardized iri+curie+prefix or None."""
    if id_str.startswith("http"):
        iri = id_str
        for pfx, base in PREFIX_TO_IRI.items():
            if iri.startswith(base):
                return {"iri": iri, "curie": f"{pfx}:{iri[len(base):]}", "prefix": pfx}
        return None
    
    if ":" in id_str:
        pfx, local = id_str.split(":", 1)
        base = PREFIX_TO_IRI.get(pfx)
        if not base:
            return None
        return {"iri": base, "curie": id_str, "prefix": pfx}
    return None

# ─── Fallback taxonomy rank via NCBI ─────────────────────────────────────────

def ncbi_get_rank(taxon_id: str) -> Optional[str]:
    """Fetch taxonomic rank from NCBI taxonomy API."""
    try:
        url = f"https://api.ncbi.nlm.nih.gov/taxonomy/v0/id/{taxon_id}?format=json"
        r = requests.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        return data.get('rank')
    except Exception:
        return None

# ─── Transform functions registry ────────────────────────────────────────────

def strip_version(val: str) -> str:
    return re.sub(r"\.\d+$", "", val)

def canonical_iri(val: str) -> Optional[str]:
    gv = strip_version(val)
    return f"http://identifiers.org/ensembl/{gv}"

def get_local_link(val: str) -> str:
    return f"/samples/{val}"

def get_iri(val: str) -> Optional[str]:
    norm = normalize_ontology_id(val)
    return norm["iri"] if norm else None

def get_name(val: str) -> Optional[str]:
    """Fetch term label from OLS4, fallback to Ontobee."""
    norm = normalize_ontology_id(val)
    if not norm:
        return None
    prefix = norm['prefix'].lower()
    curie = norm['curie']
    # primary: OLS4
    try:
        url = f"https://www.ebi.ac.uk/ols4/api/ontologies/{prefix}/terms?obo_id={curie}"
        r = requests.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        terms = r.json().get("_embedded", {}).get("terms", [])
        if terms and terms[0].get("label"):
            return terms[0]["label"]
    except Exception:
        pass
    # fallback: Ontobee SPARQL
    try:
        sparql = (
            f"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
            f" SELECT ?label WHERE {{ <{norm['iri']}> rdfs:label ?label }}"
        )
        ob_url = "https://www.ontobee.org/sparql"
        r2 = requests.get(ob_url, params={'query': sparql, 'output': 'json'}, timeout=TIMEOUT)
        r2.raise_for_status()
        bindings = r2.json().get('results', {}).get('bindings', [])
        if bindings:
            return bindings[0]['label']['value']
    except Exception:
        pass
    return None

def get_chem_class(val: str) -> Optional[str]:
    """Fetch CHEBI classification via OLS4 annotation, fallback to XML service."""
    norm = normalize_ontology_id(val)
    if norm:
        try:
            url = f"https://www.ebi.ac.uk/ols4/api/ontologies/{norm['prefix'].lower()}/terms?obo_id={norm['curie']}"
            r = requests.get(url, timeout=TIMEOUT)
            r.raise_for_status()
            ann = r.json().get("_embedded", {}).get("terms", [])[0].get("annotation", {})
            if ann.get('chebi_class'):
                return ann['chebi_class'][0]
        except Exception:
            pass
    try:
        chebi_id = val.split(':',1)[1]
        url = f"https://www.ebi.ac.uk/webservices/chebi/2.0/test/getCompleteEntity?chebiId={chebi_id}&format=xml"
        r2 = requests.get(url, timeout=TIMEOUT)
        r2.raise_for_status()
        doc = xmltodict.parse(r2.text)
        return doc['S:Envelope']['S:Body']["getCompleteEntityResponse"]["return"]["chebiAsciiName"].strip()
    except Exception:
        return None

def get_ranking(val: str) -> Optional[str]:
    """Query taxonomy rank via OLS4, fallback to NCBI taxonomy API."""
    norm = normalize_ontology_id(val)
    if not norm:
        return None
    prefix = norm['prefix'].lower()
    curie = norm['curie']
    # primary: OLS4 annotation 'has_rank'
    try:
        url = f"https://www.ebi.ac.uk/ols4/api/ontologies/{prefix}/terms?obo_id={curie}"
        r = requests.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        ann = r.json().get("_embedded", {}).get("terms", [])[0].get("annotation", {})
        if ann.get('has_rank'):
            rank = ann['has_rank']
            return rank[0] if isinstance(rank, list) else rank
    except Exception:
        pass
    # fallback: NCBI taxonomy
    if norm['prefix'] == 'NCBITaxon':
        taxid = norm['curie'].split(':',1)[1]
        return ncbi_get_rank(taxid)
    return None

def get_ontology(val: str) -> Optional[str]:
    norm = normalize_ontology_id(val)
    return norm['prefix'] if norm else None

TRANSFORM_FUNCS: Dict[str, Any] = {
    "strip_version": strip_version,
    "canonical_iri": canonical_iri,
    "get_local_link": get_local_link,
    "get_iri": get_iri,
    "get_name": get_name,
    "get_chem_class": get_chem_class,
    "get_ranking": get_ranking,
    "get_ontology": get_ontology,
}

# ─── Mapping‐driven harmonization ────────────────────────────────────────────

def load_mapping(path:Path="config/features.yml") -> Dict[str,Any]:
    with open(path, "r") as fh:
        return yaml.safe_load(fh)

# def harmonize(item: Dict[str,Any], mapping: Dict[str,Any]) -> Optional[Dict[str,Any]]:
#     col, val = item["column"], item["value"]
#     candidates = [k for k in mapping["columns"] if k.endswith(f".{col}")]
#     if len(candidates) != 1:
#         return None
#     entry = mapping["columns"][candidates[0]]
#     out = val
#     for t in entry.get("transforms", []):
#         fn = TRANSFORM_FUNCS.get(t)
#         if not fn:
#             raise KeyError(f"Unknown transform '{t}'")
#         out = fn(out)
#     return {"table": entry["target_table"], "column": entry["target_column"], "value": out}

def harmonize(item_or_list: Any, mapping: Dict[str,Any]) -> Dict[tuple, set]:
   """
   Accepts a single {column,value} or a list thereof,
   applies transforms, and returns a dict:
     { (table, column): set(values) }
   """
   # normalize to list
   items = item_or_list if isinstance(item_or_list, list) else [item_or_list]
   from collections import defaultdict
   grouped: Dict[tuple, set] = defaultdict(set)
   for item in items:
       col = item.get("column")
       val = item.get("value")
       if col is None:
           continue
       # find the mapping entry
       candidates = [k for k in mapping["columns"] if k.endswith(f".{col}")]
       if len(candidates) != 1:
           continue
       entry = mapping["columns"][candidates[0]]
       # apply transforms in order
       out = val
       for tname in entry.get("transforms", []):
           fn = TRANSFORM_FUNCS.get(tname)
           if fn:
               out = fn(out)
       # accumulate by (table, column)
       key = (entry["target_table"], entry["target_column"])
       grouped[key].add(out)
   return grouped

# ─── CLI harness ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys, json
    if len(sys.argv) < 2:
        print("Usage: harmonize.py <mapping.yml>")
        sys.exit(1)
    mapping = load_mapping(Path(sys.argv[1]))
    for line in sys.stdin:
        item = json.loads(line)
        out = harmonize(item, mapping)
        if out:
            print(json.dumps(out))
