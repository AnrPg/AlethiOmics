#!/usr/bin/env python3

import re
import requests
from typing import Optional, Dict
from urllib.parse import quote

# This dictionary maps ontology prefixes (e.g., "CL", "EFO") to the IRI base used to construct full URLs.
# It is essential for converting compact CURIEs like "CL:0000057" into their canonical IRI form.
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

# Converts a CURIE (like "CL:0000057") to its corresponding full IRI using the mapping above.
# Returns None if the CURIE is malformed or the prefix is unknown.
def curie_to_iri(curie: str) -> Optional[str]:
    if ":" not in curie:
        return None
    prefix, local_id = curie.split(":")
    base = PREFIX_TO_IRI.get(prefix)
    return base + local_id if base else None

# Converts a full IRI (like "http://purl.obolibrary.org/obo/CL_0000057") to its corresponding CURIE.
# This helps normalize identifiers and can be used to reverse earlier mappings.
def iri_to_curie(iri: str) -> Optional[str]:
    for prefix, base in PREFIX_TO_IRI.items():
        if iri.startswith(base):
            return f"{prefix}:{iri[len(base):]}"
    return None

# Takes any ontology identifier (either CURIE or IRI) and returns a normalized dictionary
# containing the canonical IRI, the CURIE form, and the ontology prefix.
# This step standardizes identifiers so later enrichment functions can treat them uniformly.
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
    return {"iri": iri, "curie": curie, "ontology_prefix": prefix}

# Queries the OLS (Ontology Lookup Service) API to get metadata (name, definition)
# for a given ontology term. The input is any string (CURIE or IRI), which is normalized internally.
# This is the primary enrichment source.
def fetch_from_ols(id_str: str) -> Optional[Dict[str, Optional[str]]]:
    norm = normalize_ontology_id(id_str)
    if norm is None:
        return None
    curie = norm["curie"]
    prefix = norm["ontology_prefix"]

    print(f"[OLS] Looking up {curie} from ontology '{prefix.lower()}'")

    # OLS expects lowercase ontology prefixes and uses the OBO ID (CURIE) as a parameter
    url = f"https://www.ebi.ac.uk/ols/api/ontologies/{prefix.lower()}/terms?obo_id={quote(curie)}"
    try:
        # Perform the HTTP request to fetch term info from OLS
        r = requests.get(url, timeout=5)
        r.raise_for_status()
        results = r.json().get("_embedded", {}).get("terms", [])
        if not results:
            print(f"[OLS] No result found for {curie}")
            return None

        # Extract term metadata from the first result (most relevant)
        term = results[0]
        name = term.get("label")
        # You asked for definition to be retrieved from the "obo_definition_citation" key instead of the "definition" key
        definition = term.get("obo_definition_citation")
        if isinstance(definition, list):
            definition = definition[0]

        print(f"[OLS] Success: {curie} → {name}")
        return {"name": name, "definition": definition}
    except Exception as e:
        print(f"[OLS ERROR] {curie}: {e}")
        return None

# If OLS fails, fallback to Ontobee by performing a SPARQL query to its public endpoint.
# This is useful for ontologies that OLS does not cover (e.g., HsapDv) or for edge cases.
def fetch_from_ontobee(iri: str) -> Optional[Dict[str, Optional[str]]]:
    print(f"[Ontobee] Trying SPARQL query for {iri}")

    # SPARQL query that asks for label and optional definition using standard RDF properties
    sparql = f"""
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?label ?def WHERE {{
      <{iri}> rdfs:label ?label .
      OPTIONAL {{ <{iri}> <http://purl.obolibrary.org/obo/IAO_0000115> ?def . }}
    }}
    LIMIT 1
    """
    try:
        # Send the query to Ontobee's SPARQL endpoint
        r = requests.get("http://www.ontobee.org/sparql", params={"query": sparql, "format": "json"}, timeout=5)
        r.raise_for_status()
        bindings = r.json()["results"]["bindings"]
        if not bindings:
            print(f"[Ontobee] No bindings returned for {iri}")
            return None

        # Extract label and definition if available
        label = bindings[0]["label"]["value"]
        definition = bindings[0].get("def", {}).get("value", None)

        print(f"[Ontobee] Success: {iri} → {label}")
        return {"name": label, "definition": definition}
    except Exception as e:
        print(f"[Ontobee ERROR] {iri}: {e}")
        return None

# High-level function that accepts any ontology ID and attempts to enrich it using:
# 1. OLS API (preferred source)
# 2. Ontobee (fallback if OLS fails)
# Always returns a full dictionary with 'iri', 'curie', 'ontology_prefix', and possibly 'name', 'definition'
def enrich_ontology_term(id_str: str) -> Optional[Dict[str, Optional[str]]]:
    norm = normalize_ontology_id(id_str)
    if norm is None:
        print(f"[enrich] Could not normalize: {id_str}")
        return None

    print(f"[enrich] Normalized: {norm}")

    # Try primary source: OLS
    enriched = fetch_from_ols(id_str)
    if enriched:
        return norm | enriched

    # If OLS failed, try Ontobee instead
    enriched = fetch_from_ontobee(norm["iri"])
    return norm | (enriched if enriched else {"name": None, "definition": None})

# canonical_ols_iri: calls enrich_ontology_term (to make sure that this ontology exists online) but keeps and returns only the iri