#!/usr/bin/env python3

import re, requests, xml.etree.ElementTree as ET
from typing import Optional, Dict, Callable
from urllib.parse import quote
import requests
from typing import Optional, Dict
from typing import Any, Dict, Optional

from etl.utils.preprocessing import lowercase_ascii
# from functools import reduce


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


TIMEOUT = 10  # seconds

# ---------------------------------------------------------
# --------------- Harmonization of Gene IDs ---------------
# ---------------------------------------------------------

# --- Regex-based gene ID format detection ---
def detect_gene_id_format(gene_id: str) -> str:
    """
    Detects the format of a gene ID.
    Returns one of: 'ensembl', 'entrez', 'symbol', 'unknown'
    """
    if re.match(r'^ENS[A-Z]{0,3}G\d{6,}$', gene_id):
        return 'ensembl'
    elif re.match(r'^\d+$', gene_id):
        return 'entrez'
    elif re.match(r'^[A-Z0-9\-]+$', gene_id):
        return 'symbol'
    else:
        return 'unknown'

# --- Harmonizer to Ensembl using MyGene.info API ---
def harmonize_to_ensembl(gene_id: str, species: str = "human") -> dict:
    """
    Converts gene ID (Entrez or Symbol) to Ensembl Gene ID using MyGene.info.
    Accepts a `species` argument, default is "human".
    
    Returns a dictionary like:
    {
        'input': 'TP53',
        'detected_format': 'symbol',
        'ensembl_id': 'ENSG00000141510',
        'gene_name': 'TP53'
    }
    """
    formatting = detect_gene_id_format(gene_id)
    if formatting == 'ensembl':
        return {
            'input': gene_id,
            'detected_format': 'ensembl',
            'ensembl_id': gene_id,
            'gene_name': None
        }

    query_url = f"https://mygene.info/v3/query?q={gene_id}&fields=ensembl.gene,symbol&species={species}"
    try:
        response = requests.get(query_url, timeout=TIMEOUT)
        data = response.json()

        if not data.get('hits'):
            return {'input': gene_id, 'detected_format': formatting, 'ensembl_id': None, 'gene_name': None}

        hit = data['hits'][0]
        ensembl_id = hit.get('ensembl', {}).get('gene') if isinstance(hit.get('ensembl'), dict) else hit.get('ensembl')[0].get('gene')
        gene_name = hit.get('symbol')
        return {
            'input': gene_id,
            'detected_format': formatting,
            'ensembl_id': ensembl_id,
            'gene_name': gene_name
        }

    except Exception as e:
        print(f"Error fetching Ensembl ID for {gene_id} ({species}): {e}")
        return {'input': gene_id, 'detected_format': formatting, 'ensembl_id': None, 'gene_name': None}

# -------------------------------------------------------------
# --------------- Harmonization of Ontology IDs ---------------
# -------------------------------------------------------------


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

def fetch_from_chebi(curie: str) -> bool:
    """Return True if the CHEBI term exists, else False (no parsing needed)."""
    chebi_id = curie.split(":")[1]          # '17924'
    url = f"https://www.ebi.ac.uk/chebi/ws/rest/chebiId/{chebi_id}"
    try:
        r = requests.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        # Minimal parse: if it’s valid XML with <ChebiEntity>, we’re good
        ET.fromstring(r.text)
        return True
    except Exception:
        return False


def fetch_from_ncbi_taxon(curie: str) -> bool:
    """Cheap existence test via Entrez ESummary."""
    tax_id = curie.split(":")[1]
    url = ("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
           f"esummary.fcgi?db=taxonomy&id={tax_id}&retmode=json")
    try:
        r = requests.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        return bool(data["result"].get(tax_id))
    except Exception:
        return False


# ----------------------------------------------------------------
#  Lookup strategy per ontology prefix
# ----------------------------------------------------------------

# Each entry is ordered list of *callables* that return True/False
PREFIX_LOOKUP_CHAIN: Dict[str, tuple[Callable[[str], bool], ...]] = {
    # Preferred specialist endpoints first, then general OLS / Ontobee
    "CHEBI":    (fetch_from_chebi, fetch_from_ols, fetch_from_ontobee),
    "NCBITaxon": (fetch_from_ncbi_taxon, fetch_from_ols, fetch_from_ontobee),
    "HsapDv":   (fetch_from_ontobee, ),                    # Ontobee only
    # Everything else defaults to OLS → Ontobee
}

DEFAULT_CHAIN = (fetch_from_ols, fetch_from_ontobee)

# -----------------------------------------------------------------------------
# 2. resolve_stimulus_iri  – map free-text metabolite names to canonical IRIs
# -----------------------------------------------------------------------------
_OLS_SEARCH_URL = "https://www.ebi.ac.uk/ols/api/search"

# Curated one-shot mapping for the metabolites you know you’ll see often
# (synonyms folded to lowercase ASCII so they match the output of lowercase_ascii)
_CURATED: dict[str, str] = {
    "butyrate":          "CHEBI:30772",
    "sodium butyrate":   "CHEBI:32177",
    "acetate":           "CHEBI:30089",
    "lps":               "CHEBI:16412",        # lipopolysaccharide
    "tnfα":              "CHEBI:18259",        # TNF alpha cytokine
    "tnfa":              "CHEBI:18259",
    "none":              None,                 # explicit “no stimulus”
}

# Loose regex to recognise an already-supplied CHEBI or EFO CURIE inside the text
_CURIE_RE = re.compile(r"\b((?:CHEBI|EFO)[:_]\d+)\b", flags=re.I)

def resolve_stimulus_iri(label: Optional[str]) -> Optional[str]:
    """
    Best-effort conversion of a *free-text* stimulus label into a canonical OBO IRI.

    Strategy
    --------
    1. Label → lowercase_ascii
    2. If it already contains a recognised CURIE (CHEBI:12345), return its IRI.
    3. Check curated synonym table (fast path).
    4. Fallback: live query to the OLS `/search` endpoint, restricted to CHEBI & EFO.
       The first exact-label hit is accepted.

    If every step fails, returns None (the ETL will keep the raw text in
    `Stimuli.label` and leave `Stimuli.iri` NULL).
    """
    if label is None:
        return None

    canon = lowercase_ascii(label)
    if canon is None:
        return None

    # Already contains CURIE?  (user may have typed "CHEBI:30772 (butyrate)")
    m = _CURIE_RE.search(canon)
    if m:
        curie = m.group(1).replace("_", ":").upper()
        return canonical_iri(curie)        # validate & expand to IRI

    # Curated dictionary hit
    if canon in _CURATED:
        curie = _CURATED[canon]
        return canonical_iri(curie) if curie else None

    # Online lookup: OLS text search limited to CHEBI + EFO
    try:
        import requests
        from urllib.parse import quote_plus

        params = {
            "q": quote_plus(canon),
            "ontology": "chebi,efo",
            "exact": "true",
            "fieldList": "iri,obo_id,label",
        }
        r = requests.get(_OLS_SEARCH_URL, params=params, timeout=5)
        r.raise_for_status()
        docs = r.json().get("response", {}).get("docs", [])
        if docs:
            # trust the first exact match
            candidate_iri  = docs[0]["iri"]
            return canonical_iri(candidate_iri)
    except Exception:
        # Network issues, unknown label – silently fall through
        pass

    return None


# ----------------------------------------------------------------
#  The canonicaliser itself
# ----------------------------------------------------------------
def canonical_iri(id_str: str) -> Optional[str]:
    """
    • Normalises *id_str* (CURIE or IRI)
    • Picks a lookup chain based on ontology prefix
    • Returns canonical IRI if the term can be resolved, None otherwise
    """
    norm = normalize_ontology_id(id_str)
    if norm is None:
        return None

    curie  = norm["curie"]
    iri    = norm["iri"]
    prefix = norm["ontology_prefix"]

    # Decide which lookup functions to try
    chain = PREFIX_LOOKUP_CHAIN.get(prefix, DEFAULT_CHAIN)

    for fetcher in chain:
        ok = False
        # fetch_from_ols / _ontobee return dict on success, specialist fetchers return bool
        try:
            res = fetcher(curie if fetcher is not fetch_from_ontobee else iri)
            ok  = bool(res)
        except Exception:
            ok = False
        if ok:
            return iri  # success → canonical IRI

    # No service could verify the term
    return None

def enrich_ontology_term(id_str: str) -> Optional[Dict[str, Any]]:
    """
    Resolve *id_str* (CURIE or IRI) and return a dictionary with:
        • iri               – canonical full IRI           (str)
        • curie             – CURIE form                  (str)
        • ontology_prefix   – e.g. 'CL', 'UBERON'         (str)
        • label             – preferred human-readable    (str or None)
        • definition        – textual definition          (str or None)
        • source            – service that supplied data  (str)
    
    On complete failure returns None.
    """
    norm = normalize_ontology_id(id_str)
    if norm is None:
        return None                        # malformed identifier

    curie  = norm["curie"]
    iri    = norm["iri"]
    prefix = norm["ontology_prefix"]

    lookups = PREFIX_LOOKUP_CHAIN.get(prefix, DEFAULT_CHAIN)

    # Helper to merge two (possibly partial) result dicts
    def _merge(d1: Dict[str, Any], d2: Dict[str, Any]) -> Dict[str, Any]:
        merged = d1.copy()
        merged.update({k: v for k, v in d2.items() if v})   # keep non-null values
        return merged

    # Start with the normalised identifiers; we’ll enrich as we go
    payload: Dict[str, Any] = {
        "iri": iri,
        "curie": curie,
        "ontology_prefix": prefix,
        "label": None,
        "definition": None,
        "source": None,
    }

    for fetcher in lookups:
        try:
            if fetcher is fetch_from_ontobee:
                res = fetcher(iri)          # Ontobee needs full IRI
            else:
                res = fetcher(curie)

            # Specialist probes (e.g. fetch_from_chebi / _ncbi_taxon) return bool
            if res is True:
                payload["source"] = fetcher.__name__
                return payload            # existence confirmed; no rich metadata available
            elif isinstance(res, dict):
                payload = _merge(payload, res)
                payload["source"] = fetcher.__name__
                # If we now have at least a label, declare success
                if payload["label"]:
                    return payload
        except Exception:                  # keep trying the next service
            continue

    # All lookups exhausted without usable data
    return None

def harmonise(records, mapping):
    staging = defaultdict(set)
    for rec in records:
        for rule_name, rule in mapping["columns"].items():
            if re.match(rule["regex"], rec["value"]):
                value = rec["value"]
                # Run transform steps in order
                for fn in rule["transforms"]:
                    value = getattr(transforms, fn)(value)
                staging[(rule["target_table"], rule["target_column"])].add(value)
                break   # stop at first matching rule
    return staging
