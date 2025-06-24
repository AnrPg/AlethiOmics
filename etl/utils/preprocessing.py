#!/usr/bin/env python3

"""
etl.utils.preprocessing
-----------------------
Light-weight, stateless helper transforms used by Harmonizer.

Functions here must be *pure*: they never hit the network or mutate globals,
so they are trivial to unit-test.  Heavyweight look-ups live in
`etl.harmonize` instead.
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Dict, Any

_CURIE_BASE = {
    "CHEBI": "http://purl.obolibrary.org/obo/CHEBI_",
    "EFO":   "http://www.ebi.ac.uk/efo/EFO_",
    "NCBITaxon": "http://purl.obolibrary.org/obo/NCBITaxon_",
    "CL":    "http://purl.obolibrary.org/obo/CL_",
    "UBERON":"http://purl.obolibrary.org/obo/UBERON_",
}

# ───────────────────────────────────────────────────────────────────────────
#  Simple transforms
# ───────────────────────────────────────────────────────────────────────────
def strip_version(ensembl_id: str) -> str:
    """Drop the ''.13'' suffix from an Ensembl accession."""
    return ensembl_id.split(".", 1)[0]


def canonical_iri(curie_or_iri: str) -> str:
    """
    Return a full IRI for common CURIEs (CHEBI:…, EFO:…, NCBITaxon:…, CL_, UBERON_).
    If the value already looks like an HTTP/HTTPS IRI, return unchanged.
    """
    if curie_or_iri.startswith(("http://", "https://")):
        return curie_or_iri

    for prefix, base in _CURIE_BASE.items():
        if curie_or_iri.startswith(f"{prefix}:"):
            return base + curie_or_iri.split(":", 1)[1]
        if curie_or_iri.startswith(f"{prefix}_"):
            return base + curie_or_iri.split("_", 1)[1]

    # unknown pattern – return untouched
    return curie_or_iri


def normalize_study_accession(acc: str) -> str:
    """Upper-case and strip spaces from study IDs such as e-mtab-1234."""
    return acc.strip().upper()


def extract_sample_id(sample_id: str) -> str:
    """
    Validate & return the sample ID.
    Raises ValueError if the string does not match the expected pattern.
    """
    if not re.fullmatch(r"SAMP[A-Z0-9]{8}", sample_id):
        raise ValueError(f"malformed sample_id: '{sample_id}'")
    return sample_id


# ───────────────────────────────────────────────────────────────────────────
#  Registry (used by Harmonizer)
# ───────────────────────────────────────────────────────────────────────────
TRANSFORM_REGISTRY: Dict[str, Any] = {
    "strip_version": strip_version,
    "canonical_iri": canonical_iri,
    "normalize_study_accession": normalize_study_accession,
    "extract_sample_id": extract_sample_id,
}
