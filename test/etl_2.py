#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pytest
import sys
import os
from pathlib import Path

import sys
from pathlib import Path

project_root = str(Path(__file__).resolve().parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)



# ============================================================================
#  ONLINE integration tests – hit live ontology services (skip if offline)
# ============================================================================
import requests, pytest, types, sys

# Quick connectivity check (ping OLS host)
try:
    _ = requests.get("https://www.ebi.ac.uk/ols4/api", timeout=3)
    _ONLINE = True
except Exception:
    _ONLINE = False

# -----------------------------------------------------------------------------
#  Make sure *harmonise.py* can import preprocessing even if project not yet
#  turned into a proper package (re‑use the shim from earlier).
# -----------------------------------------------------------------------------
import importlib.util
spec = importlib.util.find_spec("preprocessing")
if not spec:
    # If not found, try to load from the current directory
    spec = importlib.util.spec_from_file_location(
        "etl.utils.preprocessing",
        "etl/utils/preprocessing.py"
    )
if not spec:
    # If still not found, try to load from the parent directory
    spec = importlib.util.spec_from_file_location(
        "etl.utils.preprocessing",
        "../etl/utils/preprocessing.py"
    )
if not spec:
    # If still not found, try to load from the grandparent directory
    spec = importlib.util.spec_from_file_location(
        "etl.utils.preprocessing",
        "../../etl/utils/preprocessing.py"
    )

# If we found the spec, load the module
# import importlib.util
# import types
# import os

# Try the correct absolute path first
# project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# preprocessing_path = os.path.join(project_root, "etl", "utils", "preprocessing.py")
# spec = importlib.util.spec_from_file_location("etl.utils.preprocessing", preprocessing_path)

# if not spec:
#     raise ImportError(f"Could not find etl.utils.preprocessing at {preprocessing_path}")

# import sys
# import types
# if "etl.utils.preprocessing" not in sys.modules:
#     _pre_mod = importlib.util.module_from_spec(spec)
#     print(f"Loading preprocessing module from: {spec.origin}")
#     spec.loader.exec_module(_pre_mod)  # type: ignore
#     print("Module 'etl.utils.preprocessing' loaded successfully.")
#     pkg_etl = types.ModuleType("etl")
#     print("Creating 'etl' package module.")
#     pkg_utils = types.ModuleType("etl.utils")
#     print("Creating 'etl.utils' package module.")
#     sys.modules.setdefault("etl", pkg_etl)
#     print("Setting 'etl.utils.preprocessing' in sys.modules.")
#     sys.modules.setdefault("etl.utils", pkg_utils)
#     sys.modules.setdefault("etl.utils.preprocessing", _pre_mod)
#     print("Module 'etl.utils.preprocessing' is now available in sys.modules.")
    
from etl.harmonise import (
    fetch_from_ols,
    fetch_from_ontobee,
    fetch_from_chebi,
    fetch_from_ncbi_taxon,
    normalize_ontology_id,
    curie_to_iri,
    iri_to_curie,
)

@pytest.mark.skipif(not _ONLINE, reason="No internet connection – live ontology tests skipped")
def test_fetch_from_ols_live():
    """Test fetching a term from OLS (Ontology Lookup Service)"""
    # Example: CL:0000057 (embryonic stem cell)
    # Note: This test requires an internet connection to OLS
    print("Fetching term from OLS...")
    # Fetch the term using its CURIE
    term = fetch_from_ols("CL:0000057")  # embryonic stem cell
    print("Term fetched from OLS:", term)
    print("Term name:", term.get("name", "No name found"))
    assert term is not None
    assert "name" in term and term["name"]
    print("Test passed: OLS term fetched successfully.")

@pytest.mark.skipif(not _ONLINE, reason="No internet connection – live ontology tests skipped")
def test_fetch_from_ontobee_live():
    print("Fetching term from Ontobee...")
    # Example: CL:0000057 (embryonic stem cell)
    # Note: This test requires an internet connection to Ontobee
    iri = "http://purl.obolibrary.org/obo/CL_0000057"
    res = fetch_from_ontobee(iri)
    print("Term fetched from Ontobee:", res)
    assert res is not None
    print("Term name:", res.get("name", "No name found"))
    assert res.get("name")
    print("Test passed: Ontobee term fetched successfully.")

@pytest.mark.skipif(not _ONLINE, reason="No internet connection – live ontology tests skipped")
def test_fetch_from_chebi_live():
    print("Fetching term from ChEBI...")
    # Example: CHEBI:17924 (butyrate)
    # Note: This test requires an internet connection to ChEBI
    res = fetch_from_chebi("CHEBI:17924")
    print("Term fetched from https://www.ebi.ac.uk/webservices/chebi/2.0/test/getCompleteEntity?chebiId=CHEBI:17924&format=xml : ", res)
    assert res == "D-glucitol"
    print("Test passed: ChEBI term fetched successfully.")

# @pytest.mark.skipif(not _ONLINE, reason="No internet connection – live ontology tests skipped")
# def test_fetch_from_ncbi_taxon_live():
#     print("Fetching term from NCBI Taxon...")
#     # Example: NCBITaxon:9606 (Homo sapiens)
#     # Note: This test requires an internet connection to NCBI Taxon
#     res = fetch_from_ncbi_taxon("NCBITaxon:9606")
#     print("Term fetched from NCBI Taxon:", res)
#     assert res is not None
#     print("Term name:", res.get("name", "No name found"))
#     assert res.get("name")
#     print("Test passed: NCBI Taxon term fetched successfully.")

# Simple round‑trip sanity for helpers (no internet required)
@pytest.mark.parametrize("curie,iri", [
    ("CL:0000057", "http://purl.obolibrary.org/obo/CL_0000057"),
    ("NCBITaxon:9606", "http://purl.obolibrary.org/obo/NCBITaxon_9606"),
])
def test_curie_iri_roundtrip(curie, iri):
    print("Testing CURIE to IRI roundtrip...")

    value = curie_to_iri(curie)
    print("CURIE to IRI:", value)
    assert value == iri

    value = iri_to_curie(iri)
    print("IRI to CURIE:", value)
    assert value == curie

    print("Test passed: CURIE to IRI roundtrip successful.")

@pytest.mark.parametrize("input_str", ["CL:0000057", "http://purl.obolibrary.org/obo/CL_0000057"])
def test_normalize_ontology_id(input_str):
    print("Testing normalization of ontology ID...")
    """Test normalization of ontology IDs"""
    # Normalize the input string
    norm = normalize_ontology_id(input_str)
    print("Normalized ontology ID:", norm)
    # Check if the normalization result is not None
    assert norm is not None
    # Check if the normalized ID starts with the expected prefix
    assert norm["curie"].startswith("CL:")
    print("Test passed: Normalization of ontology ID successful.")

