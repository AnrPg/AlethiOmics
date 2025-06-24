#!/usr/bin/env python3

"""
etl.harmonize
-------------
Takes `(table_name, batch_of_dicts)` from `etl.extract.Extractor`,
applies the transform pipeline declared in *mapping_catalogue.yml*, and returns a
new list of dicts whose keys exactly match the MySQL column names.

Design notes
------------
* The YAML is parsed once at construction and cached.
* Transforms are looked up in a registry that merges lightweight helpers
  (`etl.utils.preprocessing.TRANSFORM_REGISTRY`) with the heavier
  look-ups defined here.
* Fetch-type transforms are **stubbed** with deterministic random data so the
  ETL can run offline; replace them with real database/API calls later.
"""
from __future__ import annotations

import json
import random
import re
from datetime import date
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List
import requests
from urllib.parse import quote_plus

import yaml

from etl.utils.preprocessing import TRANSFORM_REGISTRY as SIMPLE_REGISTRY
from etl.utils.preprocessing import (
    canonical_iri,
    strip_version,
    normalize_study_accession,
    extract_sample_id,
)

def _rand_gc() -> float:
    return round(random.uniform(30.0, 65.0), 2)

# ──────────────────────────────────────────────────────────────────────────
#  Real-world fetchers with graceful fallback
# ──────────────────────────────────────────────────────────────────────────
_H = {"User-Agent": "GutBrain-DW/0.1 (+https://example.org)"}
TIMEOUT = 8


def _safe_json(url: str) -> dict | None:
    try:
        r = requests.get(url, headers=_H, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


# ------------------------------------------------------------------------ #
#  Genes – MyGene.info  → Ensembl REST  → stub                            #
# ------------------------------------------------------------------------ #
def fetch_gene_metadata(gene_accession: str) -> Dict[str, Any]:
    g = strip_version(gene_accession)

    # 1️⃣ MyGene.info
    j = _safe_json(f"https://mygene.info/v3/gene/{g}?fields=symbol,name,genomic_pos,go")
    if j:
        pos = j.get("genomic_pos", {})
        length = (
            (pos.get("end") - pos.get("start") + 1)
            if isinstance(pos, dict) and pos.get("end")
            else None
        )
        return {
            "gene_accession": g,
            "gene_name": j.get("symbol") or j.get("name") or g,
            "species_taxon_id": 9606,
            "gene_length_bp": length or random.randint(500, 200_000),
            "gc_content_pct": _rand_gc(),
            "pathway_iris": json.dumps([]),
            "go_terms": json.dumps(list(j.get("go", {}).get("BP", []))),
        }

    # 2️⃣ Ensembl REST
    j = _safe_json(f"https://rest.ensembl.org/lookup/id/{g}?content-type=application/json")
    if j:
        length = j.get("end") - j.get("start") + 1 if j.get("end") else None
        return {
            "gene_accession": g,
            "gene_name": j.get("display_name") or g,
            "species_taxon_id": 9606,
            "gene_length_bp": length or random.randint(500, 200_000),
            "gc_content_pct": _rand_gc(),
            "pathway_iris": json.dumps([]),
            "go_terms": json.dumps([]),
        }

    # 3️⃣ Fallback stub
    return {
        "gene_accession": g,
        "gene_name": f"Gene{g[-4:]}",
        "species_taxon_id": 9606,
        "gene_length_bp": random.randint(500, 200_000),
        "gc_content_pct": _rand_gc(),
        "pathway_iris": json.dumps([]),
        "go_terms": json.dumps([]),
    }


# ------------------------------------------------------------------------ #
#  Stimuli – ChEBI WS  → EBI OLS  → stub                                   #
# ------------------------------------------------------------------------ #
def _chebi_id(iri: str) -> str | None:
    if "CHEBI" in iri:
        return iri.split(":")[-1].lstrip("CHEBI:").lstrip("CHEBI_")
    return None


def fetch_stimulus_metadata(iri: str) -> Dict[str, Any]:
    iri = canonical_iri(iri)
    label = iri.split("/")[-1]

    # 1️⃣ ChEBI (if CHEBI)
    cid = _chebi_id(iri)
    if cid:
        j = _safe_json(f"https://www.ebi.ac.uk/chebi/ws/rest/compound/{cid}")
        if j and "chebiAsciiName" in j:
            return {
                "iri": iri,
                "label": j["chebiAsciiName"],
                "class_hint": "",
                "chemical_formula": j.get("formulae", [{}])[0].get("data", ""),
                "smiles": j.get("smiles", ""),
                "molecular_weight": j.get("mass", 0) or random.randint(60, 500),
                "default_dose": 1.0,
                "dose_unit": "mM",
            }

    # 2️⃣ OLS (generic ontology lookup)
    url = f"https://www.ebi.ac.uk/ols/api/terms?id={quote_plus(iri)}"
    j = _safe_json(url)
    if j and j.get("_embedded", {}).get("terms"):
        t = j["_embedded"]["terms"][0]
        return {
            "iri": iri,
            "label": t.get("label", label),
            "class_hint": "",
            "chemical_formula": "",
            "smiles": "",
            "molecular_weight": random.randint(60, 500),
            "default_dose": 1.0,
            "dose_unit": "mM",
        }

    # 3️⃣ Stub
    return {
        "iri": iri,
        "label": label,
        "class_hint": "",
        "chemical_formula": "",
        "smiles": "",
        "molecular_weight": random.randint(60, 500),
        "default_dose": 1.0,
        "dose_unit": "mM",
    }


# ------------------------------------------------------------------------ #
#  Taxon & Microbes – NCBI EUtils  → stub                                   #
# ------------------------------------------------------------------------ #

def fetch_from_ncbi_taxon(curie: str) -> int:
    """NCBITaxon:824 returns 824 as int."""
    return int(curie.split(":")[-1])

def fetch_taxon_numeric_id(curie: str) -> int:
    return fetch_from_ncbi_taxon(curie)


def _eutils_taxon(tid: int) -> dict | None:
    return _safe_json(
        f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
        f"?db=taxonomy&id={tid}&retmode=json"
    )


def fetch_microbe_metadata(taxon_id: int) -> Dict[str, Any]:
    j = _eutils_taxon(taxon_id)
    if j:
        try:
            item = j["result"][str(taxon_id)]
            return {
                "taxon_id": taxon_id,
                "species_name": item.get("scientificname"),
                "strain_name": item.get("strain", f"Strain_{taxon_id}"),
                "genome_size_bp": random.randint(2_000_000, 5_000_000),
                "gc_content_pct": _rand_gc(),
                "oxygen_requirement": random.choice(["aerobe", "anaerobe", "facultative"]),
                "relative_abundance_pct": round(random.uniform(0.1, 30.0), 2),
                "prevalence_pct": round(random.uniform(20.0, 90.0), 2),
            }
        except Exception:
            pass

    # stub
    return {
        "taxon_id": taxon_id,
        "species_name": f"Species_{taxon_id}",
        "strain_name": f"Strain_{taxon_id}",
        "genome_size_bp": random.randint(2_000_000, 5_000_000),
        "gc_content_pct": _rand_gc(),
        "oxygen_requirement": random.choice(["aerobe", "anaerobe", "facultative"]),
        "relative_abundance_pct": round(random.uniform(0.1, 30.0), 2),
        "prevalence_pct": round(random.uniform(20.0, 90.0), 2),
    }


def fetch_taxon_metadata(taxon_id: int) -> Dict[str, Any]:
    j = _eutils_taxon(taxon_id)
    if j:
        try:
            item = j["result"][str(taxon_id)]
            return {
                "id": taxon_id,
                "kingdom": item.get("division", "Bacteria"),
                "rank": item.get("rank", "species"),
                "gc_content_pct": _rand_gc(),
                "genome_length_bp": random.randint(2_000_000, 6_000_000),
                "habitat": "intestine",
                "pathogenicity": random.choice(
                    ["commensal", "opportunist", "pathogen"]
                ),
            }
        except Exception:
            pass

    # stub
    return {
        "id": taxon_id,
        "kingdom": "Bacteria",
        "rank": "species",
        "gc_content_pct": _rand_gc(),
        "genome_length_bp": random.randint(2_000_000, 6_000_000),
        "habitat": "intestine",
        "pathogenicity": random.choice(["commensal", "opportunist", "pathogen"]),
    }


# ------------------------------------------------------------------------ #
#  Ontology term – EBI OLS  → stub                                          #
# ------------------------------------------------------------------------ #
def fetch_ontology_term_metadata(iri: str) -> Dict[str, Any]:
    url = f"https://www.ebi.ac.uk/ols/api/terms?id={quote_plus(iri)}"
    j = _safe_json(url)
    if j and j.get("_embedded", {}).get("terms"):
        t = j["_embedded"]["terms"][0]
        return {
            "iri": iri,
            "label": t.get("label"),
            "ontology": t.get("ontology_prefix"),
            "definition": (t.get("description") or [""])[0],
            "synonyms": json.dumps(t.get("synonyms", [])),
            "version": t.get("ontology_version", ""),
        }

    # stub
    return {
        "iri": iri,
        "label": iri.split("/")[-1],
        "ontology": "CL" if "CL_" in iri else "UBERON",
        "definition": "",
        "synonyms": json.dumps([]),
        "version": "2025-06",
    }


# ------------------------------------------------------------------------ #
#  Study – ArrayExpress  → GEO  → stub                                      #
# ------------------------------------------------------------------------ #
def _arrayexpress_json(acc: str) -> dict | None:
    return _safe_json(f"https://www.ebi.ac.uk/biostudies/api/v1/studies/{acc}")


def _geo_json(acc: str) -> dict | None:
    return _safe_json(
        f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={acc}&format=json"
    )


def fetch_study_metadata(study_id: str) -> Dict[str, Any]:
    acc = normalize_study_accession(study_id)

    # 1️⃣ ArrayExpress / BioStudies
    j = _arrayexpress_json(acc) if acc.startswith("E-MTAB") else None
    if j:
        return {
            "study_id": acc,
            "title": j.get("title"),
            "source_repo": "ArrayExpress",
            "publication_date": (j.get("publicationDate") or "")[:10],
            "study_type": "scRNA-seq",
            "num_samples": j.get("stats", {}).get("samples", 0),
            "contact_email": "",
        }

    # 2️⃣ GEO
    j = _geo_json(acc) if acc.startswith("GSE") else None
    if j and j.get("title"):
        return {
            "study_id": acc,
            "title": j["title"],
            "source_repo": "GEO",
            "publication_date": "",
            "study_type": "scRNA-seq",
            "num_samples": 0,
            "contact_email": "",
        }

    # 3️⃣ stub
    return {
        "study_id": acc,
        "title": f"Synthetic study {acc}",
        "source_repo": "LOCAL",
        "publication_date": date.today().isoformat(),
        "study_type": "scRNA-seq",
        "num_samples": random.randint(10, 100),
        "contact_email": f"{acc.lower()}@example.org",
    }


# ------------------------------------------------------------------------ #
#  Sample – no public API;                                                 #
# ------------------------------------------------------------------------ #

def fetch_sample_metadata(sample_id: str) -> Dict[str, Any]:
    return {
        "sample_id": sample_id,
        "study_id": "LOCAL-STUDY",
        "cell_type_iri": "CL_0000127",
        "cell_type_label": "astrocyte",
        "tissue_iri": "UBERON_0000955",
        "tissue_label": "brain",
        "organism_iri": "NCBITaxon_9606",
        "organism_label": "Homo sapiens",
        "growth_condition": "monoculture",
        "stimulus_iri": "CHEBI:30772",
        "microbe_taxon_id": 818,
        "zarr_uri": f"s3://bucket/{sample_id}.zarr",
        "collection_date": date.today().isoformat(),
        "donor_age_years": random.randint(20, 70),
        "replicate_number": 1,
        "viability_pct": round(random.uniform(70, 95), 2),
        "rin_score": round(random.uniform(7.0, 10.0), 2),
    }
    

# ----------------- Parsers for link / expression-stat rows -----------------
def _split_payload(text: str) -> List[str]:
    return re.split(r"[:,]", text, maxsplit=1)[-1].split(",")


def parse_sample_microbe_record(text: str) -> Dict[str, Any]:
    _, sample_id, taxon_curie, rel_abund, evidence = text.split(",")
    return {
        "sample_id": sample_id,
        "microbe_taxon_id": fetch_from_ncbi_taxon(taxon_curie),
        "relative_abundance_pct": float(rel_abund),
        "evidence": evidence,
    }


def parse_sample_stimulus_record(text: str) -> Dict[str, Any]:
    _, sample_id, stim_iri, exposure, marker = text.split(",")
    return {
        "sample_id": sample_id,
        "stimulus_iri": canonical_iri(stim_iri),
        "exposure_time_hr": float(exposure),
        "response_marker": marker,
    }


def parse_microbe_stimulus_record(text: str) -> Dict[str, Any]:
    taxon_curie, stim_iri, score, evidence = text.split(",")
    return {
        "microbe_taxon_id": fetch_from_ncbi_taxon(taxon_curie),
        "stimulus_iri": canonical_iri(stim_iri),
        "interaction_score": float(score),
        "evidence": evidence,
    }


def parse_expression_stat_record(text: str) -> Dict[str, Any]:
    fields = text.split(":")[1].split(",")
    return {
        "sample_id": fields[0],
        "gene_accession": strip_version(fields[1]),
        "log2_fc": float(fields[2]),
        "p_value": float(fields[3]),
        "base_mean": float(fields[4]),
        "raw_count": int(fields[5]),
        "significance": fields[6],
    }


# -------------------------------------------------------------------------
#  Merge registries
# -------------------------------------------------------------------------
_TRANSFORM_REGISTRY: Dict[str, Callable[..., Any]] = {
    **SIMPLE_REGISTRY,
    "fetch_gene_metadata": fetch_gene_metadata,
    "fetch_stimulus_metadata": fetch_stimulus_metadata,
    "fetch_from_ncbi_taxon": fetch_from_ncbi_taxon,
    "fetch_microbe_metadata": fetch_microbe_metadata,
    "fetch_taxon_numeric_id": fetch_taxon_numeric_id,
    "fetch_taxon_metadata": fetch_taxon_metadata,
    "fetch_ontology_term_metadata": fetch_ontology_term_metadata,
    "fetch_study_metadata": fetch_study_metadata,
    "fetch_sample_metadata": fetch_sample_metadata,
    "parse_sample_microbe_record": parse_sample_microbe_record,
    "parse_sample_stimulus_record": parse_sample_stimulus_record,
    "parse_microbe_stimulus_record": parse_microbe_stimulus_record,
    "parse_expression_stat_record": parse_expression_stat_record,
}

# -------------------------------------------------------------------------
#  Harmonizer class
# -------------------------------------------------------------------------
class Harmonizer:
    def __init__(self, mapping_yaml: str | Path):
        self.mapping = yaml.safe_load(Path(mapping_yaml).read_text())
        self._prep_table_index()

    # ---------------- internal helpers ----------------
    def _prep_table_index(self) -> None:
        """
        Build `self._table_rules` →  {table_name: [rule_dict, …]}  from YAML.
        Each rule dict keeps:
            regex        – compiled pattern or None
            transforms   – list[callable]
            target_cols  – list[str]
        """
        self._table_rules: Dict[str, List[Dict[str, Any]]] = {}
        for col_name, spec in self.mapping["columns"].items():
            table = spec["target_table"]
            tcols = spec["target_columns"] if "target_columns" in spec else [spec["target_column"]]
            transforms = [self._get_tf(name) for name in spec.get("transforms", [])]
            pattern = re.compile(spec["regex"]) if "regex" in spec else None
            self._table_rules.setdefault(table, []).append(
                {"regex": pattern, "transforms": transforms, "targets": tcols}
            )

    @staticmethod
    def _get_tf(name: str) -> Callable:
        try:
            return _TRANSFORM_REGISTRY[name]
        except KeyError as err:
            raise KeyError(f"Unknown transform '{name}'") from err

    # ---------------- public API ----------------
    def apply(self, table: str, rows: Iterable[Dict]) -> List[Dict]:
        """
        Apply YAML-declared transforms to *rows* (list[dict]) that were
        extracted from a physical TSV belonging to *table*.
        Returns a **new** list of harmonised dicts.
        """
        if table not in self._table_rules:
            # No rules for this table → passthrough
            return list(rows)

        rules = self._table_rules[table]
        harmonised: List[Dict] = []

        for row in rows:
            new_row = dict(row)  # shallow copy

            # Run every rule on every original value in the input row
            for val in list(row.values()):
                if not isinstance(val, str):
                    continue
                for rule in rules:
                    if rule["regex"] and not rule["regex"].fullmatch(val):
                        continue
                    payload = val
                    # Sequentially apply transforms
                    for tf in rule["transforms"]:
                        payload = tf(payload)
                    # payload may be a dict (metadata expansion) or scalar
                    if isinstance(payload, dict):
                        new_row.update(payload)
                    else:
                        if len(rule["targets"]) == 1:
                            new_row[rule["targets"][0]] = payload
                        else:
                            raise ValueError(
                                f"Transform chain for value '{val}' "
                                f"returned scalar but target_columns >1"
                            )

            harmonised.append(new_row)

        return harmonised




# --------------------------------------------------------------------------
#  Dumb fetchers that return mock (randomly generated) data
# --------------------------------------------------------------------------


# -------------------------------------------------------------------------
#  Heavyweight / metadata-generating transforms
# -------------------------------------------------------------------------

# def fetch_gene_metadata(gene_accession: str) -> Dict[str, Any]:
#     return {
#         "gene_accession": strip_version(gene_accession),
#         "gene_name": f"Gene{gene_accession[-4:]}",
#         "species_taxon_id": 9606,
#         "gene_length_bp": random.randint(500, 200_000),
#         "gc_content_pct": _rand_gc(),
#         "pathway_iris": json.dumps([]),
#         "go_terms": json.dumps([]),
#     }


# def fetch_stimulus_metadata(iri: str) -> Dict[str, Any]:
#     iri = canonical_iri(iri)
#     label = iri.split("/")[-1]
#     return {
#         "iri": iri,
#         "label": label,
#         "class_hint": "",
#         "chemical_formula": "",
#         "smiles": "",
#         "molecular_weight": random.randint(60, 500),
#         "default_dose": 1.0,
#         "dose_unit": "mM",
#     }


# def fetch_microbe_metadata(taxon_id: int) -> Dict[str, Any]:
#     return {
#         "taxon_id": taxon_id,
#         "species_name": f"Species_{taxon_id}",
#         "strain_name": f"Strain_{taxon_id}",
#         "genome_size_bp": random.randint(2_000_000, 5_000_000),
#         "gc_content_pct": _rand_gc(),
#         "oxygen_requirement": random.choice(["aerobe", "anaerobe", "facultative"]),
#         "relative_abundance_pct": round(random.uniform(0.1, 30.0), 2),
#         "prevalence_pct": round(random.uniform(20.0, 90.0), 2),
#     }


# def fetch_taxon_metadata(taxon_id: int) -> Dict[str, Any]:
#     return {
#         "id": taxon_id,
#         "kingdom": random.choice(["Bacteria", "Eukaryota", "Archaea"]),
#         "rank": "species",
#         "gc_content_pct": _rand_gc(),
#         "genome_length_bp": random.randint(2_000_000, 6_000_000),
#         "habitat": "intestine",
#         "pathogenicity": random.choice(["commensal", "opportunist", "pathogen"]),
#     }


# def fetch_ontology_term_metadata(iri: str) -> Dict[str, Any]:
#     return {
#         "iri": canonical_iri(iri),
#         "label": iri.split("/")[-1],
#         "ontology": "CL" if "CL_" in iri else "UBERON",
#         "definition": "",
#         "synonyms": json.dumps([]),
#         "version": "2025-06",
#     }


# def fetch_study_metadata(study_id: str) -> Dict[str, Any]:
#     return {
#         "study_id": study_id,
#         "title": f"Synthetic study {study_id}",
#         "source_repo": "LOCAL",
#         "publication_date": date.today().isoformat(),
#         "study_type": "scRNA-seq",
#         "num_samples": random.randint(10, 100),
#         "contact_email": f"{study_id.lower()}@example.org",
#     }
