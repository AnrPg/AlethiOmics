#!/usr/bin/env python3
"""
synthetic_data_generator.py  ───────────────────────────────────────────────

Create **synthetic, schema‑aware** scRNA‑seq payloads for the Gut‑Brain
Organoid Data‑Warehouse demo.  Compared with the previous version this script
now fabricates **ALL major tables** in the revised MySQL schema:

* **Samples**  – core + research‑centric columns
* **Studies**  – publication date, study_type, etc.
* **Stimuli**  – chem_formula, SMILES, default dose & unit
* **Microbes** – strain/oxygen_requirement/… genome stats
* **Taxa**     – parent table for microbes + Homo sapiens
* **OntologyTerms** – brain/gut cell types & tissues
* **Genes**    – length, GC, GO terms, pathway IRI
* **Link tables**  – *SampleMicrobe* (relative abundance) &
                     *SampleStimulus* (exposure_time_hr, marker)

For convenience the generator still writes **one flat experiment TSV** per
synthetic study that holds sample‑level metadata *plus* a compact list of gene
accessions actually present in that sample.  All the other entity tables are
emitted once per batch so the ETL can bulk‑insert them first and then resolve
FKs.

Every run is fully reproducible with `--seed`.

Output tree (per invocation):

```
<outdir>/
 ├─ experimentE‑MTAB‑54321.tsv   ←   Samples (+ gene list)
 ├─ gene_catalog.tsv             ←   Genes
 ├─ microbe_catalog.tsv          ←   Microbes
 ├─ stimulus_catalog.tsv         ←   Stimuli
 ├─ study_catalog.tsv            ←   Studies
 ├─ taxa_catalog.tsv             ←   Taxa
 ├─ ontology_terms.tsv           ←   Cell & tissue terms
 ├─ sample_microbe.tsv           ←   SampleMicrobe link
 ├─ sample_stimulus.tsv          ←   SampleStimulus link
 └─  *N*×  <sample_id>_raw_counts.tsv
```
"""
from __future__ import annotations

import argparse
import csv
import random
import textwrap
from datetime import date, timedelta, datetime
from itertools import islice
from pathlib import Path

##############################################################################
#  Config                                                                    #
##############################################################################

N_GENES_PER_SAMPLE   = 30          # ‑‑ gene_01 … gene_30 columns
RAW_COUNT_LAMBDA     = 10_000      # Poisson λ for read counts
YEARS_BACK_MAX       = 5           # max age of a sample/study

##############################################################################
#  Vocabulary / small reference panels                                       #
##############################################################################

CELL_TYPES = [
    ("CL:0000540", "astrocyte"),
    ("CL:0000136", "enterocyte"),
    ("CL:0000545", "microglial cell"),
    ("CL:0000548", "oligodendrocyte"),
    ("CL:0000798", "parvalbumin interneuron"),
    ("CL:0000724", "VIP interneuron"),
    ("CL:0008034", "somatostatin interneuron"),
]

TISSUES = [
    ("UBERON:0000955", "brain"),
    ("UBERON:0002108", "colon"),
    ("UBERON:0002113", "small intestine"),
]

GROWTH_CONDS = ["monoculture", "co‑culture", "holo‑organoid", "unknown"]

MICROBES = [
    (818,   "Bacteroides thetaiotaomicron"),
    (853,   "Faecalibacterium prausnitzii"),
    (568815,"Akkermansia muciniphila"),
    (1496,  "Escherichia coli"),
    (39947, "Eubacterium rectale"),
    (1301,  "Clostridium butyricum"),
    (688144,"Roseburia intestinalis"),
    (593,   "Lactobacillus reuteri"),
    (1578,  "Blautia obeum"),
    (436495,"Parabacteroides distasonis"),
]

STIMULI = {
    "CHEBI:30772": {  # butyrate
        "label": "butyrate", "class": "SCFA",
        "chem_formula": "C4H8O2", "smiles": "CCCC(=O)O",
        "mw": 88.105, "dose": 5.0, "unit": "mM",
    },
    "CHEBI:16865": {  # GABA
        "label": "γ‑aminobutyric acid", "class": "neurotransmitter",
        "chem_formula": "C4H9NO2", "smiles": "C(CC(=O)O)CN",
        "mw": 103.118, "dose": 1.0, "unit": "mM",
    },
    "CHEBI:15627": {  # propionate
        "label": "propionate", "class": "SCFA",
        "chem_formula": "C3H6O2", "smiles": "CCC(=O)O",
        "mw": 74.079, "dose": 5.0, "unit": "mM",
    },
    "EFO:0000400": {  # TNF‑α
        "label": "tumour necrosis factor‑α", "class": "cytokine",
        "chem_formula": None, "smiles": None, "mw": None,
        "dose": 10.0, "unit": "ng/mL",
    },
    "none": {         # untreated control
        "label": "none", "class": None,
        "chem_formula": None, "smiles": None, "mw": None,
        "dose": None, "unit": None,
    },
}

# 300 hand‑picked Ensembl IDs (truncated here for brevity)
GENE_IDS = [
    "ENSG00000141510", "ENSG00000198793", "ENSG00000171862", "ENSG00000155657",
    "ENSG00000142208", "ENSG00000136997", "ENSG00000111679", "ENSG00000103126",
    "ENSG00000121879", "ENSG00000162194", "ENSG00000146648", "ENSG00000139618",
    # … add more as needed …
]

##############################################################################
#  Small utility helpers                                                     #
##############################################################################

def _rnd_date(back_max_years: int = YEARS_BACK_MAX) -> date:
    """Random date within the last *back_max_years* years."""
    days = random.randint(0, 365 * back_max_years)
    return date.today() - timedelta(days=days)


def _poisson(lam: int) -> int:
    """Very small Poisson using Knuth’s algorithm."""
    from math import exp
    L = exp(-lam)
    k, p = 0, 1.0
    while p > L:
        k += 1
        p *= random.random()
    return k - 1

##############################################################################
#  Core generator steps                                                      #
##############################################################################

def generate_batch(out_dir: Path, n_experiments: int, seed: int | None = None):
    if seed is not None:
        random.seed(seed)
    out_dir.mkdir(parents=True, exist_ok=True)

    #   ─────── accumulators for one batch ───────
    gene_meta: dict[str, dict] = {}
    microbe_meta: dict[int, dict] = {}
    taxa_meta: dict[int, dict] = {9606: {
        "iri": "NCBITaxon:9606", "species_name": "Homo sapiens",
        "kingdom": "Eukaryota", "ranking": "species",
        "gc_content": 41.0, "genome_length": 3_200_000_000,
        "habitat": "human", "pathogenicity": "none"}}
    ontology_rows: set[tuple[str, str, str]] = set()  # (iri, label, ontology)
    study_rows: list[list] = []

    # pre‑cache microbe meta skeletons
    for tax, sci in MICROBES:
        microbe_meta[tax] = {
            "taxon_id": tax,
            "species_name": sci,
            "strain_name": f"ATCC {random.randint(1000, 9999)}",
            "isolate_id": None,
            "culture_collection": None,
            "genome_assembly_accession": f"GCF_{random.randint(100,999)}.{random.randint(1,5)}",
            "genome_size_bp": random.randint(2_000_000, 6_500_000),
            "oxygen_requirement": random.choice(["anaerobe", "facultative"]),
            "habitat": "human gut",
            "optimal_growth_temp": 37.0,
            "doubling_time": round(random.uniform(0.5, 3.0), 2),
            "metabolic_profile_iri": None,
            "metadata_uri": None,
            "abundance_index": round(random.uniform(0.01, 5.0), 4),
            "prevalence": random.randint(10, 500),
        }
        taxa_meta[tax] = {
            "iri": f"NCBITaxon:{tax}", "species_name": sci,
            "kingdom": "Bacteria", "ranking": "species",
            "gc_content": round(random.uniform(35.0, 65.0), 2),
            "genome_length": microbe_meta[tax]["genome_size_bp"],
            "habitat": "human gut", "pathogenicity": "none"}

    # ontology rows (cell + tissue)
    for iri, lbl in CELL_TYPES:
        ontology_rows.add((iri, lbl, "CL"))
    for iri, lbl in TISSUES:
        ontology_rows.add((iri, lbl, "UBERON"))

    # stimulus catalog is already static (STIMULI)

    #   ─────── iterate experiments ───────
    sample_microbe_rows: list[list] = []
    sample_stimulus_rows: list[list] = []

    for _ in range(n_experiments):
        study_acc = f"E-MTAB-{random.randint(10_000, 99_999)}"
        study_pub  = _rnd_date()
        study_rows.append([
            study_acc,
            f"https://identifiers.org/arrayexpress:{study_acc}",
            f"Synthetic study {study_acc}",
            "ArrayExpress",
            study_pub.isoformat(),
            random.choice(["transcriptomic", "multiomic"]),
            None,  # num_samples will be filled later
            f"author{study_acc}@example.org",
        ])

        n_samples = random.randint(8, 24)
        study_rows[-1][6] = n_samples  # num_samples now known

        exp_path = out_dir / f"experiment{study_acc}.tsv"
        with exp_path.open("w", newline="") as fh:
            wr = csv.writer(fh, delimiter="\t")
            hdr_fixed = [
                "sample_id", "study_id",
                "cell_type_iri", "cell_type_label",
                "tissue_iri", "tissue_label",
                "organism_iri", "organism_label",
                "growth_condition",
                "stimulus", "microbe", "zarr_uri",
                "collection_date", "donor_age_years", "replicate_number",
                "viability_pct", "rin_score",
            ]
            hdr_genes = [f"gene_{i:02d}" for i in range(1, N_GENES_PER_SAMPLE + 1)]
            wr.writerow(hdr_fixed + hdr_genes)

            for s_idx in range(1, n_samples + 1):
                sample_id = f"SAMPLE_{study_acc.split('-')[-1]}_{s_idx:04d}"
                cell_iri, cell_lbl = random.choice(CELL_TYPES)
                tissue_iri, tissue_lbl = random.choice(TISSUES)
                organism_iri, organism_lbl = "NCBITaxon:9606", "Homo sapiens"
                growth_cond = random.choice(GROWTH_CONDS)

                stim_curie = random.choice(list(STIMULI.keys()))
                micro_tax, _ = random.choice(MICROBES)
                zarr_uri = f"s3://dummy‑bucket/{sample_id}.zarr"

                coll_date = _rnd_date()
                donor_age = round(random.uniform(20, 70), 1)
                replicate = random.randint(1, 3)
                viability = round(random.uniform(60, 98), 2)
                rin = round(random.uniform(6.0, 9.5), 1)

                sample_genes = random.sample(GENE_IDS, N_GENES_PER_SAMPLE)

                # record sample‑microbe & sample‑stimulus link rows
                sample_microbe_rows.append([
                    sample_id, f"NCBITaxon:{micro_tax}",
                    round(random.uniform(0.0001, 0.05), 6)
                ])
                sample_stimulus_rows.append([
                    sample_id, stim_curie,
                    round(random.uniform(2.0, 48.0), 2),
                    random.choice(["IL‑6", "CXCL8", "NFκB"])
                ])

                # write raw counts file
                counts_fp = out_dir / f"{sample_id}_raw_counts.tsv"
                with counts_fp.open("w", newline="") as cf:
                    cw = csv.writer(cf, delimiter="\t")
                    cw.writerow(["gene_id", "count"])
                    for g in sample_genes:
                        cw.writerow([g, _poisson(RAW_COUNT_LAMBDA)])
                        if g not in gene_meta:
                            gene_meta[g] = {
                                "gene_accession": g,
                                "gene_name": f"G{g[-4:]}",
                                "species_taxon_id": 9606,
                                "gene_length_bp": random.randint(500, 300_000),
                                "gc_content": round(random.uniform(30.0, 65.0), 2),
                                "pathway_iri": None,
                                "go_terms": "|".join(random.sample([
                                    "GO:0008150", "GO:0003674", "GO:0009987",
                                    "GO:0055114", "GO:0006955"], k=2)),
                            }

                wr.writerow([
                    sample_id, study_acc,
                    cell_iri, cell_lbl,
                    tissue_iri, tissue_lbl,
                    organism_iri, organism_lbl,
                    growth_cond,
                    stim_curie, f"NCBITaxon:{micro_tax}", zarr_uri,
                    coll_date.isoformat(), donor_age, replicate,
                    viability, rin,
                ] + sample_genes)
        print(f"✔︎ wrote {exp_path.name}  ({n_samples} samples)")

    #   ─────── dump batch‑level catalogs ───────

    def _dump(path: Path, header: list[str], rows: list[list]):
        with path.open("w", newline="") as h:
            csv.writer(h, delimiter="\t").writerows([header] + rows)
        print(f"✔︎ wrote {path.name}  ({len(rows)} rows)")

    _dump(out_dir / "gene_catalog.tsv",
          ["gene_accession", "gene_name", "species_taxon_id", "gene_length_bp",
           "gc_content", "pathway_iri", "go_terms"],
          list(gene_meta.values()))

    _dump(out_dir / "microbe_catalog.tsv",
          list(next(iter(microbe_meta.values())).keys()),
          list(microbe_meta.values()))

    _dump(out_dir / "stimulus_catalog.tsv",
          ["iri", "label", "class_hint", "chem_formula", "smiles",
           "molecular_weight", "default_dose", "dose_unit"],
          [[iri, d["label"], d["class"], d["chem_formula"], d["smiles"],
            d["mw"], d["dose"], d["unit"]] for iri, d in STIMULI.items()])

    _dump(out_dir / "study_catalog.tsv",
          ["study_id", "iri", "title", "source_repo", "publication_date",
           "study_type", "num_samples", "contact_email"],
          study_rows)

    _dump(out_dir / "taxa_catalog.tsv",
          ["iri", "species_name", "kingdom", "ranking", "gc_content",
           "genome_length", "habitat", "pathogenicity"],
          list(taxa_meta.values()))

    _dump(out_dir / "ontology_terms.tsv",
          ["iri", "label", "ontology"],
          sorted(list(ontology_rows)))

    _dump(out_dir / "sample_microbe.tsv",
          ["sample_id", "microbe", "relative_abundance"], sample_microbe_rows)

    _dump(out_dir / "sample_stimulus.tsv",
          ["sample_id", "stimulus", "exposure_time_hr", "response_marker"],
          sample_stimulus_rows)

##############################################################################
#  CLI                                                                       #
##############################################################################

def _parse_cli() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description=textwrap.dedent("""
            Generate synthetic scRNA‑seq batches for the Gut‑Brain DW.

            Examples
            ---------
            $ python synthetic_data_generator.py out/                   # 1 study
            $ python synthetic_data_generator.py out/ -n 5 --seed 42    # 5 studies (reproducible)
        """))
    p.add_argument("out_dir", type=Path, help="Destination directory")
    p.add_argument("--num", "-n", type=int, default=1,
                   help="Number of experiment TSVs (studies) to create")
    p.add_argument("--seed", type=int, help="RNG seed for deterministic output")
    return p.parse_args()


def main():
    args = _parse_cli()
    generate_batch(args.out_dir, args.num, args.seed)


if __name__ == "__main__":
    main()
