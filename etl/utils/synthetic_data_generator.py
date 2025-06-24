#!/usr/bin/env python3

# ────────────────────────────────────────────────────────────────────────────
# synthetic_data_generator.py   (schema-aligned, multi-file edition)
# ---------------------------------------------------------------------------
# • Creates reference-catalog TSVs        (Genes, Taxa, Microbes, Stimuli,
#   OntologyTerms, Studies)
# • Creates link-table TSVs               (SampleMicrobe, SampleStimulus,
#   MicrobeStimulus)
# • Creates 1 … N experiment_*.tsv        (Samples rows + extra metadata)
# • Creates raw-count TSV for each sample (<sample_id>_raw_counts.tsv)
#
# The column order of every file exactly matches db_schema.log
# ────────────────────────────────────────────────────────────────────────────

from __future__ import annotations
import argparse, csv, datetime as dt, pathlib, random, string, sys, time
from typing import List, Dict, Tuple
import numpy as np

# :::::::::::::::::::::::::::::::::::::::::::: CONFIG :::::::::::::::::::::::

GENES_PER_RUN       = 6000
SAMPLES_PER_EXP     = 32
RAW_COUNT_MEAN      = 900
RAW_COUNT_THETA     = 8
CELL_TYPES: List[Tuple[str,str]] = [
    ("CL_0000127",  "astrocyte"),
    ("CL_0000540",  "microglial cell"),
    ("CL_0001050",  "enterocyte"),
    ("CL_0002608",  "intestinal tuft cell"),
    ("CL_0000091",  "brain endothelial cell"),
    ("CL_0000679",  "dopaminergic neuron"),
    ("CL_0000584",  "GABAergic interneuron"),
]
TISSUES: List[Tuple[str,str]] = [
    ("UBERON_0002108", "colon"),
    ("UBERON_0001155", "small intestine"),
    ("UBERON_0000955", "brain"),
]
STIMULI: List[Tuple[str,str,str]] = [
    ("CHEBI:30772", "butyrate", "SCFA"),
    ("CHEBI:16221", "propionate", "SCFA"),
    ("CHEBI:30089", "acetate", "SCFA"),
    ("CHEBI:16865", "gamma-aminobutyric acid", "neurotransmitter"),
    ("CHEBI:18257", "tumour necrosis factor-alpha", "cytokine"),
    ("NONE",        "none",     ""),
]
MICROBES: List[Tuple[int,str,str,str]] = [
    (818,  "Bacteroides thetaiotaomicron", "Bt-VPI5482",  "anaerobe"),
    (1351, "Enterococcus faecalis",        "Ef-OG1RF",    "facultative"),
    (562,  "Escherichia coli",             "Ecoli-K12",   "facultative"),
    (1491, "Lactobacillus rhamnosus",      "LGG",         "anaerobe"),
    (1301, "Faecalibacterium prausnitzii", "Fp-A2-165",   "anaerobe"),
]
TAXA: List[Tuple[int,str,str]] = [
    (9606, "Homo sapiens",      "Eukaryota"),
    (818,  "B. thetaiotaomicron","Bacteria"),
    (1351, "E. faecalis",        "Bacteria"),
    (562,  "E. coli",            "Bacteria"),
    (1491, "L. rhamnosus",       "Bacteria"),
    (1301, "F. prausnitzii",     "Bacteria"),
]

# ::::::::::::::::::::::::::::::::::::::: HELPERS :::::::::::::::::::::::::::

def ts() -> str:
    return dt.datetime.now().strftime("%Y%m%d-%H%M%S")

def rnd_id(prefix: str,length:int=6)->str:
    return prefix+"".join(random.choices(string.ascii_uppercase+string.digits,k=length))

def nb_counts(n:int, mean:int, theta:int)->np.ndarray:
    r=theta; p=r/(r+mean)
    return np.random.negative_binomial(r,p,size=n)

# :::::::::::::::::::::::::::::::::: CATALOGS :::::::::::::::::::::::::::::::

def mk_gene_catalog(out: pathlib.Path, n:int) -> List[str]:
    genes=[f"ENSG{str(i).zfill(11)}" for i in random.sample(range(1,30_000),n)]
    fp=out/"gene_catalog.tsv"
    with fp.open("w",newline="") as fh:
        w=csv.writer(fh,delimiter="\t")
        w.writerow(["gene_accession","gene_name","species_taxon_id",
                    "gene_length_bp","gc_content_pct",
                    "pathway_iris","go_terms"])
        for gid in genes:
            w.writerow([gid,
                        f"Gene{gid[-4:]}",
                        9606,
                        random.randint(500,200_000),
                        round(random.uniform(35,65),2),
                        "[]","[]"])
    return genes

def mk_taxa_catalog(out:pathlib.Path):
    fp=out/"taxa_catalog.tsv"
    with fp.open("w",newline="") as fh:
        w=csv.writer(fh,delimiter="\t")
        w.writerow(["id","kingdom","rank","gc_content_pct",
                    "genome_length_bp","habitat","pathogenicity"])
        for tid,name,king in TAXA:
            w.writerow([tid,king,"species",
                        round(random.uniform(30,65),2),
                        random.randint(2_000_000,5_500_000),
                        "intestine" if king=="Bacteria" else "human body",
                        random.choice(["commensal","opportunist","pathogen"])])
            
def mk_microbe_catalog(out:pathlib.Path):
    fp=out/"microbe_catalog.tsv"
    with fp.open("w",newline="") as fh:
        w=csv.writer(fh,delimiter="\t")
        w.writerow(["taxon_id","species_name","strain_name",
                    "genome_size_bp","gc_content_pct","oxygen_requirement",
                    "relative_abundance_pct","prevalence_pct"])
        for tid,spp,strain,oxy in MICROBES:
            w.writerow([tid,spp,strain,
                        random.randint(2_000_000,5_000_000),
                        round(random.uniform(30,60),2),
                        oxy,
                        round(random.uniform(0.1,10),2),
                        round(random.uniform(20,90),2)])

def mk_stimulus_catalog(out:pathlib.Path):
    fp=out/"stimulus_catalog.tsv"
    with fp.open("w",newline="") as fh:
        w=csv.writer(fh,delimiter="\t")
        w.writerow(["iri","label","class_hint","chemical_formula",
                    "smiles","molecular_weight","default_dose","dose_unit"])
        for iri,label,cls in STIMULI:
            chem="C4H8O2" if "butyrate" in label else ""
            w.writerow([iri,label,cls,chem,"",random.randint(60,500),
                        random.choice([0.1,1,10]),"mM"])

def mk_ontology_catalog(out:pathlib.Path):
    fp=out/"ontology_terms.tsv"
    with fp.open("w",newline="") as fh:
        w=csv.writer(fh,delimiter="\t")
        w.writerow(["iri","label","ontology","definition","synonyms","version"])
        for iri,lbl in CELL_TYPES+TISSUES:
            ont="CL" if iri.startswith("CL") else "UBERON"
            w.writerow([iri,lbl,ont,"","","", "2025-06"])

# :::::::::::::::::::::::::::::::::: STUDIES ::::::::::::::::::::::::::::::::

def mk_study_catalog(out:pathlib.Path, n_exp:int)->List[str]:
    fp=out/"study_catalog.tsv"
    studies=[]
    with fp.open("w",newline="") as fh:
        w=csv.writer(fh,delimiter="\t")
        w.writerow(["study_id","title","source_repo",
                    "publication_date","study_type",
                    "num_samples","contact_email"])
        for _ in range(n_exp):
            sid=f"E-MTAB-{random.randint(10000,99999)}"
            title=f"Synthetic gut-brain experiment {sid}"
            w.writerow([sid,title,"LOCAL",
                        dt.date.today().isoformat(),
                        "scRNA-seq",
                        SAMPLES_PER_EXP,
                        f"{sid.lower()}@example.org"])
            studies.append(sid)
    return studies

# ::::::::::::::::::::::::::::::::: LINK TABLES :::::::::::::::::::::::::::::

def init_link_files(out:pathlib.Path):
    # create headers once, append rows later
    (out/"sample_microbe.tsv").write_text(
        "sample_id\tmicrobe_taxon_id\trelative_abundance_pct\tevidence\n")
    (out/"sample_stimulus.tsv").write_text(
        "sample_id\tstimulus_iri\texposure_time_hr\tresponse_marker\n")
    (out/"microbe_stimulus.tsv").write_text(
        "microbe_taxon_id\tstimulus_iri\tinteraction_score\tevidence\n")

def append(out_file:pathlib.Path, row:List):
    with out_file.open("a",newline="") as fh:
        csv.writer(fh,delimiter="\t").writerow(row)

# ::::::::::::::::::::::::::::::::: EXPERIMENTS ::::::::::::::::::::::::::::

def make_experiments(out:pathlib.Path, studies:List[str], genes:List[str]):
    sm_link  = out/"sample_microbe.tsv"
    ss_link  = out/"sample_stimulus.tsv"
    ms_link  = out/"microbe_stimulus.tsv"

    for sid in studies:
        exp = f"experiment_{sid}"
        meta_fp=out/f"{exp}.tsv"
        with meta_fp.open("w",newline="") as fh:
            w=csv.writer(fh,delimiter="\t")
            w.writerow([
                "sample_id","study_id",
                "cell_type_iri","cell_type_label",
                "tissue_iri","tissue_label",
                "organism_iri","organism_label",
                "growth_condition",
                "stimulus_iri","stimulus_label",
                "microbe_taxon_id","microbe_species_name",
                "zarr_uri","collection_date","donor_age_years",
                "replicate_number","viability_pct","rin_score"
            ])
            for _ in range(SAMPLES_PER_EXP):
                samp=rnd_id("SAMP",8)
                cell_iri,cell_lbl=random.choice(CELL_TYPES)
                tissue_iri,tissue_lbl=random.choice(TISSUES)
                org_iri="NCBITaxon:9606"; org_lbl="Homo sapiens"
                growth=random.choice(["monoculture","co-culture"])
                stim_iri,stim_lbl,_=random.choice(STIMULI)
                micro_tid,micro_name,*_=random.choice(MICROBES)
                zarr=f"s3://synthetic/{exp}/{samp}.zarr"
                coll_date=(dt.date.today()-dt.timedelta(days=random.randint(0,180))).isoformat()
                w.writerow([
                    samp,sid,
                    cell_iri,cell_lbl,
                    tissue_iri,tissue_lbl,
                    org_iri,org_lbl,
                    growth,
                    stim_iri,stim_lbl,
                    micro_tid,micro_name,
                    zarr,coll_date,
                    random.randint(20,65),            # donor age
                    random.randint(1,3),              # replicate
                    round(random.uniform(70,95),1),   # viability
                    round(random.uniform(7,10),2)     # RIN
                ])
                # raw counts
                counts=nb_counts(len(genes),RAW_COUNT_MEAN,RAW_COUNT_THETA)
                with (out/f"{samp}_raw_counts.tsv").open("w",newline="") as cf:
                    cw=csv.writer(cf,delimiter="\t")
                    cw.writerow(["gene_id","count"])
                    cw.writerows(zip(genes,counts))
                # link-tables
                append(sm_link,[samp,micro_tid,round(random.uniform(0.1,30),2),
                                random.choice(["mgnify","literature","inferred"])])
                append(ss_link,[samp,stim_iri,random.randint(1,48),
                                random.choice(["IL6","NFkB","IFNγ"])])
        # Microbe–Stimulus edges (once per experiment)
        mi=random.choice(MICROBES); st=random.choice(STIMULI)
        append(ms_link,[mi[0],st[0],round(random.uniform(-1,1),3),
                        random.choice(["mgnify","literature","inferred"])])

# ::::::::::::::::::::::::::::::::::::: MAIN :::::::::::::::::::::::::::::::

def main():
    ap=argparse.ArgumentParser(
        description="Generate synthetic TSVs and raw counts aligned with DW schema")
    ap.add_argument("out_dir",type=pathlib.Path,help="output directory")
    ap.add_argument("-n","--num_experiments",type=int,default=1)
    ap.add_argument("--seed",type=int,help="random seed")
    args=ap.parse_args()
    
    if args.seed is not None:
        random.seed(args.seed); np.random.seed(args.seed)
    else:
        random.seed(time.time_ns()&0xFFFF_FFFF)

    out=args.outdir.resolve()
    out.mkdir(parents=True,exist_ok=True)

    genes=mk_gene_catalog(out,GENES_PER_RUN)
    mk_taxa_catalog(out)
    mk_microbe_catalog(out)
    mk_stimulus_catalog(out)
    mk_ontology_catalog(out)
    studies=mk_study_catalog(out,args.num_experiments)
    init_link_files(out)
    make_experiments(out,studies,genes)

    print(f"✔  Synthetic data written to {out}")

if __name__=="__main__":
    main()
