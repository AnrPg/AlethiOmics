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
import os
import subprocess
import fsspec, yaml
import argparse, csv, datetime as dt, pathlib, random, string, sys, time
from typing import List, Dict, Tuple
import numpy as np
from zoneinfo import ZoneInfo
from typing import List, Dict, Tuple
import numpy as np

# ::::::::::::::::::::::::::::::::::::::: GLOBALS :::::::::::::::::::::::::::
DEFAULT_TZ        = "Europe/Athens"
TIMESTAMP_FMT     = "%Y%m%d-%H%M%S"
DEFAULT_BASE_URI = os.environ.get("BASE_URI", f"file://./raw_data/synthetic_runs")


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

def ts(fmt: str = TIMESTAMP_FMT, tz: str = DEFAULT_TZ) -> str:
    """
    Return a timestamp string in the requested *fmt* and *tz* (IANA name).
    Falls back to UTC if the zone isn’t recognised.
    """
    try:
        z = ZoneInfo(tz)
    except Exception:
        z = dt.timezone.utc
    return dt.datetime.now(z).strftime(fmt)

def rnd_id(prefix: str,length:int=6)->str:
    return prefix+"".join(random.choices(string.ascii_uppercase+string.digits,k=length))

def nb_counts(n:int, mean:int, theta:int)->np.ndarray:
    r=theta; p=r/(r+mean)
    return np.random.negative_binomial(r,p,size=n)

def load_config(path: Path) -> dict:
    """
    If `path` ends in .age, run `age --decrypt` (using $AGE_IDENTITY or default).
    Otherwise load plaintext YAML.
    """
    data = None
    if path.suffix == ".age":
        # determine identity file
        identity = os.environ.get("AGE_IDENTITY")  # e.g. /home/user/key.pub or /.config/key.txt or whatever...
        cmd = ["age", "--decrypt", str(path)]
        if identity:
            cmd += ["--identity", identity]
        # decrypt into memory
        proc = subprocess.run(cmd, check=True, stdout=subprocess.PIPE)
        data = proc.stdout.decode()
    else:
        data = path.read_text()
    return yaml.safe_load(data) or {}


# :::::::::::::::::::::::::::::::::: CATALOGS :::::::::::::::::::::::::::::::

def mk_gene_catalog(fs, root:str, n:int) -> List[str]:
    genes = [f"ENSG{str(i).zfill(11)}" for i in random.sample(range(1,30_000), n)]
    path = f"{root}/gene_catalog.tsv"
    with fs.open(path, "w", newline="") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow([
            "gene_accession","gene_name","species_taxon_id",
            "gene_length_bp","gc_content_pct","pathway_iris","go_terms"
        ])
        for gid in genes:
            w.writerow([
                gid,
                f"Gene{gid[-4:]}",
                9606,
                random.randint(500,200_000),
                round(random.uniform(35,65),2),
                "[]","[]"
            ])
    return genes


def mk_taxa_catalog(fs, root:str):
    path = f"{root}/taxa_catalog.tsv"
    with fs.open(path, "w", newline="") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow(["id","kingdom","rank","gc_content_pct",
                    "genome_length_bp","habitat","pathogenicity"])
        for tid,name,king in TAXA:
            w.writerow([tid,king,"species",
                        round(random.uniform(30,65),2),
                        random.randint(2_000_000,5_500_000),
                        "intestine" if king=="Bacteria" else "human body",
                        random.choice(["commensal","opportunist","pathogen"])])
            
def mk_microbe_catalog(fs, root:str):
    path = f"{root}/microbe_catalog.tsv"
    with fs.open(path, "w", newline="") as fh:
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

def mk_stimulus_catalog(fs, root:str):
    path = f"{root}/stimulus_catalog.tsv"
    with fs.open(path, "w",newline="") as fh:
        w=csv.writer(fh,delimiter="\t")
        w.writerow(["iri","label","class_hint","chemical_formula",
                    "smiles","molecular_weight","default_dose","dose_unit"])
        for iri,label,cls in STIMULI:
            chem="C4H8O2" if "butyrate" in label else ""
            w.writerow([iri,label,cls,chem,"",random.randint(60,500),
                        random.choice([0.1,1,10]),"mM"])

def mk_ontology_catalog(fs, root:str):
    path = f"{root}/ontology_terms.tsv"
    with fs.open(path, "w",newline="") as fh:
        w=csv.writer(fh,delimiter="\t")
        w.writerow(["iri","label","ontology","definition","synonyms","version"])
        for iri,lbl in CELL_TYPES+TISSUES:
            ont="CL" if iri.startswith("CL") else "UBERON"
            w.writerow([iri,lbl,ont,"","","", "2025-06"])

# :::::::::::::::::::::::::::::::::: STUDIES ::::::::::::::::::::::::::::::::

def mk_study_catalog(fs, root:str, n_exp:int)->List[str]:
    studies=[]
    path = f"{root}/study_catalog.tsv"
    with fs.open(path, "w",newline="") as fh:
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

def init_link_files(fs, root:str) -> None:
    headers = {
        "sample_microbe.tsv":    "sample_id\tmicrobe_taxon_id\trelative_abundance_pct\tevidence\n",
        "sample_stimulus.tsv":   "sample_id\tstimulus_iri\texposure_time_hr\tresponse_marker\n",
        "microbe_stimulus.tsv":  "microbe_taxon_id\tstimulus_iri\tinteraction_score\tevidence\n",
    }
    for fname, hdr in headers.items():
        path = f"{root}/{fname}"
        with fs.open(path, "w") as fh:
            fh.write(hdr)


def append(fs, root:str, fname:str, row:List) -> None:
    path = f"{root}/{fname}"
    with fs.open(path, "a", newline="") as fh:
        csv.writer(fh, delimiter="\t").writerow(row)


def make_experiments(
    fs, root:str,
    studies:List[str],
    genes:List[str],
    base_uri:str, tz:str, ts_format:str
) -> None:
    
    for sid in studies:
        exp = f"experiment_{sid}"
        # 1) metadata TSV
        meta_path = f"{root}/{exp}.tsv"
        with fs.open(meta_path, "w", newline="") as fh:
            w = csv.writer(fh, delimiter="\t")
            w.writerow([
                "sample_id","study_id","cell_type_iri","cell_type_label",
                "tissue_iri","tissue_label","organism_iri","organism_label",
                "growth_condition","stimulus_iri","stimulus_label",
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
                
                # build a generic URI, then ensure the store “exists” via fsspec
                zarr_uri = f"{base_uri.rstrip('/')}/{ts(ts_format,tz)}/{exp}/{samp}.zarr"
                # ensure Zarr store path exists
                zfs, zroot = fsspec.core.url_to_fs(zarr_uri)
                try:
                    zfs.makedirs(zroot, exist_ok=True)
                except AttributeError:
                    pass


                coll_date=(dt.date.today()-dt.timedelta(days=random.randint(0,180))).isoformat()
                w.writerow([
                    samp,sid,
                    cell_iri,cell_lbl,
                    tissue_iri,tissue_lbl,
                    org_iri,org_lbl,
                    growth,
                    stim_iri,stim_lbl,
                    micro_tid,micro_name,
                    zarr_uri,coll_date,
                    random.randint(20,65),            # donor age
                    random.randint(1,3),              # replicate
                    round(random.uniform(70,95),1),   # viability
                    round(random.uniform(7,10),2)     # RIN
                ])
                # raw counts
                counts=nb_counts(len(genes),RAW_COUNT_MEAN,RAW_COUNT_THETA)
                raw_counts_path = f"{root}/{samp}_raw_counts.tsv"
                with fs.open(raw_counts_path, "w", newline="") as cf:
                    cw=csv.writer(cf,delimiter="\t")
                    cw.writerow(["gene_id","count"])
                    cw.writerows(zip(genes,counts))
                # link-tables
                # TODO: fs, root might need to be one argument in the next three append instructions
                append(fs, root, "sample_microbe.tsv", [samp,micro_tid,round(random.uniform(0.1,30),2),
                                random.choice(["mgnify","literature","inferred"])])
                append(fs, root, "sample_stimulus.tsv", [samp,stim_iri,random.randint(1,48),
                                random.choice(["IL6","NFkB","IFNγ"])])
        # Microbe–Stimulus edges (once per experiment)
        mi=random.choice(MICROBES);
        st=random.choice(STIMULI)
        append(fs, root, "microbe_stimulus.tsv", [mi[0],st[0],round(random.uniform(-1,1),3),
                        random.choice(["mgnify","literature","inferred"])])

# ::::::::::::::::::::::::::::::::::::: MAIN :::::::::::::::::::::::::::::::

def main():
    ap=argparse.ArgumentParser(
        description="Generate synthetic TSVs and raw counts aligned with DW schema")
    ap.add_argument("-n","--num_experiments",type=int,default=1)
    ap.add_argument("--seed",type=int,help="random seed")
    ap.add_argument("--tz","--timezone",dest="tz",default=DEFAULT_TZ,
                    help="IANA time-zone used for the run folder timestamp "
                         "(default: %(default)s)")
    ap.add_argument("--ts-format",default=TIMESTAMP_FMT,
                    help="strftime() format for the run folder timestamp "
                         "(default: %(default)s)")
    ap.add_argument("--config",      default="config.yml",
                    help="YAML config with base_uri, tz, ts_format")
    ap.add_argument("-o","--out_dir",type=pathlib.Path,help="output directory which is prefixed by <base-uri>")
    ap.add_argument("-b","--base-uri",    dest="base_uri", default=None,
                    help="Base URI for all outputs (file://, s3://, gs://…)") 
    args=ap.parse_args()
    
    if args.seed is not None:
        random.seed(args.seed); np.random.seed(args.seed)
    else:
        random.seed(time.time_ns()&0xFFFF_FFFF)

    cfg  = load_config(args.config)

    # Merge CLI > config > defaults
    base_uri   = args.base_uri   or cfg.get("base_uri")      or DEFAULT_BASE_URI
    tz         = args.tz         or cfg.get("tz")            or DEFAULT_TZ
    ts_format  = args.ts_format  or cfg.get("ts_format")     or TIMESTAMP_FMT

    # Build a per-run root URI
    stamp     = ts(ts_format, tz)
    run_uri   = f"{base_uri.rstrip('/')}/{stamp}"
    fs, root  = fsspec.core.url_to_fs(run_uri)
    # ensure the directory exists (local: mkdir, remote: noop or bucket check)
    try: fs.makedirs(root, exist_ok=True)
    except AttributeError: pass

    genes=genes = mk_gene_catalog(fs, root, GENES_PER_RUN)
    mk_taxa_catalog(out)
    mk_microbe_catalog(out)
    mk_stimulus_catalog(out)
    mk_ontology_catalog(out)
    studies = mk_study_catalog(fs, root, args.num_experiments)
    init_link_files(fs, root)
    make_experiments(fs, root, studies, genes, base_uri=base_uri, tz=tz, ts_format=ts_format)

    print(f"✔  Synthetic data written to {run_uri}")

if __name__=="__main__":
    main()
