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
import json
import os
import subprocess
import fsspec, yaml
import argparse, csv, datetime as dt, pathlib, random, string, sys, time
from typing import List, Dict, Tuple
import numpy as np
from zoneinfo import ZoneInfo
from typing import List, Dict, Tuple
import numpy as np
import zarr
from etl.utils.log import get_logger

# ::::::::::::::::::::::::::::::::::::::: GLOBALS :::::::::::::::::::::::::::
DEFAULT_TZ        = "Europe/Athens"
TIMESTAMP_FMT     = "%Y%m%d-%H%M%S"
DEFAULT_BASE_URI = os.environ.get("BASE_URI", f"file://./raw_data/synthetic_runs")


# :::::::::::::::::::::::::::::::::::::::::::: CONFIG :::::::::::::::::::::::

GENES_PER_RUN       = 10 # *fallback* size – ignored if --zarr-dir supplied
SAMPLES_PER_EXP     = 5
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
    ("UBERON_0001155", "colon"),
    ("UBERON_0002108", "small intestine"),
    ("UBERON_0000955", "brain"),
]
STIMULI: List[Tuple[str,str,str]] = [
    ("CHEBI_17968",     "butyrate",                     "SCFA",             "C4H7O2-", "CCCC(=O)[O-]", "110.09"),
    ("CHEBI_17272",     "propionate",                   "SCFA",             "C3H5O2-", "CCC(=O)[O-]", "73.07"),
    ("CHEBI_30089",     "acetate",                      "SCFA",             "C2H3O2–", "CC(=O)[O-]", "59.04"),
    ("CHEBI_16865",     "gamma-aminobutyric acid",      "neurotransmitter", "C4H9NO2", "NCCCC(=O)O", "103.12"),
    ("PR_000026791",    "tumour necrosis factor-alpha", "cytokine",         "", "", "17.3"),
    ("NONE",            "none",                         "",                 "", "", ""),
]
MICROBES: List[Tuple[int,str,str,str]] = [
    ("NCBITaxon_226186",    "Bacteroides thetaiotaomicron", "Bt-VPI5482",  "anaerobe"),
    ("NCBITaxon_474186",    "Enterococcus faecalis",        "Ef-OG1RF",    "facultative"),
    ("NCBITaxon_83333",     "Escherichia coli",             "Ecoli-K12",   "facultative"),
    ("NCBITaxon_568703",    "Lactobacillus rhamnosus",      "LGG",         "anaerobe"),
    ("NCBITaxon_853",       "Faecalibacterium prausnitzii", "Fp-A2-165",   "anaerobe"),
]
TAXA: List[Tuple[int,str,str]] = [
    # !!! Species -Level IDs:
    ("NCBITaxon_9606",      "Homo sapiens",         "Eukaryota","Species"),
    ("NCBITaxon_226186",    "B. thetaiotaomicron",  "Bacteria", "Species"),
    ("NCBITaxon_1351",      "E. faecalis",          "Bacteria", "Species"),
    ("NCBITaxon_562",       "E. coli",              "Bacteria", "Species"),
    ("NCBITaxon_47715",     "L. rhamnosus",         "Bacteria", "Species"),
    ("NCBITaxon_853",       "F. prausnitzii",       "Bacteria", "Species"),
    # !!! Strain-Level IDs:
    ("NCBITaxon_226186",    "Bt-VPI5482",           "Bacteria", "Strain"),
    ("NCBITaxon_474186",    "Ef-OG1RF",             "Bacteria", "Strain"),
    ("NCBITaxon_83333",     "Ecoli-K12",            "Bacteria", "Strain"),
    ("NCBITaxon_568703",    "LGG",                  "Bacteria", "Strain"),
    ("NCBITaxon_853",       "Fp-A2-165",            "Bacteria", "Strain"),
]

# ::::::::::::::::::::::::::::::::::::::: HELPERS :::::::::::::::::::::::::::

# Helper: IRI for *Homo sapiens* – used by Genes rows -----------------------
HUMAN_TAXON_IRI = next(t[0] for t in TAXA if t[0].endswith("9606"))

# grab a module‐scoped logger
logger = get_logger(__name__)

def ts(fmt: str = TIMESTAMP_FMT, tz: str = DEFAULT_TZ) -> str:
    """
    Return a timestamp string in the requested *fmt* and *tz* (IANA name).
    Falls back to UTC if the zone isn’t recognised.
    """
    try:
        z = ZoneInfo(tz)
    except Exception: # pragma: no cover
        z = dt.timezone.utc
    return dt.datetime.now(z).strftime(fmt)

def rnd_id(prefix: str,length:int=6)->str:
    return prefix+"".join(random.choices(string.ascii_uppercase+string.digits,k=length))

def nb_counts(n:int, mean:int, theta:int)->np.ndarray:
    r=theta; p=r/(r+mean)
    return np.random.negative_binomial(r,p,size=n)

def make_date_pool(n_dates: int, start: dt.date, end: dt.date) -> List[dt.date]:
    """Return a list of n_dates random dates between start and end."""
    start_ord = start.toordinal()
    end_ord = end.toordinal()
    return [dt.date.fromordinal(random.randint(start_ord, end_ord)).isoformat()
            for _ in range(n_dates)]

def _harmonise_gene_name(name: str) -> str:
    """Drop ENSG version or strip whitespace – very light touch for demo."""
    return name.split(".")[0].strip()

def load_config(path: pathlib.Path) -> dict:
    """
    If `path` ends in .age, run `age --decrypt` (using $AGE_IDENTITY or default).
    Otherwise load plaintext YAML.
    """ 
    data: str | None = None
    if path.suffix == ".age":
        # determine identity file
        identity = os.environ.get("AGE_IDENTITY")  # e.g. /home/user/key.pub or /.config/key.txt or whatever...
        cmd = ["age"]
        if identity:
            cmd += ["--identity", identity]
        cmd += ["--decrypt", str(path)]
        print(f"\t\t############ Running: {cmd!r}")
        # decrypt into memory
        proc = subprocess.run(cmd, check=True, stdout=subprocess.PIPE)
        data = proc.stdout.decode()
    else:
        data = path.read_text()
    return yaml.safe_load(data) or {}

def _genes_from_zarr(zarr_dir: pathlib.Path) -> List[str]:
    """Try to infer gene list from the first *.zarr found inside *zarr_dir*."""
    if zarr is None:
        raise RuntimeError("zarr not installed – cannot extract genes from .zarr")

    zarr_paths = sorted(zarr_dir.glob("*.zarr"))
    if not zarr_paths:
        raise FileNotFoundError(f"No .zarr store found in {zarr_dir}")

    gpath = zarr_paths[0]
    grp = zarr.open_group(str(gpath), mode="r")

    # Common AnnData layout: /var/_index (1‑D string array)
    if "var" in grp and "_index" in grp["var"]:
        genes = [g.decode() if isinstance(g, bytes) else str(g) for g in grp["var"]["_index"][:]]
    else:
        # Fallback: any 1‑D array called gene_ids / genes / names, etc.
        for candidate in ["gene_ids", "genes", "names", "feature_name"]:
            if candidate in grp:
                genes = [g.decode() if isinstance(g, bytes) else str(g) for g in grp[candidate][:]]
                break
        else:
            raise RuntimeError("Could not locate gene list inside .zarr store.")
    return [_harmonise_gene_name(g) for g in genes]


# :::::::::::::::::::::::::::::::::: CATALOGS :::::::::::::::::::::::::::::::

def mk_gene_catalog(fs, root:str, n:int = GENES_PER_RUN, zarr_dir: str | None = None,) -> List[str]:
    
    logger.debug("mk_gene_catalog: writing %d genes to %s", n, root)
    
    if zarr_dir:
        genes_in = _genes_from_zarr(pathlib.Path(zarr_dir))
        n = len(genes_in)
    else:
        genes_in = []  # will fabricate below
        
    out_path = f"{root}/gene_catalog.tsv"
    genes_out: List[str] = []

    with fs.open(out_path, "w", newline="") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow(
            [
                "gene_accession",
                "gene_name",
                "species_taxon_iri",
                "gene_length_bp",
                "gc_content_pct",
                "pathway_iri",
                "go_terms",
            ]
        )

        if genes_in:
            for idx, gname in enumerate(genes_in, 1):
                gid = f"ENSG{idx:011d}"
                w.writerow(
                    [
                        gid,
                        gname,
                        HUMAN_TAXON_IRI,  # <- FK to Taxa
                        random.randint(500, 200_000),
                        round(random.uniform(35, 65), 2),
                        "KEGG:hsa" + str(random.randint(1000, 9999)),
                        "|".join([f"GO:{random.randint(1000000, 9999999)}" for _ in range(3)]),
                    ]
                )
                genes_out.append(gid)
        else:
            # fallback synthetic catalogue
            genes_out = [f"ENSG{str(i).zfill(11)}" for i in random.sample(range(1, 30_000), n)]
            for gid in genes_out:
                w.writerow(
                    [
                        gid,
                        f"Gene{gid[-4:]}",
                        HUMAN_TAXON_IRI,
                        random.randint(500, 200_000),
                        round(random.uniform(35, 65), 2),
                        "KEGG:hsa" + str(random.randint(1000, 9999)),
                        "|".join([f"GO:{random.randint(1000000, 9999999)}" for _ in range(3)]),
                    ]
                )
    return genes_out


def mk_taxa_catalog(fs, root:str) -> List[str]:
    logger.debug("mk_taxa_catalog: root=%s", root)
    path = f"{root}/taxa_catalog.tsv"
    out: List[str] = []
    with fs.open(path, "w", newline="") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow(["iri","species_name","kingdom","ranking","gc_content",
                    "genome_length","habitat","pathogenicity"])
        for iri,name,kingdom,ranking in TAXA:
            w.writerow([iri,name,kingdom,ranking,
                        round(random.uniform(30,65),2),
                        random.randint(2_000_000,5_500_000),
                        "intestine" if kingdom=="Bacteria" else "human body",
                        random.choice(["commensal","opportunist","pathogen"])])
            out.append(iri)
    return out
def mk_microbe_catalog(fs, root:str) -> List[str]:
    logger.debug("mk_microbe_catalog: root=%s", root)
    path = f"{root}/microbe_catalog.tsv"
    out: List[str] = []
    with fs.open(path, "w", newline="") as fh:
        w=csv.writer(fh,delimiter="\t")
        w.writerow(["taxon_iri","strain_name",
                    "genome_size_bp","oxygen_requirement",
                    "abundance_index","prevalence",
                    "culture_collection","genome_assembly_accession",
                    "habitat","optimal_growth_temp",
                    "doubling_time","metabolic_profile_iri",])
        for tid,_,strain,oxy in MICROBES:
            w.writerow([tid,
                        strain,
                        random.randint(2_000_000,5_000_000),
                        oxy,
                        round(random.uniform(0.01,100),4),
                        round(random.uniform(5,100)),
                        f"DSMZ:{random.randint(1000, 9999)}",
                        f"GCF_{random.randint(1000000, 9999999)}.{random.randint(1,9)}",
                        random.choice(["intestine", "oral cavity", "soil", "skin"]),
                        round(random.uniform(25.0, 45.0), 1),  # °C
                        round(random.uniform(0.2, 4.0), 2),    # h
                        f"KEGG:map{random.randint(9000, 9999)}",])
            out.append(tid)
    return out

def mk_stimulus_catalog(fs, root:str) -> List[str]:
    logger.debug("mk_stimulus_catalog: root=%s", root)
    path = f"{root}/stimulus_catalog.tsv"
    out: List[str] = []
    with fs.open(path, "w",newline="") as fh:
        w=csv.writer(fh,delimiter="\t")
        w.writerow(["iri","label","class_hint","chem_formula",
                    "smiles","molecular_weight","default_dose","dose_unit"])
        for iri,label,cls,chem,smiles,molc_weight in STIMULI:
            w.writerow([iri,label,cls,chem,smiles,molc_weight,
                        random.choice([0.1,1,10]),"mM"])
            out.append(iri)
    return out

def mk_ontology_catalog(fs, root:str) -> List[str]:
    logger.debug("mk_ontology_catalog: root=%s", root)
    path = f"{root}/ontology_terms.tsv"
    out: List[str] = []
    with fs.open(path, "w",newline="") as fh:
        w=csv.writer(fh,delimiter="\t")
        w.writerow(["iri","label","ontology","term_definition","synonyms","onto_version"])
        for iri,lbl in CELL_TYPES+TISSUES:
            ont="CL" if iri.startswith("CL") else "UBERON"
            w.writerow([iri,lbl,ont,f"Definition of {lbl}",json.dumps([f"{lbl}_syn{n}" for n in range(2)]), "2025-06"])
            out.append(iri)
    return out

# :::::::::::::::::::::::::::::::::: STUDIES ::::::::::::::::::::::::::::::::

def mk_study_catalog(fs, root:str, n_exp:int)->List[str]:
    """Return mapping {study_id: publication_date} (ISO date strings)."""
    
    logger.debug("mk_study_catalog: n_exp=%d, root=%s", n_exp, root)
    studies: Dict[str, str] = {}
    path = f"{root}/study_catalog.tsv"
    date_pool = make_date_pool(
        n_dates=10,
        start=dt.date(2008, 1, 1),
        end=dt.date(2025, 6, 25)
    )

    with fs.open(path, "w",newline="") as fh:
        w=csv.writer(fh,delimiter="\t")
        w.writerow(["iri","title","source_repo",
                    "publication_date","study_type",
                    "num_samples","contact_email"])
        for _ in range(n_exp):
            sid=f"E-MTAB-{random.randint(10000,99999)}"
            title=f"Synthetic gut-brain experiment {sid}"
            pub_date = random.choice(date_pool)
            w.writerow([sid,title,"LOCAL",
                        pub_date,
                        "scRNA-seq",
                        SAMPLES_PER_EXP,
                        f"{sid.lower()}@example.org"])
            studies[sid] = pub_date                
    return studies

# ::::::::::::::::::::::::::::::::: LINK TABLES :::::::::::::::::::::::::::::

def init_link_files(fs, root:str) -> None:
    headers = {
        # TODO: check header names are correct
        "sample_microbe.tsv":    "sample_id\tmicrobe_id\tevidence\trelative_abundance\n",
        "sample_stimulus.tsv":   "sample_id\tstimulus_id\texposure_time_hr\tresponse_marker\n",
        "microbe_stimulus.tsv":  "microbe_id\tstimulus_id\tevidence\tinteraction_score\n",
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
    
    logger.debug("make_experiments: %d studies, %d genes, root=%s", len(studies), len(genes), root)


    RESPONSE_MARKERS = ["IL6", "NFkB", "MAPK", "cFos", "none"]
    
    for sid, pub_date in studies.items():
        logger.debug("→ experiment %s (pub_date=%s)", sid, pub_date)
        exp = f"experiment_{sid}"
        
        # Pre‑compute per‑study collection date pool ----------------------
        base = dt.date.fromisoformat(pub_date)
        k = random.choice([1, 2, 4])
        coll_dates = [base + dt.timedelta(days=i) for i in random.sample(range(1, 15), k)]

        # metadata TSV
        meta_path = f"{root}/{exp}.tsv"
        with fs.open(meta_path, "w", newline="") as fh:
            w = csv.writer(fh, delimiter="\t")
            w.writerow([
                "iri","study_iri","cell_type_iri","tissue_iri",
                "organism_iri","growth_condition","raw_counts_uri",
                "collection_date","donor_age_years",
                "replicate_number","viability_pct","rin_score"
            ])
            for sample_id in range(SAMPLES_PER_EXP):
                samp=rnd_id("SAMP",8)
                logger.debug("  writing sample %s for %s", samp, sid)
                cell_iri,_=random.choice(CELL_TYPES)
                tissue_iri,_=random.choice(TISSUES)
                org_iri="NCBITaxon:9606"
                growth=random.choice(["monoculture","co-culture"]) 
                microbe_tid,_, _, _ =random.choice(MICROBES)
                stimulus_id=random.randint(0,len(STIMULI))            
                
                # build a generic URI, then ensure the store “exists” via fsspec
                raw_counts_uri = f"{root}/{samp}_raw_counts.tsv"
                # # ensure raw_counts_uri (Zarr or TSV) store path exists
                rc_fs, rc_path = fsspec.core.url_to_fs(raw_counts_uri)
                parent = os.path.dirname(rc_path)
                try:
                    rc_fs.makedirs(parent, exist_ok=True)
                except AttributeError:
                    pass

                coll_date = random.choice(coll_dates).isoformat()

                # Write sample metadata row --------------------------------
                w.writerow([
                    samp,
                    sid,
                    cell_iri,
                    tissue_iri,
                    org_iri,
                    growth,                                        
                    raw_counts_uri,
                    coll_date,
                    random.randint(20,65),            # donor age
                    random.randint(1,3),              # replicate
                    round(random.uniform(70,97),2),   # viability
                    round(random.uniform(7,10),3)     # RIN
                ])
                # Raw counts for this sample
                counts=nb_counts(len(genes),RAW_COUNT_MEAN,RAW_COUNT_THETA)
                with fs.open(raw_counts_uri, "w", newline="") as cf:
                    cw=csv.writer(cf,delimiter="\t")
                    cw.writerow(["gene_id","count"])
                    cw.writerows(zip(genes,counts))
                # link-tables
                append(fs, root, "sample_microbe.tsv", [sample_id,random.randint(0,len(MICROBES)),random.choice(["mgnify","literature","inferred"]),round(random.uniform(0.01,300),4)])
                append(fs, root, "sample_stimulus.tsv", [sample_id,random.randint(0,len(STIMULI)),round(random.uniform(1,24*10),1),random.choice(RESPONSE_MARKERS),])
        # Microbe–Stimulus edges (once per experiment)
        logger.debug("  appending microbe_stimulus for %s", sid)
        microbe_tid=random.randint(0,len(MICROBES))
        stimulus_id=random.randint(0,len(STIMULI))

        append(fs, root, "microbe_stimulus.tsv", [microbe_tid,stimulus_id,random.choice(["mgnify","literature","inferred"]),round(random.uniform(-30,40),3)])

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
    ap.add_argument("-z","--zarr-dir", help="Directory containing at least one *.zarr store with gene names", default=None)
    args=ap.parse_args()
    
    if args.seed is not None:
        random.seed(args.seed)
        np.random.seed(args.seed)
    else:
        random.seed(time.time_ns()&0xFFFF_FFFF)

    cfg = load_config(pathlib.Path(args.config)) if pathlib.Path(args.config).exists() else {}


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
    except AttributeError: pass # remote FS (e.g. S3) – bucket already exists

    genes = mk_gene_catalog(fs, root, GENES_PER_RUN, zarr_dir=args.zarr_dir)
    mk_taxa_catalog(fs, root)
    mk_microbe_catalog(fs, root)
    mk_stimulus_catalog(fs, root)
    mk_ontology_catalog(fs, root)
    studies = mk_study_catalog(fs, root, args.num_experiments)
    init_link_files(fs, root)
    make_experiments(fs, root, studies, genes, base_uri=base_uri, tz=tz, ts_format=ts_format)

    print(f"✔  Synthetic data written to {run_uri}")

if __name__=="__main__":
    main()
