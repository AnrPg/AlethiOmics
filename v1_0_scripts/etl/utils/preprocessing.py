#!/usr/bin/env python3

import re
from typing import List, Optional, Union
import unicodedata

# ---------------------------------------------------------------------
#  strip_version  – remove “.17” (or any dot-suffix) from Ensembl IDs
# ---------------------------------------------------------------------
_ENS_VERSION_RE = re.compile(r"^([A-Za-z]{2,4}\d{6,})(?:\.\d+)?$")

def strip_version(gene_id: Union[str, None]) -> Optional[str]:
    """
    Remove the version suffix from Ensembl-style identifiers.

        ENSG00000123456.3   →  ENSG00000123456
        ENSMUSG00000017167  →  ENSMUSG00000017167   (unchanged, no version)

    Returns
    -------
    str | None
        • canonical ID without the version part, if pattern matches
        • original string (trimmed) if no version was present
        • None for empty / None input
    """
    if gene_id is None:
        return None
    gene_id = gene_id.strip()
    if not gene_id:
        return None

    m = _ENS_VERSION_RE.match(gene_id)
    if m:
        return m.group(1)      # the part before ".<ver>"
    return gene_id             # not an Ensembl ID – leave untouched


# ---------------------------------------------------------------------
#  split_commas  – turn “A,B , C” into ["A", "B", "C"]
# ---------------------------------------------------------------------
def split_commas(raw: Union[str, None]) -> List[str]:
    """
    Split a comma-separated string into a clean list of tokens.

    • Trims whitespace around each token
    • Drops empty elements (e.g. double commas)
    • Returns [] on empty / None input

        "HANCESTRO:0005, HANCESTRO:0008"
            → ["HANCESTRO:0005", "HANCESTRO:0008"]
    """
    if raw is None:
        return []
    tokens = [tok.strip() for tok in str(raw).split(",")]
    return [tok for tok in tokens if tok]        # remove blanks

# ---------------------------------------------------------------------
#  normalize case  – convert text to plain ASCII lowercase
# ---------------------------------------------------------------------

GREEK_TO_ASCII = {
    "α": "alpha",  "β": "beta",   "γ": "gamma", "δ": "delta",
    "ε": "epsilon","ζ": "zeta",   "η": "eta",   "θ": "theta",
    "ι": "iota",   "κ": "kappa",  "λ": "lambda","μ": "mu",
    "ν": "nu",     "ξ": "xi",     "ο": "omicron","π": "pi",
    "ρ": "rho",    "σ": "sigma",  "ς": "sigma", "τ": "tau",
    "υ": "upsilon","φ": "phi",    "χ": "chi",   "ψ": "psi",
    "ω": "omega",
    
    "Α": "alpha",  "Β": "beta",   "Γ": "gamma", "Δ": "delta",
    "Ε": "epsilon","Ζ": "zeta",   "Η": "eta",   "Θ": "theta",
    "Ι": "iota",   "Κ": "kappa",  "Λ": "lambda","Μ": "mu",
    "Ν": "nu",     "Ξ": "xi",     "Ο": "omicron","Π": "pi",
    "Ρ": "rho",    "Ο": "sigma",  "Σ": "sigma", "Τ": "tau",
    "Υ": "upsilon","Φ": "phi",    "Χ": "chi",   "Ψ": "psi",
    "Ω": "omega"
}

PUNCT_MAP = {
    # dashes
    "\u2010": "-", "\u2011": "-", "\u2012": "-", "\u2013": "-",
    "\u2014": "-", "\u2212": "-",
    # quotes
    "“": '"', "”": '"', "„": '"', "‟": '"', "«": '"', "»": '"',
    "‘": "'", "’": "'", "‚": "'", "‹": "'", "›": "'",
    # misc symbols
    "×": "x",  "·": ".",  "±": "+/-", "µ": "u", "°": "deg",
    "\u00A0": " ",
}
_PUNCT_REGEX = re.compile("|".join(map(re.escape, PUNCT_MAP)))
_greek_pattern = re.compile("|".join(map(re.escape, GREEK_TO_ASCII)))

def tidy_punct(text: str) -> str:
    """Translate non-ASCII punctuation to ASCII equivalents."""
    return _PUNCT_REGEX.sub(lambda m: PUNCT_MAP[m.group(0)], text)


def ascii_slug(text: str) -> str | None:
    """
    → Lower-case ASCII with Greek letters spelled out.
    Returns None on blank / None input.
    """
    if not text or text.strip() == "":
        return None

    # 1) swap Greek letters for their ASCII names
    text = _greek_pattern.sub(lambda m: GREEK_TO_ASCII[m.group(0)], text)

    return re.sub(r"\s+", " ", text).strip().lower()

def lowercase_ascii(text: Optional[str]) -> Optional[str]:
    """
    Convert *text* to plain ASCII lowercase.

        • NFKD-normalize so 'β-alanine' → 'beta-alanine'
        • encode/decode to drop any remaining non-ASCII
        • strip leading/trailing whitespace

    Returns None if input is None or empty/whitespace only.
    """
    if text is None or not text.strip() or text == "":
        return None

    norm = (
        unicodedata.normalize("NFKD", ascii_slug(tidy_punct(text)))
        .encode("ascii", "ignore")
        .decode("ascii")
        .strip()
        .lower()
    )
    return norm or None           # return None for empty string