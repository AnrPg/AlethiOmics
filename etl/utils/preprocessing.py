#!/usr/bin/env python3

import re
from typing import List, Optional, Union
import unicodedata

# ---------------------------------------------------------------------
#  strip_version  – remove “.17” (or any dot-suffix) from Ensembl IDs
# ---------------------------------------------------------------------
_ENS_VERSION_RE = re.compile(r"^([A-Z]{2,4}\d{6,})(?:\.\d+)?$")

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

def lowercase_ascii(text: Optional[str]) -> Optional[str]:
    """
    Convert *text* to plain ASCII lowercase.

        • NFKD-normalize so 'β-alanine' → 'beta-alanine'
        • encode/decode to drop any remaining non-ASCII
        • strip leading/trailing whitespace

    Returns None if input is None or empty/whitespace only.
    """
    if text is None:
        return None

    # Unicode → decomposed ASCII
    norm = (
        unicodedata.normalize("NFKD", text)
        .encode("ascii", "ignore")
        .decode("ascii")
        .strip()
        .lower()
    )
    return norm or None           # return None for empty string