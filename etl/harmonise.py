#!/usr/bin/env python3

import re
import requests

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
