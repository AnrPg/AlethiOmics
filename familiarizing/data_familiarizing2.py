#!/usr/bin/env python3

import pandas as pd
from scipy.io import mmread
import os
import random

# === CONFIG ===
data_dir = "raw_data/NCBI_GEO/GSE208438_SUPP/GSE208438_SA05_cellranger_outputs"
matrix_file = os.path.join(data_dir, "matrix.mtx")
genes_file = os.path.join(data_dir, "features.tsv")
barcodes_file = os.path.join(data_dir, "barcodes.tsv")

# === Step 1: Load matrix and labels ===
print("📥 Loading raw matrix...")
X = mmread(matrix_file).tocsc()  # genes x cells → sparse format

genes = pd.read_csv(genes_file, sep="\t", header=None)
genes.columns = ["gene_id", "gene_symbol", "feature_type"]

barcodes = pd.read_csv(barcodes_file, sep="\t", header=None)
barcodes.columns = ["cell_barcode"]

# === Step 2: Convert to DataFrame ===
print("🔄 Converting sparse matrix to dense...")
df = pd.DataFrame(X.T.toarray(), columns=genes["gene_symbol"])  # cells x genes
df.insert(0, "cell_barcode", barcodes["cell_barcode"])

# === Step 3: Preview ===
print("\n🔍 First 5 rows:")
print(df.head())

print("\n🎲 Random 5 rows:")
print(df.sample(5, random_state=42))  # consistent across runs

print(f"\n📐 Full matrix shape: {df.shape} (cells x genes)")

# === Step 4: Save to CSV ===
csv_path = os.path.join(data_dir, "expression_matrix.csv")
df.to_csv(csv_path, index=False)
print(f"\n✅ Exported full matrix to CSV: {csv_path}")
