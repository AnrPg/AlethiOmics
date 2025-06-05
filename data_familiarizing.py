#!/usr/bin/env python3


import anndata as ad

# Open the .h5ad file in read-only backed mode
adata = ad.read_h5ad("raw_data/CELLxGENE/fc63b06e-a3bf-4978-bc65-e5cd5aad6071.h5ad", backed='r')

# Export to Zarr format without loading full data into memory
adata.write_zarr("intestine_organoid_data.zarr")
