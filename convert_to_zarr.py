import os
import scanpy as sc

def convert_h5ad_to_zarr(root_dir):
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith(".h5ad"):
                h5ad_path = os.path.join(dirpath, filename)
                zarr_filename = filename.replace(".h5ad", ".zarr")
                zarr_path = os.path.join(dirpath, zarr_filename)

                try:
                    print(f"Reading: {h5ad_path}")
                    adata = sc.read_h5ad(h5ad_path)

                    print(f"Writing: {zarr_path}")
                    adata.write_zarr(zarr_path)  # ← no compression arg

                    print("✓ Done\n")
                except Exception as e:
                    print(f"✗ Error converting {h5ad_path}: {e}\n")

if __name__ == "__main__":
    convert_h5ad_to_zarr(".")
