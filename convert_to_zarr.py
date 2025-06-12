#!/usr/bin/env python3

import os
temp_dir = os.path.expanduser("~/joblib_temp")
os.makedirs(temp_dir, exist_ok=True)
os.environ["JOBLIB_TEMP_FOLDER"] = temp_dir
print("JOBLIB_TEMP_FOLDER =", os.environ["JOBLIB_TEMP_FOLDER"])

import scanpy as sc

os.environ["JOBLIB_TEMP_FOLDER"] = os.path.expanduser("~/joblib_temp")

def convert_h5ad_to_zarr(root_dir):
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith(".h5ad"):
                h5ad_path = os.path.join(dirpath, filename)
                zarr_filename = filename.replace(".h5ad", ".zarr")
                zarr_path = os.path.join(dirpath, zarr_filename)

                if os.path.exists(zarr_path) and os.path.isdir(zarr_path):
                    try:
                        if os.path.exists(os.path.join(zarr_path, ".zattrs")):
                            print(f"⏩ Skipping (zarr exists): {filename}")
                            continue
                    except Exception as e:
                        print(f"⚠️ Error checking existing zarr for {filename}: {e}")


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
