#!/usr/bin/env python3


# import anndata
# import scanpy
# import anndata
import zarr

print("Up and running...")
# This script is designed to familiarize with the structure of a Zarr file
# created from a .h5ad file, specifically for the Parkinson's organoid dataset.
# It reads the .h5ad file in a memory-efficient way and exports it to Zarr format.
# It then explores the Zarr file structure, printing out categorical fields
# and their values, while avoiding loading the entire dataset into memory.

# Open the .h5ad file in read-only backed mode
# adata = ad.read_h5ad("raw_data/CELLxGENE/parkinson_vs_normal.h5ad", backed='r')

# print("Data loaded in backed mode. Exploring structure...")
# Print basic information about the AnnData object

# Export to Zarr format without loading full data into memory
# adata.write_zarr("parkinson_organoid_data.zarr")
# print("Data exported to Zarr format.")
# The Zarr file is now created, and we can explore its structure without loading the full dataset into memory.

print("Now exploring the Zarr file structure...")
# Reopen the Zarr file to explore its structure
# adata = ad.read_zarr("parkinson_organoid_data.zarr")
# Open the Zarr file
# root = zarr.open("parkinson_organoid_data.zarr", mode="r")
root = zarr.open("raw_data/CELLxGENE/dementia/astrocyte_DLPFC.zarr", mode="r")

def print_and_log(message, logfile="./data_familiarizing_astrocyte_DLPFC.log"):
    """It prints a message to the console and logs it to a file.

    Args:
        message (string): the message to print and log.
        logfile (string, optional): the full path to the log file . Defaults to None.
    """
    if not isinstance(message, str):
        message = str(message)
    print(message)
    with open(logfile, "a") as log_file:
        log_file.writelines(line + "\n" for line in message.splitlines())


# Print tree structure
print_and_log(str(root.tree()))

# ----------------------------
# Function to decode Zarr scalar (0D array)
# ----------------------------
def read_scalar(zarr_array):
    val = zarr_array[()]
    return val.decode() if isinstance(val, bytes) else val

# ----------------------------
# Function to read and print categorical fields
# ----------------------------
def print_categories(zgroup, path_prefix=""):
    for key in zgroup.keys():
        try:
            subgroup = zgroup[key]
            if isinstance(subgroup, zarr.hierarchy.Group) and "categories" in subgroup and "codes" in subgroup:
                codes = set(subgroup["codes"])
                categories = subgroup["categories"]
                values = set(categories[i] for i in codes)
                if len(codes) < 30 and len(categories) < 30 and len(values) > 0:
                    print_and_log(f"\n{path_prefix}{key}: {sorted(values)}")
                else:
                    print_and_log(f"\n{path_prefix}{key}: too many unique values ({len(values)}) to display")
                    print_and_log(f"\nPrinting the first 10 unique values for {path_prefix}{key}: {sorted(values)[:10]}")
            elif isinstance(subgroup, zarr.hierarchy.Group):
                # Recursively check subgroups
                print_categories(subgroup, path_prefix + key + ".")
            else:
                # Handle scalar or non-categorical arrays
                if subgroup.ndim == 0:
                    scalar_value = read_scalar(subgroup)
                    if isinstance(scalar_value, str) and len(scalar_value) < 100:
                        print_and_log(f"{path_prefix}{key}: {scalar_value}")
                else:
                    # For non-scalar arrays, we can print the shape or type
                    print_and_log(f"{path_prefix}{key}: {subgroup.shape} (not categorical)")
        except Exception as e:
            print(f"Error reading {path_prefix}{key}: {e}")

# ----------------------------
# Extract all categorical fields from obs, var, and raw/var
# ----------------------------
print_and_log("\n[obs] categorical fields:")
print_categories(root["obs"], path_prefix="obs.")

print_and_log("\n[var] categorical fields:")
print_categories(root["var"], path_prefix="var.")


