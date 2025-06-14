#!/usr/bin/env python3

import os
import sys
from ftplib import FTP

def download_geo_supp_file(gse_id, filename):
    if not gse_id.startswith("GSE") or not gse_id[3:].isdigit():
        print("❌ Invalid GEO Series ID. Format must be like: GSE123553")
        return

    gse_numeric = int(gse_id[3:])
    gse_prefix = f"GSE{gse_numeric // 1000}nnn"
    ftp_host = "ftp.ncbi.nlm.nih.gov"
    ftp_path = f"/geo/series/{gse_prefix}/{gse_id}/suppl/"
    download_dir = f"{gse_id}_SUPP"
    os.makedirs(download_dir, exist_ok=True)
    local_path = os.path.join(download_dir, filename)

    print(f"🔗 Connecting to: ftp://{ftp_host}{ftp_path}")

    ftp = FTP(ftp_host)
    ftp.login()

    try:
        ftp.cwd(ftp_path)
    except Exception as e:
        print(f"❌ Could not access FTP directory: {ftp_path}")
        print("Reason:", e)
        return

    file_list = ftp.nlst()
    if filename not in file_list:
        print(f"❌ File {filename} not found in FTP directory.")
        print("Available files:", file_list)
        return

    print(f"📦 Downloading {filename} ...")
    with open(local_path, "wb") as f:
        ftp.retrbinary(f"RETR {filename}", f.write)
    print(f"✅ Downloaded to {local_path}")

    ftp.quit()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python download_geo_supp_file.py <GSE_ID> <filename>")
        sys.exit(1)

    gse_id = sys.argv[1]
    filename = sys.argv[2]
    download_geo_supp_file(gse_id, filename)
