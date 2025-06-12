#!/usr/bin/env python3

import os
import sys
from ftplib import FTP

def download_geo_raw_tar(gse_id):
    if not gse_id.startswith("GSE") or not gse_id[3:].isdigit():
        print("❌ Invalid GEO Series ID. Format: GSE123553")
        return

    gse_numeric = int(gse_id[3:])
    gse_prefix = f"GSE{gse_numeric // 1000}nnn"
    ftp_host = "ftp.ncbi.nlm.nih.gov"
    ftp_dir = f"/geo/series/{gse_prefix}/{gse_id}/suppl/"
    raw_tar = f"{gse_id}_RAW.tar"
    local_dir = f"{gse_id}_RAW"
    os.makedirs(local_dir, exist_ok=True)
    local_tar_path = os.path.join(local_dir, raw_tar)

    print(f"🔗 Connecting to: ftp://{ftp_host}{ftp_dir}")
    ftp = FTP(ftp_host)
    ftp.login()

    try:
        ftp.cwd(ftp_dir)
    except Exception as e:
        print(f"❌ Could not access FTP directory: {ftp_dir}")
        print("Reason:", e)
        return

    if raw_tar not in ftp.nlst():
        print(f"❌ File {raw_tar} not found in FTP directory.")
        return

    print(f"📦 Downloading {raw_tar} ...")
    with open(local_tar_path, "wb") as f:
        ftp.retrbinary(f"RETR {raw_tar}", f.write)
    print(f"✅ Downloaded to {local_tar_path}")

    ftp.quit()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python download_geo_raw_tar.py <GSE_ID>")
        sys.exit(1)

    gse_id = sys.argv[1]
    download_geo_raw_tar(gse_id)
