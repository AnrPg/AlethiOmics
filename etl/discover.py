#!/usr/bin/env python3

from pathlib import Path

def discover(landing_dir="raw_data"):
    for path in Path(landing_dir).rglob("*"):
        if path.suffix in {".zarr", ".tsv", ".txt"}:
            yield path
        elif path.is_dir() and not path.name.startswith("."):
            # Recursively yield files in subdirectories
            yield from discover(path)
        elif path.is_file() and not path.name.startswith("."):
            # Yield individual files that are not hidden
            yield path
        else:
            # Skip hidden files and directories
            continue
            pass
    return