#!/usr/bin/env python3

import argparse
import logging
from pathlib import Path
from typing import Generator, List, Union

LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def discover_files(
    data_dir: Union[str, Path],
    extensions: List[str] = None
) -> Generator[Path, None, None]:
    """
    Recursively discover files in `data_dir` matching given extensions.

    :param data_dir: Root directory to scan.
    :param extensions: List of file extensions to include (e.g. ['.tsv', '.zarr']).
                       If None, defaults to ['.tsv', '.zarr'].
    :yield: Path objects for each matching file.
    """
    base = Path(data_dir).expanduser().resolve()
    if extensions is None:
        extensions = ['.tsv', '.zarr']

    LOGGER.info("🔍 Discovering files in %s with extensions %s", base, extensions)
    for ext in extensions:
        pattern = f"*{ext}"
        for path in base.rglob(pattern):
            if path.is_file():
                LOGGER.debug("Found file: %s", path)
                yield path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Discover raw data files for the Gut–Brain DW pipeline"
    )
    parser.add_argument(
        "--data-dir", 
        required=True,
        help="Root directory containing raw data files"
    )
    parser.add_argument(
        "--extensions", 
        nargs='+', 
        default=['.tsv', '.zarr'],
        help="File extensions to include (e.g. .tsv .zarr)"
    )
    args = parser.parse_args()

    for file_path in discover_files(args.data_dir, args.extensions):
        print(file_path)


if __name__ == '__main__':
    main()
