#!/usr/bin/env python3

# etl/extract.py
# ───────────────────────────────────────────────────────────────────────────
"""
Extraction stage for the Gut–Brain Organoid Data-Warehouse pipeline.

Reads the flat files discovered in *raw_data/*** (synthetic or real),
converts them to dict records keyed by **DB column names**, groups them in
batches, and yields `(table_name, [row_dict, …])` to the Harmonizer.

Usage
-----
>>> from etl.extract import Extractor
>>> for table, batch in Extractor("raw_data/synthetic_data",
...                               mode="metadata",
...                               batch_size=500).iter_batches():
...     Harmonizer.apply(table, batch)

A CLI wrapper is provided for ad-hoc runs:

    python -m etl.extract --data-dir raw_data/synthetic_data \\
                          --mode       all                  \\
                          --batch-size 1000
"""
from __future__ import annotations

import argparse
import csv
import itertools
import logging
import pathlib
from typing import Dict, Generator, Iterable, List, Tuple

LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

# ───────────────────────────────────────────────────────────────────────────
# Helper: filename → logical warehouse table
# ───────────────────────────────────────────────────────────────────────────
_TABLE_MAP: List[Tuple[str, str]] = [
    ("gene_catalog.tsv",        "Genes"),
    ("taxa_catalog.tsv",        "Taxa"),
    ("microbe_catalog.tsv",     "Microbes"),
    ("stimulus_catalog.tsv",    "Stimuli"),
    ("ontology_terms.tsv",      "OntologyTerms"),
    ("study_catalog.tsv",       "Studies"),
    ("sample_microbe.tsv",      "SampleMicrobe"),
    ("sample_stimulus.tsv",     "SampleStimulus"),
    ("microbe_stimulus.tsv",    "MicrobeStimulus"),
    ("experiment_",             "Samples"),        # prefix match
    ("_raw_counts.tsv",         "RawCounts"),      # special table (not in DB)
]


def _table_for(fname: str) -> str | None:
    """Return the warehouse table indicated by *fname*, or None if unknown."""
    for pattern, table in _TABLE_MAP:
        if pattern in fname:
            return table
    return None


# ───────────────────────────────────────────────────────────────────────────
# Extractor
# ───────────────────────────────────────────────────────────────────────────
class Extractor:
    """
    Stream TSV → batches of dicts.

    Parameters
    ----------
    data_dir :
        Root directory that contains the raw `.tsv` and `.zarr` files
        discovered earlier by *etl.discover*.
    mode :
        'metadata' → skip `*_raw_counts.tsv`
        'raw_counts' → only those
        'all' → everything
    batch_size :
        How many rows to emit per `(table, rows)` batch.
    """

    #: recognised operating modes
    _MODES = {"metadata", "raw_counts", "all"}

    def __init__(
        self,
        data_dir: str | pathlib.Path,
        mode: str = "all",
        batch_size: int = 1_000,
    ):
        self.data_dir = pathlib.Path(data_dir).expanduser().resolve()
        if mode not in self._MODES:
            raise ValueError(f"Unsupported mode '{mode}'. Choose from {self._MODES}.")
        self.mode = mode
        self.batch_size = batch_size

    # ────────────────────────────────────────────────────────────────────
    # Public iterator
    # ────────────────────────────────────────────────────────────────────
    def iter_batches(self) -> Generator[Tuple[str, List[Dict]], None, None]:
        """
        Yield `(table_name, batch)` where *batch* is a list of row-dicts.
        """
        files = self._select_files()
        LOGGER.debug("iter_batches: %d files selected (mode=%s)", len(files), self.mode)
        for fpath in files:
            table = _table_for(fpath.name)
            if table is None:
                LOGGER.debug("Skipping unrecognised file %s", fpath)
                continue

            LOGGER.info("⏳  Extracting %s → %s", fpath.name, table)
            for batch in self._read_file(fpath, table):
                LOGGER.debug("  yielding batch of %d rows from %s", len(batch), fpath.name)
                yield table, batch

            LOGGER.info("✅  Finished %s", fpath.name)

    # ────────────────────────────────────────────────────────────────────
    # File scanning
    # ────────────────────────────────────────────────────────────────────
    def _select_files(self) -> List[pathlib.Path]:
        all_files = sorted(self.data_dir.rglob("*.tsv"))
        LOGGER.debug("_select_files: found %d TSV files under %s", len(all_files), self.data_dir)

        if self.mode == "all":
            return all_files
        if self.mode == "metadata":
            return [p for p in all_files if not p.name.endswith("_raw_counts.tsv")]
        # raw_counts
        return [p for p in all_files if p.name.endswith("_raw_counts.tsv")]

    # ────────────────────────────────────────────────────────────────────
    # File reader → batches
    # ────────────────────────────────────────────────────────────────────
    def _read_file(
        self, path: pathlib.Path, table: str
    ) -> Generator[List[Dict], None, None]:
        """
        Stream *path* and yield lists of size ≤ `self.batch_size`.
        A `sample_id` field is injected for raw-counts rows.
        """
        LOGGER.debug("_read_file: opening %s for table %s", path, table)
        with path.open(newline="") as fh:

            reader = csv.DictReader(fh, delimiter="\t")
            batch: List[Dict] = []
            sample_id = None

            if table == "RawCounts":
                # infer sample_id from filename prefix
                sample_id = path.stem.replace("_raw_counts", "")

            for row in reader:
                # count each row read
                if len(batch) == 0:
                    LOGGER.debug("  _read_file: starting new batch for %s", path.name)
                if table == "RawCounts":
                    row = {"sample_id": sample_id, **row}  # prepend FK

                batch.append(row)
                if len(batch) >= self.batch_size:
                    LOGGER.debug("  _read_file: batch full (%d rows), yielding", len(batch))
                    yield batch
                    batch = []
            if batch:
                LOGGER.debug("  _read_file: final batch (%d rows), yielding", len(batch))
                yield batch



# ───────────────────────────────────────────────────────────────────────────
# CLI entry-point
# ───────────────────────────────────────────────────────────────────────────
def _cli() -> None:
    ap = argparse.ArgumentParser(description="Extract TSV → dict batches")
    ap.add_argument(
        "--data-dir",
        required=True,
        help="Root folder produced by synthetic_data_generator.py "
        "or discovered by etl.discover",
    )
    ap.add_argument(
        "--mode",
        choices=Extractor._MODES,
        default="all",
        help="Select only metadata, only raw-counts, or all files.",
    )
    ap.add_argument(
        "--batch-size",
        type=int,
        default=1_000,
        help="Rows per yielded batch.",
    )
    args = ap.parse_args()

    extractor = Extractor(args.data_dir, mode=args.mode, batch_size=args.batch_size)
    total_rows = 0
    for table, batch in extractor.iter_batches():
        # For CLI demo we just count; in production you would pass the
        # batch to `Harmonizer.apply(table, batch)`
        total_rows += len(batch)
    LOGGER.info("Done. Total rows extracted: %s", total_rows)


if __name__ == "__main__":
    _cli()
