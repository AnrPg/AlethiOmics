"""
Gut–Brain Organoid DW – ETL root package
----------------------------------------

Convenience re-exports:

    >>> from etl import discover_files, Extractor, Harmonizer, MySQLLoader
"""

from importlib.metadata import PackageNotFoundError, version

# --------------------------------------------------------------------- #
#  Public version string                                                #
# --------------------------------------------------------------------- #
try:
    __version__: str = version(__name__)          # read from wheel / pyproject
except PackageNotFoundError:                      # fallback for source checkout
    __version__ = "0.0.0+"                        # pragma: no cover

# --------------------------------------------------------------------- #
#  Handy one-liners for notebooks / REPL                                #
# --------------------------------------------------------------------- #
from pathlib import Path
from typing import Generator, List  # re-export for typing hints

from .discover import discover_files           # 🔍  file scanner  :contentReference[oaicite:0]{index=0}
from .extract import Extractor                 # 🗄   TSV reader   :contentReference[oaicite:1]{index=1}
from .harmonize import Harmonizer              # 🔧  YAML mapper   :contentReference[oaicite:2]{index=2}
from .load import MySQLLoader                  # 🚚  batch loader  :contentReference[oaicite:3]{index=3}

__all__ = [
    "__version__",
    "discover_files",
    "Extractor",
    "Harmonizer",
    "MySQLLoader",
]
