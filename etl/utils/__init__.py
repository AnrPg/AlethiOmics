"""
Utility sub-package
-------------------
Lightweight, *pure* helpers live in :pymod:`etl.utils.preprocessing`.
Heavier scripts (e.g. the synthetic data generator) are here so they can
be launched as **modules**::

    python -m etl.utils.synthetic_data_generator --help
"""

# --------------------------- stateless helpers --------------------------- #
from .preprocessing import (          # :contentReference[oaicite:4]{index=4}
    strip_version,
    canonical_iri,
    normalize_study_accession,
    extract_sample_id,
    TRANSFORM_REGISTRY,
)

# --------------------------- synthetic runner ---------------------------- #
# When the project is installed without the generator script (e.g. on CI),
# we fail *gracefully* so imports elsewhere still succeed.
try:
    from .synthetic_data_generator import main as synthetic_data_generator
except ModuleNotFoundError:           # pragma: no cover
    synthetic_data_generator = None  # type: ignore[assignment]

__all__ = [
    # preprocessing helpers
    "strip_version",
    "canonical_iri",
    "normalize_study_accession",
    "extract_sample_id",
    "TRANSFORM_REGISTRY",
    # CLI entry-point for subprocess calls
    "synthetic_data_generator",
]
