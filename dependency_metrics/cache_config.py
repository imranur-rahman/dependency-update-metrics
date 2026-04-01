"""
Centralized memory and cache tuning configuration.

To add support for a new ecosystem, add an entry to RESOLVE_CACHE_MAX.
"""

from __future__ import annotations

import psutil

# Fraction of *total* RAM to use for disk-cache warm-up.
# Keeping this below 0.5 leaves headroom for metadata + worker caches during analysis.
WARM_DISK_FRACTION: float = 0.30


def warm_disk_max_bytes() -> int:
    """Return the byte cap for disk-cache warm-up (30% of total RAM)."""
    return int(psutil.virtual_memory().total * WARM_DISK_FRACTION)


# Per-ecosystem in-memory resolve-cache caps (max number of entries).
# None means unlimited.
#
# Entry costs differ per ecosystem:
#   pypi – one entry = a PackageFinder + PipSession object (~1-5 MB), keyed by before_date.
#           Few unique keys, high cost each → small cap (~1 GB total).
#   npm  – one entry = a resolved version string (~1 KB), keyed by (dep, constraint, date).
#           Many unique keys, low cost each → larger cap (~150 MB total).
#
# Add a new ecosystem by inserting a line below:
RESOLVE_CACHE_MAX: dict[str, int | None] = {
    "pypi": 500,  # PackageFinder objects; ~1-5 MB each → ~1 GB max
    "npm": 200_000,  # (dep, constraint, date) strings; ~1 KB each → ~150 MB max
    # "maven": 100_000,
}
