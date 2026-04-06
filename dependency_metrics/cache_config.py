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


# Namespaces to skip during warm_from_disk.
# "metadata" is excluded because the prefetch phase loads it into metadata_cache
# anyway; warming it into _disk_preload would pin all objects in memory for the
# entire run with no way to GC them, causing the bulk of the observed OOM kills.
WARM_SKIP_NAMESPACES: frozenset = frozenset({"metadata"})

# Max entries in metadata_cache (combined across all ecosystems).
# Each npm entry is ~1-2 MB; each PyPI entry ~100-500 KB.
# 500 entries ≈ 500 MB–1 GB — enough for 8 workers and their active dependencies.
# None = unlimited.
METADATA_CACHE_MAX: int | None = 500

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

# Per-version PyPI metadata blobs (100-500 KB each).
# 2 000 entries ≈ 200–500 MB.
PYPI_VERSION_METADATA_CACHE_MAX: int | None = 2_000

# Extracted dependency dicts per version (1-10 KB each).
# 5 000 entries ≈ 5–50 MB.
PYPI_VERSION_DEPS_CACHE_MAX: int | None = 5_000

# Preprocessed version prefix lists per package (50-200 KB each).
# 1 000 entries ≈ 50–200 MB.
VERSION_PREFIX_CACHE_MAX: int | None = 1_000

# npm version→timestamp maps per package (50-500 KB each).
# 1 000 entries ≈ 50–500 MB.
NPM_TIME_CACHE_MAX: int | None = 1_000
