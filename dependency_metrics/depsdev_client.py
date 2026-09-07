"""
HTTP client for the deps.dev REST API v3.

All package metadata and dependency requirements are fetched exclusively
from deps.dev — no calls are made to npm, PyPI, or crates.io registries.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Dict, Optional
from urllib.parse import quote

from .time_utils import ensure_utc, parse_timestamp

if TYPE_CHECKING:
    from .resolvers import ResolverCache

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.deps.dev/v3"

# Disk-cache namespace constants
_NS_PACKAGE = "depsdev_package"
_NS_REQUIREMENTS = "depsdev_req"


def _newest_published_at(data: Dict) -> Optional[datetime]:
    """Newest ``publishedAt`` among a cached GetPackage response's versions."""
    newest: Optional[datetime] = None
    for entry in data.get("versions", []) or []:
        ts = parse_timestamp(entry.get("publishedAt", ""))
        if ts is not None and (newest is None or ts > newest):
            newest = ts
    return newest


def _package_listing_may_be_stale(data: Dict, end_date: Optional[datetime]) -> bool:
    """True if a cached GetPackage response might be missing versions
    published on/before *end_date*.

    A package's version list is mutable (new releases keep appearing), unlike
    a single version's own requirements/metadata, which never change once
    published. A disk-cached listing has no way to know about a release that
    didn't exist when it was written, so if the newest release it knows about
    predates *end_date*, a newer one may since have appeared and the cache
    must be refreshed. If we can't determine a newest-known date, or no
    *end_date* was given, trust the cache rather than refetch blindly.
    """
    if end_date is None:
        return False
    newest = _newest_published_at(data)
    if newest is None:
        return False
    return newest < ensure_utc(end_date)


class DepsDevClient:
    """Thin wrapper around the deps.dev REST API v3.

    All responses are disk-cached via ``ResolverCache`` so that repeated runs
    (or parallel workers analysing the same dependency) never make duplicate
    network calls.

    Args:
        cache: Shared ``ResolverCache`` instance that provides HTTP session
               management and disk caching.
    """

    def __init__(self, cache: "ResolverCache") -> None:
        self._cache = cache

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_package(
        self, system: str, package_name: str, end_date: Optional[datetime] = None
    ) -> Dict:
        """Return the ``GetPackage`` response for *package_name*.

        Args:
            system: deps.dev system identifier, e.g. ``"NPM"``, ``"PYPI"``,
                    ``"CARGO"``.
            package_name: The package name as it appears in the registry.
            end_date: The analysis window's end date, if known. Used only to
                decide whether a cached response might be missing versions
                published since it was cached — see
                :func:`_package_listing_may_be_stale`. Omit to always trust
                the cache once present.

        Returns:
            Parsed JSON response dict with a ``"versions"`` list, each entry
            containing ``"versionKey"`` and ``"publishedAt"``.

        Raises:
            requests.HTTPError: On non-2xx responses.
        """
        cache_key = f"{system}:{package_name}"
        cached = self._cache.load_json(_NS_PACKAGE, cache_key)
        if cached is not None and not _package_listing_may_be_stale(cached, end_date):
            return cached

        url = (
            f"{_BASE_URL}/systems/{quote(system, safe='')}/packages/{quote(package_name, safe='')}"
        )
        logger.debug("deps.dev GetPackage: %s", url)
        resp = self._cache.get(url)
        resp.raise_for_status()
        data: Dict = resp.json()
        self._cache.save_json(_NS_PACKAGE, cache_key, data)
        return data

    def get_requirements(self, system: str, package_name: str, version: str) -> Dict:
        """Return the ``GetRequirements`` response for a specific version.

        Args:
            system: deps.dev system identifier.
            package_name: Package name.
            version: Exact version string (e.g. ``"4.18.2"``).

        Returns:
            Parsed JSON response dict with a ``"nodes"`` list.  Each node has
            a ``"versionKey"`` whose ``"version"`` field is the constraint
            string (e.g. ``"^4.0.0"``), and a ``"relation"`` field (e.g.
            ``"DIRECT"`` or ``"INDIRECT"``).

        Raises:
            requests.HTTPError: On non-2xx responses.
        """
        cache_key = f"{system}:{package_name}:{version}"
        cached = self._cache.load_json(_NS_REQUIREMENTS, cache_key)
        if cached is not None:
            return cached

        url = (
            f"{_BASE_URL}/systems/{quote(system, safe='')}"
            f"/packages/{quote(package_name, safe='')}"
            f"/versions/{quote(version, safe='')}:requirements"
        )
        logger.debug("deps.dev GetRequirements: %s", url)
        resp = self._cache.get(url)
        resp.raise_for_status()
        data = resp.json()
        self._cache.save_json(_NS_REQUIREMENTS, cache_key, data)
        return data
