"""
HTTP client for the deps.dev REST API v3.

All package metadata and dependency requirements are fetched exclusively
from deps.dev — no calls are made to npm, PyPI, or crates.io registries.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Dict, Optional
from urllib.parse import quote

import requests

if TYPE_CHECKING:
    from .resolvers import ResolverCache

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.deps.dev/v3"

# Disk-cache namespace constants
_NS_PACKAGE = "depsdev_package"
_NS_REQUIREMENTS = "depsdev_req"


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

    def get_package(self, system: str, package_name: str) -> Dict:
        """Return the ``GetPackage`` response for *package_name*.

        Args:
            system: deps.dev system identifier, e.g. ``"NPM"``, ``"PYPI"``,
                    ``"CARGO"``.
            package_name: The package name as it appears in the registry.

        Returns:
            Parsed JSON response dict with a ``"versions"`` list, each entry
            containing ``"versionKey"`` and ``"publishedAt"``.

        Raises:
            requests.HTTPError: On non-2xx responses.
        """
        cache_key = f"{system}:{package_name}"
        cached = self._cache.load_json(_NS_PACKAGE, cache_key)
        if cached is not None:
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
