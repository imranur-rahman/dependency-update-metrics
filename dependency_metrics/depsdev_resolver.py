"""
PackageResolver implementation backed by the deps.dev API v3.

All data (versions, dates, dependency constraints) is fetched exclusively
from deps.dev — no calls are made to npm, PyPI, or crates.io registries.

Constraint matching is done locally in Python:
  - NPM / CARGO : ``semantic_version.NpmSpec`` (handles ^, ~, *, ranges)
  - PYPI        : ``packaging.specifiers.SpecifierSet`` (PEP 440)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple

from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.version import InvalidVersion
from packaging import version as pkg_version

from .depsdev_client import DepsDevClient
from .models import PackageVersion
from .time_utils import parse_timestamp

logger = logging.getLogger(__name__)

# deps.dev system name → canonical lowercase ecosystem used internally
_SYSTEM_TO_ECOSYSTEM: Dict[str, str] = {
    "NPM": "npm",
    "PYPI": "pypi",
    "CARGO": "cargo",
}


def _parse_published_at(raw: str) -> Optional[datetime]:
    """Parse a deps.dev ``publishedAt`` RFC-3339 string to an aware datetime."""
    ts = parse_timestamp(raw)
    if ts is None:
        return None
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _is_prerelease_semver(version_str: str) -> bool:
    """Return True if *version_str* looks like a pre-release (contains a hyphen after digits)."""
    # Covers 1.0.0-alpha, 1.0.0-rc.1, etc.
    return "-" in version_str


def _is_prerelease_pypi(version_str: str) -> bool:
    """Return True if the PyPI version is a pre-release according to PEP 440."""
    try:
        return pkg_version.parse(version_str).is_prerelease
    except InvalidVersion:
        return False


def _match_npm_or_cargo(versions: List[str], constraint: str) -> Optional[str]:
    """Return the highest version in *versions* that satisfies *constraint*.

    Uses ``semantic_version.NpmSpec`` which covers Cargo and npm semantics
    (^, ~, *, x, comparison operators, hyphen ranges).

    Returns ``None`` if no version matches or the library is unavailable.
    """
    try:
        import semantic_version  # type: ignore[import]
    except ImportError:
        logger.error(
            "semantic_version is not installed. " "Run: pip install semantic_version>=2.10.0"
        )
        return None

    # Normalise bare version to caret requirement (Cargo convention: "1.2.3" == "^1.2.3")
    raw = constraint.strip()
    if raw in ("", "*"):
        spec_str = "*"
    else:
        spec_str = raw

    try:
        spec = semantic_version.NpmSpec(spec_str)
    except ValueError:
        logger.debug("Cannot parse npm/cargo constraint %r — skipping", constraint)
        return None

    best: Optional[semantic_version.Version] = None
    for ver_str in versions:
        try:
            v = semantic_version.Version.coerce(ver_str)
        except ValueError:
            continue
        if v in spec:
            if best is None or v > best:
                best = v
    return str(best) if best is not None else None


def _match_pypi(versions: List[str], constraint: str) -> Optional[str]:
    """Return the highest PEP 440 version in *versions* that satisfies *constraint*."""
    raw = constraint.strip()
    try:
        spec = SpecifierSet(raw) if raw and raw != "*" else SpecifierSet("")
    except InvalidSpecifier:
        logger.debug("Cannot parse PyPI constraint %r — skipping", constraint)
        return None

    best: Optional[pkg_version.Version] = None
    for ver_str in versions:
        try:
            v = pkg_version.parse(ver_str)
        except InvalidVersion:
            continue
        if v in spec:
            if best is None or v > best:
                best = v
    return str(best) if best is not None else None


def _match_constraint(system: str, versions: List[str], constraint: str) -> Optional[str]:
    """Dispatch to the correct constraint matcher for *system*."""
    if system == "PYPI":
        return _match_pypi(versions, constraint)
    # NPM and CARGO both use npm-style semver semantics
    return _match_npm_or_cargo(versions, constraint)


def _best_semver(system: str, versions: List[str]) -> Optional[str]:
    """Return the highest non-prerelease version, falling back to any version."""
    if system == "PYPI":
        candidates = []
        for v in versions:
            try:
                parsed = pkg_version.parse(v)
                candidates.append((parsed, v))
            except InvalidVersion:
                continue
        if not candidates:
            return None
        stable = [(p, s) for p, s in candidates if not p.is_prerelease]
        pool = stable if stable else candidates
        return max(pool, key=lambda t: t[0])[1]

    # NPM / CARGO — use semantic_version
    try:
        import semantic_version  # type: ignore[import]
    except ImportError:
        return versions[-1] if versions else None

    parsed = []
    for v in versions:
        try:
            parsed.append((semantic_version.Version.coerce(v), v))
        except ValueError:
            continue
    if not parsed:
        return versions[-1] if versions else None
    stable = [(sv, s) for sv, s in parsed if not sv.prerelease]
    pool = stable if stable else parsed
    return max(pool, key=lambda t: t[0])[1]


class DepsDevResolver:
    """``PackageResolver`` implementation that fetches all data from deps.dev.

    Args:
        system: deps.dev system identifier (``"NPM"``, ``"PYPI"``, ``"CARGO"``).
        package: Root package name being analysed.
        start_date: Analysis window start (used externally; stored for compatibility).
        end_date: Analysis window end; determines "version at date" lookups.
        client: Shared :class:`DepsDevClient` instance.
    """

    def __init__(
        self,
        system: str,
        package: str,
        start_date: datetime,
        end_date: datetime,
        client: DepsDevClient,
    ) -> None:
        self.system = system.upper()
        self.ecosystem = _SYSTEM_TO_ECOSYSTEM.get(self.system, self.system.lower())
        self.package = package
        self.start_date = start_date
        self.end_date = end_date
        self._client = client

        # Per-resolver in-memory caches (short-lived; one resolver per package)
        self._package_cache: Dict[str, Dict] = {}
        self._resolve_cache: Dict[Tuple[str, str, str], Optional[str]] = {}

    # ------------------------------------------------------------------
    # PackageResolver protocol
    # ------------------------------------------------------------------

    def fetch_package_metadata(self, package_name: str) -> Dict:
        """Fetch and cache the GetPackage response for *package_name*.

        Returns a dict whose ``"versions"`` list contains entries with
        ``"versionKey"`` (system, name, version) and ``"publishedAt"``.
        """
        if package_name in self._package_cache:
            return self._package_cache[package_name]
        try:
            data = self._client.get_package(self.system, package_name)
        except Exception as exc:
            logger.warning(
                "deps.dev GetPackage failed for %s/%s: %s", self.system, package_name, exc
            )
            data = {"versions": []}
        self._package_cache[package_name] = data
        return data

    def get_all_versions_with_dates(
        self, metadata: Dict, package_name: Optional[str] = None
    ) -> Iterable[PackageVersion]:
        """Yield :class:`~dependency_metrics.models.PackageVersion` for every entry in *metadata*.

        Pre-release versions are included so the caller can decide whether to
        filter them.
        """
        name = package_name or metadata.get("packageKey", {}).get("name", self.package)
        for entry in metadata.get("versions", []):
            vk = entry.get("versionKey", {})
            ver = vk.get("version", "")
            published_raw = entry.get("publishedAt", "")
            if not ver or not published_raw:
                continue
            released_at = _parse_published_at(published_raw)
            if released_at is None:
                continue
            yield PackageVersion(name=name, version=ver, released_at=released_at)

    def get_package_version_at_date(self, metadata: Dict) -> Tuple[str, Dict]:
        """Return the highest stable version with ``publishedAt <= self.end_date``.

        Returns a ``(version_str, stub_dict)`` pair where *stub_dict* contains
        ``"_package"`` and ``"_version"`` keys consumed by
        :meth:`extract_dependencies`.

        Raises:
            ValueError: If no qualifying version is found.
        """
        end = self.end_date
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)

        eligible: List[Tuple[datetime, str]] = []
        for pv in self.get_all_versions_with_dates(metadata):
            if pv.released_at <= end:
                eligible.append((pv.released_at, pv.version))

        if not eligible:
            raise ValueError(f"No version of {self.package} found on or before {end.date()}")

        version_strs = [v for _, v in eligible]
        chosen = _best_semver(self.system, version_strs)
        if chosen is None:
            chosen = eligible[-1][1]

        stub = {"_package": self.package, "_version": chosen}
        return chosen, stub

    def extract_dependencies(self, version_data: Dict) -> Dict[str, str]:
        """Extract direct dependency constraints from *version_data*.

        *version_data* must be a stub dict produced by
        :meth:`get_package_version_at_date` or :meth:`get_package_version_at_date`,
        containing ``"_package"`` and ``"_version"`` keys.

        Returns:
            ``{dep_name: constraint_str}`` mapping.
        """
        package = version_data.get("_package", self.package)
        version = version_data.get("_version", "")
        if not version:
            return {}
        return self.get_version_dependencies(package, version)

    def get_version_dependencies(self, package: str, version: str) -> Dict[str, str]:
        """Call GetRequirements and return ``{dep_name: constraint}`` for DIRECT deps only."""
        try:
            data = self._client.get_requirements(self.system, package, version)
        except Exception as exc:
            logger.warning(
                "deps.dev GetRequirements failed for %s/%s@%s: %s",
                self.system,
                package,
                version,
                exc,
            )
            return {}

        deps: Dict[str, str] = {}
        for node in data.get("nodes", []):
            relation = node.get("relation", "")
            if relation != "DIRECT":
                continue
            vk = node.get("versionKey", {})
            dep_name = vk.get("name", "")
            constraint = vk.get("version", "")
            if dep_name:
                deps[dep_name] = constraint
        return deps

    def resolve_dependency_version(
        self, dependency: str, constraint: str, before_date: datetime
    ) -> Optional[str]:
        """Resolve *constraint* for *dependency* to the highest matching version
        released on or before *before_date*.

        Results are memoised per ``(dependency, constraint, date)`` key.
        """
        if before_date.tzinfo is None:
            before_date = before_date.replace(tzinfo=timezone.utc)
        date_key = before_date.date().isoformat()
        cache_key = (dependency, constraint, date_key)
        if cache_key in self._resolve_cache:
            return self._resolve_cache[cache_key]

        metadata = self.fetch_package_metadata(dependency)
        eligible: List[str] = []
        for pv in self.get_all_versions_with_dates(metadata, package_name=dependency):
            if pv.released_at <= before_date:
                eligible.append(pv.version)

        result = _match_constraint(self.system, eligible, constraint) if eligible else None
        self._resolve_cache[cache_key] = result
        return result

    def get_highest_semver_version_at_date(
        self,
        package_name: str,
        at_date: datetime,
        metadata: Optional[Dict] = None,
    ) -> Optional[str]:
        """Return the highest stable version of *package_name* released on or before *at_date*."""
        if at_date.tzinfo is None:
            at_date = at_date.replace(tzinfo=timezone.utc)

        if metadata is None:
            metadata = self.fetch_package_metadata(package_name)

        eligible: List[str] = []
        for pv in self.get_all_versions_with_dates(metadata, package_name=package_name):
            if pv.released_at <= at_date:
                eligible.append(pv.version)

        return _best_semver(self.system, eligible)
