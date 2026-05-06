"""
Ecosystem-specific package resolvers.
"""

from __future__ import annotations

import bisect
import json
import logging
import sqlite3
import subprocess
import threading
from collections import OrderedDict
from urllib.parse import quote
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import psutil

from .cache_config import (
    METADATA_CACHE_MAX,
    NPM_TIME_CACHE_MAX,
    PYPI_VERSION_DEPS_CACHE_MAX,
    PYPI_VERSION_METADATA_CACHE_MAX,
    RESOLVE_CACHE_MAX,
    VERSION_PREFIX_CACHE_MAX,
    WARM_DISK_FRACTION,
    WARM_SKIP_NAMESPACES,
    warm_disk_max_bytes,
)

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from packaging import version as pkg_version
from packaging.specifiers import SpecifierSet
from packaging.requirements import Requirement

from .interfaces import PackageResolver
from .models import PackageVersion
from .pypi_resolver import resolve_pypi_version
from .time_utils import parse_timestamp

logger = logging.getLogger(__name__)


def resolve_pypi_version_locally(
    dep_metadata: Dict,
    constraint: str,
    before_date: datetime,
) -> Optional[str]:
    """Resolve a PyPI dependency version locally using cached package metadata.

    Avoids any network call by filtering the releases dict that is already
    present in the JSON metadata fetched from PyPI.  This is a direct
    replacement for the vendored-pip path in the hot loop.

    Args:
        dep_metadata: Full JSON metadata dict for the dependency (from PyPI API).
        constraint: PEP 440 version specifier string, e.g. ">=2.0,<3" or "*".
        before_date: Only consider versions uploaded on or before this datetime.

    Returns:
        Highest matching version string, or None if none exists.
    """
    releases = dep_metadata.get("releases", {})
    specifier = SpecifierSet(constraint) if constraint and constraint != "*" else SpecifierSet("")

    valid: list = []
    for ver_str, files in releases.items():
        if not files:
            continue
        upload_time = files[0].get("upload_time")
        if not upload_time:
            continue
        try:
            pub_date = parse_timestamp(upload_time)
            if pub_date is None:
                continue
            # Normalize before_date to UTC for comparison
            if before_date.tzinfo is None:
                from datetime import timezone as _tz

                cmp_date = before_date.replace(tzinfo=_tz.utc)
            else:
                cmp_date = before_date
            if pub_date > cmp_date:
                continue
            parsed = pkg_version.parse(ver_str)
            if parsed in specifier:
                valid.append(parsed)
        except Exception:
            continue

    if not valid:
        return None
    return str(max(valid))


def npm_semver_key(
    version: str,
) -> Optional[Tuple[int, int, int, int, Tuple[Tuple[int, object], ...]]]:
    """Return a sortable key for npm semver strings or None if invalid."""
    if version is None:
        return None
    cleaned = str(version).strip()
    while cleaned and cleaned[0] in ("v", "="):
        cleaned = cleaned[1:]
    if not cleaned:
        return None

    # Drop build metadata
    cleaned = cleaned.split("+", 1)[0]

    prerelease_key: Tuple[Tuple[int, object], ...] = tuple()
    is_release = 1
    if "-" in cleaned:
        base, prerelease = cleaned.split("-", 1)
        is_release = 0
        identifiers = prerelease.split(".") if prerelease else []
        pre_parts: List[Any] = []
        for ident in identifiers:
            if ident.isdigit():
                pre_parts.append((0, int(ident)))
            else:
                pre_parts.append((1, ident))
        prerelease_key = tuple(pre_parts)
    else:
        base = cleaned

    parts = base.split(".")
    if len(parts) > 3:
        return None
    while len(parts) < 3:
        parts.append("0")

    try:
        major = int(parts[0])
        minor = int(parts[1])
        patch = int(parts[2])
    except ValueError:
        return None

    return (major, minor, patch, is_release, prerelease_key)


@dataclass
class ResolverCache:
    """Shared in-memory caches for resolver operations."""

    metadata_cache: OrderedDict = field(default_factory=OrderedDict)
    pypi_version_metadata_cache: OrderedDict = field(default_factory=OrderedDict)
    pypi_version_deps_cache: OrderedDict = field(default_factory=OrderedDict)
    npm_time_cache: OrderedDict = field(default_factory=OrderedDict)
    npm_resolve_cache: OrderedDict = field(default_factory=OrderedDict)
    invalid_version_strings: Dict[Tuple[str, str], Set[str]] = field(default_factory=dict)
    missing_packages: Set[Tuple[str, str]] = field(default_factory=set)
    version_prefix_cache: OrderedDict = field(default_factory=OrderedDict)
    cache_dir: Optional[Path] = None
    request_timeout: Tuple[float, float] = (5.0, 30.0)
    _thread_local: threading.local = field(default_factory=threading.local, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _metadata_locks: Dict[Any, threading.Lock] = field(default_factory=dict, init=False, repr=False)
    _disk_preload: Dict[Tuple[str, str], Any] = field(default_factory=dict, init=False, repr=False)

    def get_key_lock(self, key: Any) -> threading.Lock:
        """Return a per-cache-key Lock, creating one on first use.

        Used to serialize concurrent fetches for the same package so that
        at most one thread performs the network call (thundering-herd fix).
        """
        with self._lock:
            if key not in self._metadata_locks:
                self._metadata_locks[key] = threading.Lock()
            return self._metadata_locks[key]

    def _capped_set(
        self, cache: OrderedDict, key: Any, value: Any, max_size: Optional[int]
    ) -> None:
        """Write key→value into an OrderedDict cache, evicting oldest 10% when cap is reached."""
        with self._lock:
            cache[key] = value
            if max_size is not None and len(cache) > max_size:
                evict_count = max(1, max_size // 10)
                for _ in range(evict_count):
                    try:
                        cache.popitem(last=False)
                    except KeyError:
                        break

    def npm_resolve_set(self, key: Tuple[str, str, str], value: Optional[str]) -> None:
        """Write an entry to npm_resolve_cache with FIFO eviction when the cap is reached."""
        self._capped_set(self.npm_resolve_cache, key, value, RESOLVE_CACHE_MAX.get("npm"))

    def metadata_set(self, key: Tuple[str, str], value: Dict) -> None:
        """Write an entry to metadata_cache with FIFO eviction when the cap is reached."""
        self._capped_set(self.metadata_cache, key, value, METADATA_CACHE_MAX)

    def pypi_version_metadata_set(self, key: str, value: Dict) -> None:
        """Write an entry to pypi_version_metadata_cache with FIFO eviction."""
        self._capped_set(
            self.pypi_version_metadata_cache, key, value, PYPI_VERSION_METADATA_CACHE_MAX
        )

    def pypi_version_deps_set(self, key: str, value: Dict) -> None:
        """Write an entry to pypi_version_deps_cache with FIFO eviction."""
        self._capped_set(self.pypi_version_deps_cache, key, value, PYPI_VERSION_DEPS_CACHE_MAX)

    def npm_time_set(self, key: str, value: Dict) -> None:
        """Write an entry to npm_time_cache with FIFO eviction."""
        self._capped_set(self.npm_time_cache, key, value, NPM_TIME_CACHE_MAX)

    def version_prefix_set(self, key: Tuple, value: Any) -> None:
        """Write an entry to version_prefix_cache with FIFO eviction."""
        self._capped_set(self.version_prefix_cache, key, value, VERSION_PREFIX_CACHE_MAX)

    def _get_sqlite_conn(self) -> sqlite3.Connection:
        """Get or create a thread-local SQLite connection to the cache DB.

        Uses WAL mode so multiple reader threads can query concurrently while
        a writer commits without blocking reads.
        """
        conn = getattr(self._thread_local, "sqlite_conn", None)
        if conn is not None:
            return conn
        assert self.cache_dir is not None
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.cache_dir / "cache.db"
        conn = sqlite3.connect(str(db_path), check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS cache "
            "(namespace TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, "
            "PRIMARY KEY (namespace, key))"
        )
        conn.commit()
        self._thread_local.sqlite_conn = conn
        return conn

    def warm_from_disk(self) -> None:
        """Pre-load hot cache namespaces from SQLite into _disk_preload.

        Reads up to WARM_DISK_FRACTION of total RAM worth of entries.
        The "metadata" namespace is skipped — it is populated by the prefetch
        phase into metadata_cache and warming it into _disk_preload would pin
        all metadata objects in memory with no way to evict them.
        """
        if self.cache_dir is None:
            return
        db_path = self.cache_dir / "cache.db"
        if not db_path.exists():
            return
        max_bytes = warm_disk_max_bytes()
        proc = psutil.Process()
        initial_rss = proc.memory_info().rss
        target_namespaces = tuple(
            ns
            for ns in (
                "depsdev_package",
                "depsdev_req",
                "npm_time",
                "resolve_npm",
                "invalid_versions",
            )
            if ns not in WARM_SKIP_NAMESPACES
        )
        total = 0
        truncated = False
        try:
            conn = self._get_sqlite_conn()
            placeholders = ",".join("?" * len(target_namespaces))
            rows = conn.execute(
                f"SELECT namespace, key, value FROM cache " f"WHERE namespace IN ({placeholders})",
                target_namespaces,
            ).fetchall()
            for ns, k, raw in rows:
                self._disk_preload[(ns, k)] = json.loads(raw)
                total += 1
                if total % 500 == 0:
                    rss_delta = proc.memory_info().rss - initial_rss
                    if rss_delta > max_bytes:
                        logger.warning(
                            "Cache warm-up: reached %.0f MB RSS limit (%.0f%% of total RAM). "
                            "Remaining entries will be read from SQLite on demand.",
                            max_bytes / 1024 / 1024,
                            WARM_DISK_FRACTION * 100,
                        )
                        truncated = True
                        break
        except Exception as exc:
            logger.warning("Cache warm-up failed: %s", exc)
            return
        rss_delta_mb = (proc.memory_info().rss - initial_rss) / 1024 / 1024
        logger.info(
            "Cache warm-up: loaded %d entries (+%.0f MB RSS)%s",
            total,
            rss_delta_mb,
            " [truncated at RAM cap]" if truncated else "",
        )

    def get_session(self) -> requests.Session:
        """Get or create a thread-local HTTP session with bounded pooling and retries."""
        session = getattr(self._thread_local, "session", None)
        if session is not None:
            return session

        session = requests.Session()
        retry = Retry(
            total=5,
            connect=5,
            read=5,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET"}),
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=10,
            pool_maxsize=10,
            pool_block=True,
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        self._thread_local.session = session
        return session

    def get(self, url: str, **kwargs: Any) -> requests.Response:
        """Issue a GET request through the thread-local session."""
        if "timeout" not in kwargs:
            kwargs["timeout"] = self.request_timeout
        return self.get_session().get(url, **kwargs)

    def load_json(self, namespace: str, key: str) -> Optional[Dict[str, Any]]:
        if self.cache_dir is None:
            return None
        # Check in-memory preload before hitting SQLite
        preloaded = self._disk_preload.get((namespace, key))
        if preloaded is not None:
            return preloaded
        try:
            conn = self._get_sqlite_conn()
            row = conn.execute(
                "SELECT value FROM cache WHERE namespace=? AND key=?", (namespace, key)
            ).fetchone()
            return json.loads(row[0]) if row else None
        except Exception:
            return None

    def save_json(self, namespace: str, key: str, data: Dict[str, Any]) -> None:
        if self.cache_dir is None:
            return
        try:
            conn = self._get_sqlite_conn()
            conn.execute(
                "INSERT OR REPLACE INTO cache(namespace, key, value) VALUES (?,?,?)",
                (namespace, key, json.dumps(data)),
            )
            conn.commit()
        except Exception:
            return

    def load_invalid_versions(self, ecosystem: str, package_name: str) -> Set[str]:
        """Load known-invalid version strings from disk into memory, return the set."""
        cache_key = (ecosystem, package_name)
        if cache_key in self.invalid_version_strings:
            return self.invalid_version_strings[cache_key]
        disk_key = f"{ecosystem}:{package_name}"
        data = self.load_json("invalid_versions", disk_key)
        result: Set[str] = set(data.get("invalid", [])) if data else set()
        self.invalid_version_strings[cache_key] = result
        return result

    def record_invalid_version(self, ecosystem: str, package_name: str, version: str) -> None:
        """Mark a version string as invalid in memory and persist to disk."""
        cache_key = (ecosystem, package_name)
        s = self.invalid_version_strings.setdefault(cache_key, set())
        if version in s:
            return  # already recorded
        s.add(version)
        disk_key = f"{ecosystem}:{package_name}"
        self.save_json("invalid_versions", disk_key, {"invalid": sorted(s)})


class NpmResolver(PackageResolver):
    """Resolver for npm packages."""

    ecosystem = "npm"

    def __init__(
        self,
        package: str,
        start_date: datetime,
        end_date: datetime,
        registry_urls: Dict[str, str],
        cache: ResolverCache,
    ) -> None:
        self.package = package
        self.start_date = start_date
        self.end_date = end_date
        self.registry_urls = registry_urls
        self.cache = cache

    def fetch_package_metadata(self, package_name: str) -> Dict:
        cache_key = (self.ecosystem, package_name)
        # Fast path: GIL-safe dict lookup (no lock needed)
        if cache_key in self.cache.metadata_cache:
            logger.debug("Cache hit: metadata %s:%s", self.ecosystem, package_name)
            return self.cache.metadata_cache[cache_key]
        if cache_key in self.cache.missing_packages:
            raise requests.HTTPError(f"Package not found (cached): {package_name}")

        # Serialize concurrent fetches for the same key (thundering-herd fix)
        with self.cache.get_key_lock(cache_key):
            # Double-check after acquiring lock
            if cache_key in self.cache.metadata_cache:
                logger.debug("Cache hit (post-lock): metadata %s:%s", self.ecosystem, package_name)
                return self.cache.metadata_cache[cache_key]
            if cache_key in self.cache.missing_packages:
                raise requests.HTTPError(f"Package not found (cached): {package_name}")

            disk_key = f"{self.ecosystem}:{package_name}"
            cached = self.cache.load_json("metadata", disk_key)
            if cached is not None:
                self.cache.metadata_set(cache_key, cached)
                return cached

            encoded_package = quote(package_name, safe="")
            url = f"{self.registry_urls['npm']}/{encoded_package}"
            logger.info("Fetching metadata for %s", package_name)
            try:
                with self.cache.get(url) as response:
                    response.raise_for_status()
                    data = response.json()
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 404:
                    self.cache.missing_packages.add(cache_key)
                    logger.warning(
                        "Package not found on registry, skipping further requests: %s",
                        package_name,
                    )
                raise
            self.cache.metadata_set(cache_key, data)
            self.cache.save_json("metadata", disk_key, data)
            return data

    def get_package_version_at_date(self, metadata: Dict) -> Tuple[str, Dict]:
        try:
            time_data = self._get_npm_time_data(self.package)
            if time_data:
                valid_versions = []
                for ver, timestamp in time_data.items():
                    try:
                        pub_date = parse_timestamp(timestamp)
                        if pub_date is None:
                            continue
                        if pub_date <= self.end_date:
                            valid_versions.append((ver, pub_date))
                    except (ValueError, AttributeError):
                        continue

                if valid_versions:
                    valid_versions.sort(key=lambda x: x[1], reverse=True)
                    latest_version = valid_versions[0][0]
                    versions = metadata.get("versions", {})
                    version_data = versions.get(latest_version, {})
                    return latest_version, version_data
        except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError) as e:
            logger.warning("npm view time failed, falling back to metadata parsing: %s", e)

        return self._parse_versions_from_metadata(metadata)

    def get_all_versions_with_dates(
        self, metadata: Dict, package_name: Optional[str] = None
    ) -> Iterable[PackageVersion]:
        if package_name:
            try:
                time_data = self._get_npm_time_data(package_name)
                if time_data:
                    version_dates = []
                    for ver, timestamp in time_data.items():
                        try:
                            pub_date = parse_timestamp(timestamp)
                            if pub_date is None:
                                continue
                            if self.start_date <= pub_date <= self.end_date:
                                version_dates.append(
                                    PackageVersion(
                                        name=package_name,
                                        version=ver,
                                        released_at=pub_date,
                                    )
                                )
                        except (ValueError, AttributeError):
                            continue
                    return sorted(version_dates, key=lambda x: x.released_at)
            except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError) as e:
                logger.warning(
                    "npm view time failed for %s, falling back to metadata: %s", package_name, e
                )

        versions = metadata.get("versions", {})
        version_dates = []
        for ver, ver_data in versions.items():
            published = ver_data.get("dist", {}).get("published")
            if not published:
                continue
            try:
                pub_date = parse_timestamp(published)
                if pub_date is None:
                    continue
                if self.start_date <= pub_date <= self.end_date:
                    version_dates.append(
                        PackageVersion(
                            name=package_name or self.package,
                            version=ver,
                            released_at=pub_date,
                        )
                    )
            except (ValueError, AttributeError):
                continue
        return sorted(version_dates, key=lambda x: x.released_at)

    def resolve_dependency_version(
        self, dependency: str, constraint: str, before_date: datetime
    ) -> Optional[str]:
        cache_key = (dependency, constraint, before_date.isoformat())
        if cache_key in self.cache.npm_resolve_cache:
            logger.debug("Cache hit: npm resolve %s %s %s", dependency, constraint, before_date)
            return self.cache.npm_resolve_cache[cache_key]

        # Check disk cache (persists across restarts / --resume runs)
        disk_key = f"npm:{dependency}|{constraint}|{before_date.isoformat()}"
        cached = self.cache.load_json("resolve_npm", disk_key)
        if cached is not None:
            result = cached.get("version")
            self.cache.npm_resolve_set(cache_key, result)
            return result

        try:
            cmd = [
                "npm",
                "view",
                f"{dependency}@{constraint}",
                "version",
                "--json",
                "--before",
                before_date.isoformat(),
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode == 0:
                output = result.stdout.strip()
                if output:
                    versions = json.loads(output)
                    if isinstance(versions, list):
                        resolved = versions[-1]
                    else:
                        resolved = versions
                    self.cache.npm_resolve_set(cache_key, resolved)
                    self.cache.save_json("resolve_npm", disk_key, {"version": resolved})
                    return resolved
        except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError) as e:
            logger.warning("Error resolving npm version for %s: %s", dependency, e)

        self.cache.npm_resolve_set(cache_key, None)
        self.cache.save_json("resolve_npm", disk_key, {"version": None})
        return None

    def _get_preprocessed_versions(self, package_name: str) -> Tuple[List, List, List]:
        cache_key = (self.ecosystem, package_name)
        if cache_key in self.cache.version_prefix_cache:
            return self.cache.version_prefix_cache[cache_key]  # type: ignore[return-value]

        known_invalid = self.cache.load_invalid_versions(self.ecosystem, package_name)
        entries: List[Tuple] = []  # (pub_date, semver_key_or_None, ver_str)

        time_data = None
        try:
            time_data = self._get_npm_time_data(package_name)
        except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError) as e:
            logger.warning(
                "npm view time failed for %s, falling back to metadata: %s", package_name, e
            )

        if time_data:
            for ver, timestamp in time_data.items():
                try:
                    pub_date = parse_timestamp(timestamp)
                    if pub_date is None:
                        continue
                except (ValueError, AttributeError):
                    continue
                if ver in known_invalid:
                    key = None
                else:
                    key = npm_semver_key(ver)
                    if key is None:
                        logger.debug("Skipping invalid npm semver %s for %s", ver, package_name)
                        self.cache.record_invalid_version(self.ecosystem, package_name, ver)
                entries.append((pub_date, key, ver))
        else:
            metadata = self.fetch_package_metadata(package_name)
            for ver, ver_data in metadata.get("versions", {}).items():
                published = ver_data.get("dist", {}).get("published")
                if not published:
                    continue
                try:
                    pub_date = parse_timestamp(published)
                    if pub_date is None:
                        continue
                except (ValueError, AttributeError):
                    continue
                if ver in known_invalid:
                    key = None
                else:
                    key = npm_semver_key(ver)
                    if key is None:
                        logger.debug("Skipping invalid npm semver %s for %s", ver, package_name)
                        self.cache.record_invalid_version(self.ecosystem, package_name, ver)
                entries.append((pub_date, key, ver))

        entries.sort(key=lambda e: e[0])

        sorted_dates: List[datetime] = []
        prefix_best_semver: List[Optional[str]] = []
        prefix_best_alpha: List[Optional[str]] = []
        best_semver: Optional[Tuple] = None  # (key_tuple, ver_str)
        best_alpha: Optional[str] = None

        for pub_date, key, ver in entries:
            sorted_dates.append(pub_date)
            if key is not None and (best_semver is None or key > best_semver[0]):
                best_semver = (key, ver)
            if best_alpha is None or ver > best_alpha:
                best_alpha = ver
            prefix_best_semver.append(best_semver[1] if best_semver else None)
            prefix_best_alpha.append(best_alpha)

        result = (sorted_dates, prefix_best_semver, prefix_best_alpha)
        self.cache.version_prefix_set(cache_key, result)
        return result

    def get_highest_semver_version_at_date(
        self, package_name: str, at_date: datetime, metadata: Optional[Dict] = None
    ) -> Optional[str]:
        if (self.ecosystem, package_name) in self.cache.missing_packages:
            return None
        try:
            sorted_dates, prefix_best_semver, prefix_best_alpha = self._get_preprocessed_versions(
                package_name
            )
            if not sorted_dates:
                return None
            idx = bisect.bisect_right(sorted_dates, at_date) - 1
            if idx < 0:
                return None
            if prefix_best_semver[idx] is not None:
                return prefix_best_semver[idx]
            return prefix_best_alpha[idx]
        except Exception as e:
            message = f"Error getting highest semver version for {package_name}: {e}"
            logger.warning(message)
            raise RuntimeError(message) from e

    def extract_dependencies(self, version_data: Dict) -> Dict[str, str]:
        return version_data.get("dependencies", {})

    def get_version_dependencies(self, package: str, version: str) -> Dict[str, str]:
        metadata = self.fetch_package_metadata(package)
        ver_data = metadata.get("versions", {}).get(version, {})
        return ver_data.get("dependencies", {})

    def _get_npm_time_data(self, package_name: str) -> Optional[Dict[str, str]]:
        if package_name in self.cache.npm_time_cache:
            logger.debug("Cache hit: npm time %s", package_name)
            return self.cache.npm_time_cache[package_name]

        disk_key = f"npm:{package_name}"
        cached = self.cache.load_json("npm_time", disk_key)
        if cached is not None:
            self.cache.npm_time_set(package_name, cached)
            return cached

        cmd = ["npm", "view", package_name, "time", "--json"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            return None

        time_data = json.loads(result.stdout)
        time_data.pop("modified", None)
        time_data.pop("created", None)
        self.cache.npm_time_set(package_name, time_data)
        self.cache.save_json("npm_time", disk_key, time_data)
        return time_data

    def _parse_versions_from_metadata(self, metadata: Dict) -> Tuple[str, Dict]:
        versions = metadata.get("versions", {})
        valid_versions = []

        for ver, ver_data in versions.items():
            published = ver_data.get("dist", {}).get("published")
            if not published:
                continue

            try:
                pub_date = parse_timestamp(published)
                if pub_date is None:
                    continue
                if pub_date <= self.end_date:
                    valid_versions.append((ver, pub_date, ver_data))
            except (ValueError, AttributeError):
                continue

        if not valid_versions:
            raise ValueError(f"No versions found before {self.end_date}")

        valid_versions.sort(key=lambda x: x[1], reverse=True)
        return valid_versions[0][0], valid_versions[0][2]


class PyPIResolver(PackageResolver):
    """Resolver for PyPI packages."""

    ecosystem = "pypi"

    def __init__(
        self,
        package: str,
        start_date: datetime,
        end_date: datetime,
        registry_urls: Dict[str, str],
        cache: ResolverCache,
    ) -> None:
        self.package = package
        self.start_date = start_date
        self.end_date = end_date
        self.registry_urls = registry_urls
        self.cache = cache

    def fetch_package_metadata(self, package_name: str) -> Dict:
        cache_key = (self.ecosystem, package_name)
        # Fast path: GIL-safe dict lookup (no lock needed)
        if cache_key in self.cache.metadata_cache:
            logger.debug("Cache hit: metadata %s:%s", self.ecosystem, package_name)
            return self.cache.metadata_cache[cache_key]
        if cache_key in self.cache.missing_packages:
            raise requests.HTTPError(f"Package not found (cached): {package_name}")

        # Serialize concurrent fetches for the same key (thundering-herd fix)
        with self.cache.get_key_lock(cache_key):
            # Double-check after acquiring lock
            if cache_key in self.cache.metadata_cache:
                logger.debug("Cache hit (post-lock): metadata %s:%s", self.ecosystem, package_name)
                return self.cache.metadata_cache[cache_key]
            if cache_key in self.cache.missing_packages:
                raise requests.HTTPError(f"Package not found (cached): {package_name}")

            disk_key = f"{self.ecosystem}:{package_name}"
            cached = self.cache.load_json("metadata", disk_key)
            if cached is not None:
                self.cache.metadata_set(cache_key, cached)
                return cached

            encoded_package = quote(package_name, safe="")
            url = f"{self.registry_urls['pypi']}/{encoded_package}/json"
            logger.info("Fetching metadata for %s", package_name)
            try:
                with self.cache.get(url) as response:
                    response.raise_for_status()
                    data = response.json()
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 404:
                    self.cache.missing_packages.add(cache_key)
                    logger.warning(
                        "Package not found on registry, skipping further requests: %s",
                        package_name,
                    )
                raise
            self.cache.metadata_set(cache_key, data)
            self.cache.save_json("metadata", disk_key, data)
            return data

    def get_package_version_at_date(self, metadata: Dict) -> Tuple[str, Dict]:
        releases = metadata.get("releases", {})
        valid_versions = []

        for ver, release_files in releases.items():
            if not release_files:
                continue

            upload_time = release_files[0].get("upload_time")
            if not upload_time:
                continue

            try:
                pub_date = parse_timestamp(upload_time)
                if pub_date is None:
                    continue
                if pub_date <= self.end_date:
                    valid_versions.append((ver, pub_date))
            except (ValueError, AttributeError):
                continue

        if not valid_versions:
            raise ValueError(f"No versions found before {self.end_date}")

        valid_versions.sort(key=lambda x: x[1], reverse=True)
        latest_version = valid_versions[0][0]

        try:
            version_metadata = self._get_pypi_version_metadata(self.package, latest_version)
            version_data = {
                "upload_time": valid_versions[0][1].isoformat(),
                "requires_dist": version_metadata.get("info", {}).get("requires_dist", []),
            }
            return latest_version, version_data
        except Exception as e:
            logger.warning("Failed to fetch version-specific data: %s", e)
            return latest_version, {
                "upload_time": valid_versions[0][1].isoformat(),
                "requires_dist": [],
            }

    def get_all_versions_with_dates(
        self, metadata: Dict, package_name: Optional[str] = None
    ) -> Iterable[PackageVersion]:
        releases = metadata.get("releases", {})
        version_dates = []

        for ver, ver_data in releases.items():
            if isinstance(ver_data, list) and ver_data:
                published = ver_data[0].get("upload_time")
            else:
                published = ver_data.get("upload_time") if isinstance(ver_data, dict) else None

            if not published:
                continue

            try:
                pub_date = parse_timestamp(published)
                if pub_date is None:
                    continue
                if self.start_date <= pub_date <= self.end_date:
                    version_dates.append(
                        PackageVersion(
                            name=package_name or self.package,
                            version=ver,
                            released_at=pub_date,
                        )
                    )
            except (ValueError, AttributeError):
                continue

        return sorted(version_dates, key=lambda x: x.released_at)

    def resolve_dependency_version(
        self, dependency: str, constraint: str, before_date: datetime
    ) -> Optional[str]:
        try:
            return resolve_pypi_version(dependency, constraint, before_date)
        except Exception as e:
            logger.warning("Error resolving pypi version for %s: %s", dependency, e)
            return None

    def _get_preprocessed_versions(self, package_name: str) -> Tuple[List, List, List]:
        cache_key = (self.ecosystem, package_name)
        if cache_key in self.cache.version_prefix_cache:
            return self.cache.version_prefix_cache[cache_key]  # type: ignore[return-value]

        known_invalid = self.cache.load_invalid_versions(self.ecosystem, package_name)
        metadata = self.fetch_package_metadata(package_name)
        entries: List[Tuple] = []  # (pub_date, parsed_version_or_None, ver_str)

        releases = metadata.get("releases", {})
        for ver, release_files in releases.items():
            if not release_files:
                continue
            upload_time = release_files[0].get("upload_time")
            if not upload_time:
                continue
            try:
                pub_date = parse_timestamp(upload_time)
                if pub_date is None:
                    continue
            except (ValueError, AttributeError):
                continue
            if ver in known_invalid:
                parsed = None
            else:
                try:
                    parsed = pkg_version.parse(ver)
                except Exception:
                    logger.debug("Skipping non-PEP440 version %s for %s", ver, package_name)
                    self.cache.record_invalid_version(self.ecosystem, package_name, ver)
                    parsed = None
            entries.append((pub_date, parsed, ver))

        entries.sort(key=lambda e: e[0])

        sorted_dates: List[datetime] = []
        sorted_parsed: List[Optional[Any]] = []
        prefix_best_semver: List[Optional[str]] = []
        best_semver: Optional[Tuple] = None  # (parsed_version, ver_str)

        for pub_date, parsed, ver in entries:
            sorted_dates.append(pub_date)
            sorted_parsed.append(parsed)
            if parsed is not None and (best_semver is None or parsed > best_semver[0]):
                best_semver = (parsed, ver)
            prefix_best_semver.append(best_semver[1] if best_semver else None)

        result = (sorted_dates, prefix_best_semver, sorted_parsed)
        self.cache.version_prefix_set(cache_key, result)
        return result

    def resolve_constraint_at_date(
        self, package_name: str, constraint: str, at_date: datetime
    ) -> Optional[str]:
        """Return the highest version of package_name satisfying constraint at at_date.

        Uses pre-parsed version objects from _get_preprocessed_versions to avoid
        repeated pkg_version.parse() calls across multiple (constraint, date) queries.
        """
        if (self.ecosystem, package_name) in self.cache.missing_packages:
            return None
        cmp_date = at_date.replace(tzinfo=timezone.utc) if at_date.tzinfo is None else at_date
        try:
            sorted_dates, _, sorted_parsed = self._get_preprocessed_versions(package_name)
        except Exception as e:
            logger.warning("Error getting preprocessed versions for %s: %s", package_name, e)
            return None
        if not sorted_dates:
            return None
        idx = bisect.bisect_right(sorted_dates, cmp_date) - 1
        if idx < 0:
            return None
        specifier = (
            SpecifierSet(constraint) if constraint and constraint != "*" else SpecifierSet("")
        )
        best = None
        for i in range(idx + 1):
            parsed = sorted_parsed[i]
            if parsed is not None and parsed in specifier:
                if best is None or parsed > best:
                    best = parsed
        return str(best) if best is not None else None

    def get_highest_semver_version_at_date(
        self, package_name: str, at_date: datetime, metadata: Optional[Dict] = None
    ) -> Optional[str]:
        if (self.ecosystem, package_name) in self.cache.missing_packages:
            return None
        try:
            sorted_dates, prefix_best_semver, _ = self._get_preprocessed_versions(package_name)
            if not sorted_dates:
                return None
            idx = bisect.bisect_right(sorted_dates, at_date) - 1
            if idx < 0:
                return None
            return prefix_best_semver[idx]
        except Exception as e:
            logger.warning("Error getting highest semver version for %s: %s", package_name, e)

        return None

    def extract_dependencies(self, version_data: Dict) -> Dict[str, str]:
        requires_dist = version_data.get("requires_dist", [])
        deps = {}
        for req in requires_dist or []:
            try:
                requirement = Requirement(req)
            except Exception:
                continue
            if requirement.extras:
                continue
            constraint = str(requirement.specifier) if requirement.specifier else "*"
            deps[requirement.name] = constraint
        return deps

    def get_version_dependencies(self, package: str, version: str) -> Dict[str, str]:
        cache_key = f"{package}@{version}"
        if cache_key in self.cache.pypi_version_deps_cache:
            logger.debug("Cache hit: pypi deps %s", cache_key)
            return self.cache.pypi_version_deps_cache[cache_key]

        deps: Dict[str, str] = {}
        try:
            version_metadata = self._get_pypi_version_metadata(package, version)
            version_data = {
                "requires_dist": version_metadata.get("info", {}).get("requires_dist", [])
            }
            deps = self.extract_dependencies(version_data)
        except Exception as e:
            logger.warning("Failed to fetch dependencies for %s==%s: %s", package, version, e)

        self.cache.pypi_version_deps_set(cache_key, deps)
        return deps

    def _get_pypi_version_metadata(self, package: str, version: str) -> Dict:
        cache_key = f"{package}@{version}"
        if cache_key in self.cache.pypi_version_metadata_cache:
            logger.debug("Cache hit: pypi version metadata %s", cache_key)
            return self.cache.pypi_version_metadata_cache[cache_key]

        disk_key = f"{package}:{version}"
        cached = self.cache.load_json("pypi_version", disk_key)
        if cached is not None:
            self.cache.pypi_version_metadata_set(cache_key, cached)
            return cached

        encoded_package = quote(package, safe="")
        encoded_version = quote(version, safe="")
        version_url = f"{self.registry_urls['pypi']}/{encoded_package}/{encoded_version}/json"
        with self.cache.get(version_url) as response:
            response.raise_for_status()
            data = response.json()
        self.cache.pypi_version_metadata_set(cache_key, data)
        self.cache.save_json("pypi_version", disk_key, data)
        return data
