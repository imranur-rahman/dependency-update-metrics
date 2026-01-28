"""
Ecosystem-specific package resolvers.
"""

from __future__ import annotations

import json
import logging
import subprocess
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Iterable, Optional, Tuple

import requests
from packaging import version as pkg_version
from packaging.requirements import Requirement

from .interfaces import PackageResolver
from .models import PackageVersion
from .pypi_resolver import resolve_pypi_version
from .time_utils import parse_timestamp


logger = logging.getLogger(__name__)


@dataclass
class ResolverCache:
    """Shared in-memory caches for resolver operations."""

    metadata_cache: Dict[Tuple[str, str], Dict] = field(default_factory=dict)
    pypi_version_metadata_cache: Dict[str, Dict] = field(default_factory=dict)
    pypi_version_deps_cache: Dict[str, Dict[str, str]] = field(default_factory=dict)
    npm_time_cache: Dict[str, Dict[str, str]] = field(default_factory=dict)
    npm_resolve_cache: Dict[Tuple[str, str, str], Optional[str]] = field(default_factory=dict)
    session: requests.Session = field(default_factory=requests.Session)


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
        if cache_key in self.cache.metadata_cache:
            logger.debug("Cache hit: metadata %s:%s", self.ecosystem, package_name)
            return self.cache.metadata_cache[cache_key]

        url = f"{self.registry_urls['npm']}/{package_name}"
        logger.info("Fetching metadata for %s", package_name)
        with self.cache.session.get(url) as response:
            response.raise_for_status()
            data = response.json()
        self.cache.metadata_cache[cache_key] = data
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
                    versions = metadata.get('versions', {})
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
                                version_dates.append(PackageVersion(
                                    name=package_name,
                                    version=ver,
                                    released_at=pub_date,
                                ))
                        except (ValueError, AttributeError):
                            continue
                    return sorted(version_dates, key=lambda x: x.released_at)
            except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError) as e:
                logger.warning("npm view time failed for %s, falling back to metadata: %s", package_name, e)

        versions = metadata.get('versions', {})
        version_dates = []
        for ver, ver_data in versions.items():
            published = ver_data.get('dist', {}).get('published')
            if not published:
                continue
            try:
                pub_date = parse_timestamp(published)
                if pub_date is None:
                    continue
                if self.start_date <= pub_date <= self.end_date:
                    version_dates.append(PackageVersion(
                        name=package_name or self.package,
                        version=ver,
                        released_at=pub_date,
                    ))
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

        try:
            cmd = [
                'npm', 'view',
                f'{dependency}@{constraint}',
                'version',
                '--json',
                '--before', before_date.isoformat()
            ]
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            if result.returncode == 0:
                output = result.stdout.strip()
                if output:
                    versions = json.loads(output)
                    if isinstance(versions, list):
                        resolved = versions[-1]
                    else:
                        resolved = versions
                    self.cache.npm_resolve_cache[cache_key] = resolved
                    return resolved
        except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError) as e:
            logger.warning("Error resolving npm version for %s: %s", dependency, e)

        self.cache.npm_resolve_cache[cache_key] = None
        return None

    def get_highest_semver_version_at_date(
        self, package_name: str, at_date: datetime, metadata: Optional[Dict] = None
    ) -> Optional[str]:
        try:
            if metadata is None:
                metadata = self.fetch_package_metadata(package_name)

            valid_versions = []
            try:
                time_data = self._get_npm_time_data(package_name)
                if time_data:
                    for ver, timestamp in time_data.items():
                        try:
                            pub_date = parse_timestamp(timestamp)
                            if pub_date is None:
                                continue
                            if pub_date <= at_date:
                                valid_versions.append(ver)
                        except (ValueError, AttributeError):
                            continue
            except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError) as e:
                logger.warning("npm view time failed for %s, falling back to metadata: %s", package_name, e)

            if not valid_versions:
                versions = metadata.get('versions', {})
                for ver, ver_data in versions.items():
                    published = ver_data.get('dist', {}).get('published')
                    if not published:
                        continue
                    try:
                        pub_date = parse_timestamp(published)
                        if pub_date is None:
                            continue
                        if pub_date <= at_date:
                            valid_versions.append(ver)
                    except (ValueError, AttributeError):
                        continue

            if valid_versions:
                valid_versions.sort(key=lambda v: pkg_version.parse(v))
                return valid_versions[-1]
        except Exception as e:
            message = f"Error getting highest semver version for {package_name}: {e}"
            logger.warning(message)
            raise RuntimeError(message) from e

        return None

    def extract_dependencies(self, version_data: Dict) -> Dict[str, str]:
        return version_data.get('dependencies', {})

    def get_version_dependencies(self, package: str, version: str) -> Dict[str, str]:
        metadata = self.fetch_package_metadata(package)
        ver_data = metadata.get('versions', {}).get(version, {})
        return ver_data.get('dependencies', {})

    def _get_npm_time_data(self, package_name: str) -> Optional[Dict[str, str]]:
        if package_name in self.cache.npm_time_cache:
            logger.debug("Cache hit: npm time %s", package_name)
            return self.cache.npm_time_cache[package_name]

        cmd = ['npm', 'view', package_name, 'time', '--json']
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30
        )
        if result.returncode != 0:
            return None

        time_data = json.loads(result.stdout)
        time_data.pop('modified', None)
        time_data.pop('created', None)
        self.cache.npm_time_cache[package_name] = time_data
        return time_data

    def _parse_versions_from_metadata(self, metadata: Dict) -> Tuple[str, Dict]:
        versions = metadata.get('versions', {})
        valid_versions = []

        for ver, ver_data in versions.items():
            published = ver_data.get('dist', {}).get('published')
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
        if cache_key in self.cache.metadata_cache:
            logger.debug("Cache hit: metadata %s:%s", self.ecosystem, package_name)
            return self.cache.metadata_cache[cache_key]

        url = f"{self.registry_urls['pypi']}/{package_name}/json"
        logger.info("Fetching metadata for %s", package_name)
        with self.cache.session.get(url) as response:
            response.raise_for_status()
            data = response.json()
        self.cache.metadata_cache[cache_key] = data
        return data

    def get_package_version_at_date(self, metadata: Dict) -> Tuple[str, Dict]:
        releases = metadata.get('releases', {})
        valid_versions = []

        for ver, release_files in releases.items():
            if not release_files:
                continue

            upload_time = release_files[0].get('upload_time')
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
                'upload_time': valid_versions[0][1].isoformat(),
                'requires_dist': version_metadata.get('info', {}).get('requires_dist', [])
            }
            return latest_version, version_data
        except Exception as e:
            logger.warning("Failed to fetch version-specific data: %s", e)
            return latest_version, {'upload_time': valid_versions[0][1].isoformat(), 'requires_dist': []}

    def get_all_versions_with_dates(
        self, metadata: Dict, package_name: Optional[str] = None
    ) -> Iterable[PackageVersion]:
        releases = metadata.get('releases', {})
        version_dates = []

        for ver, ver_data in releases.items():
            if isinstance(ver_data, list) and ver_data:
                published = ver_data[0].get('upload_time')
            else:
                published = ver_data.get('upload_time') if isinstance(ver_data, dict) else None

            if not published:
                continue

            try:
                pub_date = parse_timestamp(published)
                if pub_date is None:
                    continue
                if self.start_date <= pub_date <= self.end_date:
                    version_dates.append(PackageVersion(
                        name=package_name or self.package,
                        version=ver,
                        released_at=pub_date,
                    ))
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

    def get_highest_semver_version_at_date(
        self, package_name: str, at_date: datetime, metadata: Optional[Dict] = None
    ) -> Optional[str]:
        try:
            if metadata is None:
                metadata = self.fetch_package_metadata(package_name)

            valid_versions = []
            releases = metadata.get('releases', {})
            for ver, release_files in releases.items():
                if not release_files:
                    continue
                upload_time = release_files[0].get('upload_time')
                if not upload_time:
                    continue
                try:
                    upload_date = parse_timestamp(upload_time)
                    if upload_date is None:
                        continue
                    if upload_date <= at_date:
                        valid_versions.append(ver)
                except (ValueError, AttributeError):
                    continue

            if valid_versions:
                valid_versions.sort(key=lambda v: pkg_version.parse(v))
                return valid_versions[-1]
        except Exception as e:
            message = f"Error getting highest semver version for {package_name}: {e}"
            logger.warning(message)
            raise RuntimeError(message) from e

        return None

    def extract_dependencies(self, version_data: Dict) -> Dict[str, str]:
        requires_dist = version_data.get('requires_dist', [])
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
                'requires_dist': version_metadata.get('info', {}).get('requires_dist', [])
            }
            deps = self.extract_dependencies(version_data)
        except Exception as e:
            logger.warning("Failed to fetch dependencies for %s==%s: %s", package, version, e)

        self.cache.pypi_version_deps_cache[cache_key] = deps
        return deps

    def _get_pypi_version_metadata(self, package: str, version: str) -> Dict:
        cache_key = f"{package}@{version}"
        if cache_key in self.cache.pypi_version_metadata_cache:
            logger.debug("Cache hit: pypi version metadata %s", cache_key)
            return self.cache.pypi_version_metadata_cache[cache_key]

        version_url = f"{self.registry_urls['pypi']}/{package}/{version}/json"
        with self.cache.session.get(version_url) as response:
            response.raise_for_status()
            data = response.json()
        self.cache.pypi_version_metadata_cache[cache_key] = data
        return data
