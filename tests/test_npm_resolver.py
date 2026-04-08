"""Tests for NpmResolver in dependency_metrics/resolvers.py."""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from dependency_metrics.resolvers import NpmResolver, ResolverCache


def _utc(year: int, month: int, day: int) -> datetime:
    return datetime(year, month, day, tzinfo=timezone.utc)


def _make_resolver(tmp_path: Path, package: str = "lodash") -> NpmResolver:
    cache = ResolverCache(cache_dir=tmp_path / "cache")
    return NpmResolver(
        package=package,
        start_date=_utc(2018, 1, 1),
        end_date=_utc(2022, 1, 1),
        registry_urls={"npm": "https://registry.npmjs.org"},
        cache=cache,
    )


def _time_data_3_versions() -> dict:
    """npm time dict with 3 stable versions published across 2019-2021."""
    return {
        "1.0.0": "2019-01-01T00:00:00Z",
        "2.0.0": "2020-06-01T00:00:00Z",
        "3.0.0": "2021-09-01T00:00:00Z",
    }


# ---------------------------------------------------------------------------
# get_highest_semver_version_at_date
# ---------------------------------------------------------------------------


def test_get_highest_semver_version_at_date_basic(tmp_path):
    resolver = _make_resolver(tmp_path)

    with patch.object(resolver, "_get_npm_time_data", return_value=_time_data_3_versions()):
        result = resolver.get_highest_semver_version_at_date("lodash", _utc(2020, 7, 1))

    assert result == "2.0.0"


def test_get_highest_semver_version_at_date_returns_latest_when_all_published(tmp_path):
    resolver = _make_resolver(tmp_path)

    with patch.object(resolver, "_get_npm_time_data", return_value=_time_data_3_versions()):
        result = resolver.get_highest_semver_version_at_date("lodash", _utc(2022, 1, 1))

    assert result == "3.0.0"


def test_get_highest_semver_version_at_date_filters_future_versions(tmp_path):
    resolver = _make_resolver(tmp_path)

    with patch.object(resolver, "_get_npm_time_data", return_value=_time_data_3_versions()):
        # Before any version is published
        result = resolver.get_highest_semver_version_at_date("lodash", _utc(2018, 6, 1))

    assert result is None


def test_get_highest_semver_version_at_date_prefers_stable_over_prerelease(tmp_path):
    resolver = _make_resolver(tmp_path)
    # Same version: stable release beats the prerelease with identical major.minor.patch.
    # npm_semver_key uses is_release=1 for stable, is_release=0 for pre-release,
    # so (1,0,0,1,()) > (1,0,0,0,...) → stable wins.
    time_data = {
        "1.0.0-beta.1": "2019-06-01T00:00:00Z",
        "1.0.0": "2020-01-01T00:00:00Z",
    }

    with patch.object(resolver, "_get_npm_time_data", return_value=time_data):
        result = resolver.get_highest_semver_version_at_date("lodash", _utc(2021, 1, 1))

    assert result == "1.0.0"


def test_get_highest_semver_version_at_date_missing_package_returns_none(tmp_path):
    resolver = _make_resolver(tmp_path)
    resolver.cache.missing_packages.add(("npm", "no-such-pkg"))

    result = resolver.get_highest_semver_version_at_date("no-such-pkg", _utc(2021, 1, 1))
    assert result is None


# ---------------------------------------------------------------------------
# resolve_dependency_version: memory cache hit
# ---------------------------------------------------------------------------


def test_resolve_dependency_version_memory_cache_hit(tmp_path):
    resolver = _make_resolver(tmp_path)
    key = ("express", "^4.0.0", _utc(2021, 1, 1).isoformat())
    resolver.cache.npm_resolve_cache[key] = "4.18.0"

    # No subprocess should run — returns from cache
    result = resolver.resolve_dependency_version("express", "^4.0.0", _utc(2021, 1, 1))
    assert result == "4.18.0"


def test_resolve_dependency_version_disk_cache_hit(tmp_path):
    resolver = _make_resolver(tmp_path)
    disk_key = "npm:express|^4.0.0|2021-01-01T00:00:00+00:00"
    resolver.cache.save_json("resolve_npm", disk_key, {"version": "4.17.0"})

    result = resolver.resolve_dependency_version("express", "^4.0.0", _utc(2021, 1, 1))
    assert result == "4.17.0"


# ---------------------------------------------------------------------------
# _parse_versions_from_metadata
# ---------------------------------------------------------------------------


def test_parse_versions_from_metadata_returns_latest(tmp_path):
    resolver = _make_resolver(tmp_path)
    metadata = {
        "versions": {
            "1.0.0": {"dist": {"published": "2019-01-01T00:00:00Z"}},
            "2.0.0": {"dist": {"published": "2020-06-01T00:00:00Z"}},
        }
    }

    latest_ver, _ = resolver._parse_versions_from_metadata(metadata)
    assert latest_ver == "2.0.0"


def test_parse_versions_from_metadata_all_future_raises(tmp_path):
    resolver = _make_resolver(tmp_path)
    # All versions are after resolver.end_date (2022-01-01)
    metadata = {
        "versions": {
            "1.0.0": {"dist": {"published": "2025-01-01T00:00:00Z"}},
        }
    }

    with pytest.raises(ValueError, match="No versions found"):
        resolver._parse_versions_from_metadata(metadata)


def test_parse_versions_from_metadata_ignores_missing_published(tmp_path):
    resolver = _make_resolver(tmp_path)
    metadata = {
        "versions": {
            "1.0.0": {"dist": {}},  # no "published" field
            "2.0.0": {"dist": {"published": "2020-01-01T00:00:00Z"}},
        }
    }

    latest_ver, _ = resolver._parse_versions_from_metadata(metadata)
    assert latest_ver == "2.0.0"


# ---------------------------------------------------------------------------
# _get_preprocessed_versions: version_prefix_cache hit
# ---------------------------------------------------------------------------


def test_get_preprocessed_versions_returns_from_cache(tmp_path):
    resolver = _make_resolver(tmp_path)
    cached_result = ([], [], [])
    resolver.cache.version_prefix_set(("npm", "lodash"), cached_result)

    with patch.object(resolver, "_get_npm_time_data") as mock_time:
        result = resolver._get_preprocessed_versions("lodash")
        mock_time.assert_not_called()

    assert result is cached_result


def test_get_preprocessed_versions_populates_cache(tmp_path):
    resolver = _make_resolver(tmp_path)

    with patch.object(resolver, "_get_npm_time_data", return_value=_time_data_3_versions()):
        resolver._get_preprocessed_versions("lodash")

    assert ("npm", "lodash") in resolver.cache.version_prefix_cache


# ---------------------------------------------------------------------------
# extract_dependencies / get_version_dependencies
# ---------------------------------------------------------------------------


def test_extract_dependencies_returns_deps_dict(tmp_path):
    resolver = _make_resolver(tmp_path)
    ver_data = {"dependencies": {"express": "^4.0.0", "lodash": "^4.17"}}
    result = resolver.extract_dependencies(ver_data)
    assert result == {"express": "^4.0.0", "lodash": "^4.17"}


def test_extract_dependencies_missing_key_returns_empty(tmp_path):
    resolver = _make_resolver(tmp_path)
    assert resolver.extract_dependencies({}) == {}
