"""Tests for DepsDevResolver in dependency_metrics/depsdev_resolver.py."""

import threading
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from dependency_metrics.depsdev_client import DepsDevClient
from dependency_metrics.depsdev_resolver import (
    DepsDevResolver,
    _best_semver,
    _match_constraint,
    _match_npm_or_cargo,
    _match_pypi,
)
from dependency_metrics.models import PackageVersion
from dependency_metrics.resolvers import ResolverCache

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utc(year: int, month: int, day: int) -> datetime:
    return datetime(year, month, day, tzinfo=timezone.utc)


def _make_resolver(system: str = "NPM", package: str = "express") -> DepsDevResolver:
    client = MagicMock(spec=DepsDevClient)
    client._cache = ResolverCache(cache_dir=None)
    return DepsDevResolver(
        system=system,
        package=package,
        start_date=_utc(2020, 1, 1),
        end_date=_utc(2023, 1, 1),
        client=client,
    )


def _package_response(*entries) -> dict:
    """Build a minimal GetPackage response dict.

    Each entry is (version_str, published_at_str).
    """
    return {
        "packageKey": {"system": "NPM", "name": "express"},
        "versions": [
            {"versionKey": {"system": "NPM", "name": "express", "version": v}, "publishedAt": d}
            for v, d in entries
        ],
    }


def _npm_requirements_response(*runtime_deps, dev_deps=(), optional_deps=()) -> dict:
    """Build an npm-format GetRequirements response.

    Each dep is a ``(name, requirement)`` tuple.
    """
    return {
        "npm": {
            "dependencies": {
                "dependencies": [{"name": n, "requirement": r} for n, r in runtime_deps],
                "devDependencies": [{"name": n, "requirement": r} for n, r in dev_deps],
                "optionalDependencies": [{"name": n, "requirement": r} for n, r in optional_deps],
                "peerDependencies": [],
                "bundleDependencies": [],
                "peerDependencyMetadata": [],
            },
            "bundled": [],
            "os": [],
            "cpu": [],
        }
    }


def _pypi_requirements_response(*deps) -> dict:
    """Build a PyPI-format GetRequirements response.

    Each dep is a ``(project_name, version_specifier, environment_marker)`` tuple.
    """
    return {
        "pypi": {
            "dependencies": [
                {
                    "projectName": name,
                    "versionSpecifier": spec,
                    "environmentMarker": marker,
                    "extras": "",
                }
                for name, spec, marker in deps
            ],
            "providedExtras": [],
            "externalDependencies": [],
            "requiredPythonVersion": ">=3.8",
        }
    }


def _cargo_requirements_response(*deps) -> dict:
    """Build a Cargo-format GetRequirements response.

    Each dep is a ``(name, requirement, kind)`` tuple where kind is one of
    ``"normal"``, ``"dev"``, or ``"build"``.
    """
    return {
        "cargo": {
            "dependencies": [
                {"name": n, "requirement": r, "kind": k, "optional": False} for n, r, k in deps
            ]
        }
    }


# ---------------------------------------------------------------------------
# _match_npm_or_cargo
# ---------------------------------------------------------------------------


def test_match_npm_caret_returns_highest_compatible(tmp_path):
    versions = ["1.0.0", "1.2.3", "1.9.9", "2.0.0"]
    result = _match_npm_or_cargo(versions, "^1.0.0")
    assert result == "1.9.9"


def test_match_npm_tilde_restricts_to_patch(tmp_path):
    versions = ["1.2.0", "1.2.5", "1.3.0"]
    result = _match_npm_or_cargo(versions, "~1.2.0")
    assert result == "1.2.5"


def test_match_npm_wildcard_returns_highest(tmp_path):
    versions = ["1.0.0", "2.0.0", "3.0.0"]
    result = _match_npm_or_cargo(versions, "*")
    assert result == "3.0.0"


def test_match_npm_empty_constraint_returns_highest(tmp_path):
    versions = ["1.0.0", "2.0.0"]
    result = _match_npm_or_cargo(versions, "")
    assert result == "2.0.0"


def test_match_npm_range_respects_upper_bound(tmp_path):
    versions = ["1.0.0", "1.5.0", "2.0.0", "2.5.0"]
    result = _match_npm_or_cargo(versions, ">=1.0.0 <2.0.0")
    assert result == "1.5.0"


def test_match_npm_exact_version(tmp_path):
    versions = ["1.0.0", "1.1.0", "2.0.0"]
    result = _match_npm_or_cargo(versions, "1.0.0")
    assert result == "1.0.0"


def test_match_npm_returns_none_when_no_match(tmp_path):
    versions = ["1.0.0", "1.1.0"]
    result = _match_npm_or_cargo(versions, "^2.0.0")
    assert result is None


def test_match_npm_skips_unparseable_versions(tmp_path):
    versions = ["not-semver", "1.0.0", "also-bad"]
    result = _match_npm_or_cargo(versions, "*")
    assert result == "1.0.0"


def test_match_npm_returns_none_for_empty_list(tmp_path):
    assert _match_npm_or_cargo([], "^1.0.0") is None


# ---------------------------------------------------------------------------
# _match_pypi
# ---------------------------------------------------------------------------


def test_match_pypi_gte_returns_highest(tmp_path):
    versions = ["1.0.0", "2.0.0", "3.0.0"]
    result = _match_pypi(versions, ">=2.0.0")
    assert result == "3.0.0"


def test_match_pypi_range_respects_upper_bound(tmp_path):
    versions = ["1.0.0", "1.5.0", "2.0.0"]
    result = _match_pypi(versions, ">=1.0.0,<2.0.0")
    assert result == "1.5.0"


def test_match_pypi_exact_pin(tmp_path):
    versions = ["1.0.0", "1.1.0", "2.0.0"]
    result = _match_pypi(versions, "==1.1.0")
    assert result == "1.1.0"


def test_match_pypi_wildcard_returns_highest(tmp_path):
    versions = ["0.9.0", "1.0.0", "2.0.0"]
    result = _match_pypi(versions, "*")
    assert result == "2.0.0"


def test_match_pypi_empty_constraint_returns_highest(tmp_path):
    versions = ["1.0.0", "2.0.0"]
    result = _match_pypi(versions, "")
    assert result == "2.0.0"


def test_match_pypi_excludes_version_with_ne(tmp_path):
    versions = ["1.0.0", "1.1.0", "2.0.0"]
    result = _match_pypi(versions, "!=1.1.0,<2.0.0")
    assert result == "1.0.0"


def test_match_pypi_returns_none_when_no_match(tmp_path):
    versions = ["1.0.0", "1.5.0"]
    result = _match_pypi(versions, ">=2.0.0")
    assert result is None


def test_match_pypi_skips_unparseable_version_strings(tmp_path):
    versions = ["bad-version", "1.0.0"]
    result = _match_pypi(versions, ">=1.0.0")
    assert result == "1.0.0"


# ---------------------------------------------------------------------------
# _match_constraint dispatch
# ---------------------------------------------------------------------------


def test_match_constraint_routes_pypi(tmp_path):
    result = _match_constraint("PYPI", ["1.0.0", "2.0.0"], ">=1.0.0,<2.0.0")
    assert result == "1.0.0"


def test_match_constraint_routes_npm(tmp_path):
    result = _match_constraint("NPM", ["1.0.0", "1.5.0", "2.0.0"], "^1.0.0")
    assert result == "1.5.0"


def test_match_constraint_routes_cargo_same_as_npm(tmp_path):
    # Cargo uses the same caret semantics as npm
    result = _match_constraint("CARGO", ["0.1.0", "0.1.9", "0.2.0"], "^0.1.0")
    assert result == "0.1.9"


# ---------------------------------------------------------------------------
# _best_semver
# ---------------------------------------------------------------------------


def test_best_semver_npm_prefers_stable_over_prerelease(tmp_path):
    versions = ["1.0.0-alpha", "1.0.0", "2.0.0-beta"]
    assert _best_semver("NPM", versions) == "1.0.0"


def test_best_semver_npm_falls_back_to_prerelease_when_no_stable(tmp_path):
    versions = ["1.0.0-alpha", "2.0.0-beta"]
    assert _best_semver("NPM", versions) == "2.0.0-beta"


def test_best_semver_pypi_prefers_stable(tmp_path):
    versions = ["1.0.0a1", "1.0.0", "2.0.0b1"]
    assert _best_semver("PYPI", versions) == "1.0.0"


def test_best_semver_returns_none_for_empty_list(tmp_path):
    assert _best_semver("NPM", []) is None


def test_best_semver_cargo_highest_stable(tmp_path):
    versions = ["0.9.0", "1.0.0", "1.1.0-rc1", "1.0.5"]
    assert _best_semver("CARGO", versions) == "1.0.5"


# ---------------------------------------------------------------------------
# DepsDevResolver.fetch_package_metadata
# ---------------------------------------------------------------------------


def test_fetch_package_metadata_calls_client(tmp_path):
    resolver = _make_resolver()
    data = _package_response(("1.0.0", "2020-01-01T00:00:00Z"))
    resolver._client.get_package.return_value = data

    result = resolver.fetch_package_metadata("express")

    resolver._client.get_package.assert_called_once_with("NPM", "express")
    assert result == data


def test_fetch_package_metadata_memoises_result(tmp_path):
    resolver = _make_resolver()
    resolver._client.get_package.return_value = _package_response()

    resolver.fetch_package_metadata("express")
    resolver.fetch_package_metadata("express")

    assert resolver._client.get_package.call_count == 1


def test_fetch_package_metadata_returns_empty_versions_on_error(tmp_path):
    resolver = _make_resolver()
    resolver._client.get_package.side_effect = Exception("network error")

    result = resolver.fetch_package_metadata("no-such-pkg")
    assert result == {"versions": []}


# ---------------------------------------------------------------------------
# DepsDevResolver.get_all_versions_with_dates
# ---------------------------------------------------------------------------


def test_get_all_versions_with_dates_yields_package_versions(tmp_path):
    resolver = _make_resolver()
    metadata = _package_response(
        ("1.0.0", "2020-01-01T00:00:00Z"),
        ("2.0.0", "2021-06-01T00:00:00Z"),
    )

    versions = list(resolver.get_all_versions_with_dates(metadata, package_name="express"))

    assert len(versions) == 2
    assert all(isinstance(v, PackageVersion) for v in versions)
    assert versions[0].version == "1.0.0"
    assert versions[1].version == "2.0.0"
    assert versions[0].released_at == _utc(2020, 1, 1)


def test_get_all_versions_with_dates_skips_missing_published_at(tmp_path):
    resolver = _make_resolver()
    metadata = {
        "versions": [
            {"versionKey": {"version": "1.0.0"}, "publishedAt": ""},
            {"versionKey": {"version": "2.0.0"}, "publishedAt": "2021-01-01T00:00:00Z"},
        ]
    }

    versions = list(resolver.get_all_versions_with_dates(metadata))
    assert len(versions) == 1
    assert versions[0].version == "2.0.0"


def test_get_all_versions_with_dates_skips_entries_missing_version(tmp_path):
    resolver = _make_resolver()
    metadata = {
        "versions": [
            {"versionKey": {"version": ""}, "publishedAt": "2020-01-01T00:00:00Z"},
            {"versionKey": {"version": "1.0.0"}, "publishedAt": "2020-06-01T00:00:00Z"},
        ]
    }

    versions = list(resolver.get_all_versions_with_dates(metadata))
    assert len(versions) == 1
    assert versions[0].version == "1.0.0"


# ---------------------------------------------------------------------------
# DepsDevResolver.get_package_version_at_date
# ---------------------------------------------------------------------------


def test_get_package_version_at_date_returns_highest_stable_before_end(tmp_path):
    resolver = DepsDevResolver(
        system="NPM",
        package="express",
        start_date=_utc(2020, 1, 1),
        end_date=_utc(2022, 1, 1),
        client=MagicMock(),
    )
    metadata = _package_response(
        ("1.0.0", "2020-01-01T00:00:00Z"),
        ("2.0.0", "2021-06-01T00:00:00Z"),
        ("3.0.0", "2023-01-02T00:00:00Z"),  # after end_date
    )

    version, stub = resolver.get_package_version_at_date(metadata)

    assert version == "2.0.0"
    assert stub["_version"] == "2.0.0"
    assert stub["_package"] == "express"


def test_get_package_version_at_date_raises_when_all_versions_in_future(tmp_path):
    resolver = DepsDevResolver(
        system="NPM",
        package="express",
        start_date=_utc(2020, 1, 1),
        end_date=_utc(2021, 1, 1),
        client=MagicMock(),
    )
    metadata = _package_response(("1.0.0", "2022-01-01T00:00:00Z"))

    with pytest.raises(ValueError, match="No version"):
        resolver.get_package_version_at_date(metadata)


def test_get_package_version_at_date_prefers_stable_over_prerelease(tmp_path):
    resolver = DepsDevResolver(
        system="NPM",
        package="pkg",
        start_date=_utc(2020, 1, 1),
        end_date=_utc(2023, 1, 1),
        client=MagicMock(),
    )
    metadata = _package_response(
        ("1.0.0", "2021-01-01T00:00:00Z"),
        ("2.0.0-beta.1", "2022-01-01T00:00:00Z"),
    )

    version, _ = resolver.get_package_version_at_date(metadata)
    assert version == "1.0.0"


# ---------------------------------------------------------------------------
# DepsDevResolver.extract_dependencies / get_version_dependencies
# ---------------------------------------------------------------------------


def test_extract_dependencies_calls_get_requirements_via_stub(tmp_path):
    resolver = _make_resolver()
    resolver._client.get_requirements.return_value = _npm_requirements_response(
        ("body-parser", "^1.20.0"),
        ("accepts", "~1.3.8"),
    )
    stub = {"_package": "express", "_version": "4.18.0"}

    deps = resolver.extract_dependencies(stub)

    resolver._client.get_requirements.assert_called_once_with("NPM", "express", "4.18.0")
    assert deps == {"body-parser": "^1.20.0", "accepts": "~1.3.8"}


def test_extract_dependencies_excludes_dev_deps(tmp_path):
    resolver = _make_resolver()
    resolver._client.get_requirements.return_value = _npm_requirements_response(
        ("express", "^4.18.0"),
        dev_deps=[("typescript", "^5.0.0")],
    )

    deps = resolver.extract_dependencies({"_package": "pkg", "_version": "1.0.0"})

    assert "typescript" not in deps
    assert deps == {"express": "^4.18.0"}


def test_extract_dependencies_returns_empty_on_client_error(tmp_path):
    resolver = _make_resolver()
    resolver._client.get_requirements.side_effect = Exception("timeout")

    deps = resolver.extract_dependencies({"_package": "pkg", "_version": "1.0.0"})
    assert deps == {}


def test_extract_dependencies_returns_empty_when_version_missing_from_stub(tmp_path):
    resolver = _make_resolver()
    deps = resolver.extract_dependencies({"_package": "pkg"})
    assert deps == {}
    resolver._client.get_requirements.assert_not_called()


def test_get_version_dependencies_npm_runtime_deps(tmp_path):
    resolver = _make_resolver()
    resolver._client.get_requirements.return_value = _npm_requirements_response(
        ("dep-a", "^1.0.0"),
        ("dep-b", "^2.0.0"),
    )

    result = resolver.get_version_dependencies("express", "4.18.0")
    assert result == {"dep-a": "^1.0.0", "dep-b": "^2.0.0"}


def test_get_version_dependencies_npm_excludes_dev_deps(tmp_path):
    resolver = _make_resolver()
    resolver._client.get_requirements.return_value = _npm_requirements_response(
        ("express", "^4.18.0"),
        dev_deps=[("typescript", "^5.0.0")],
    )

    result = resolver.get_version_dependencies("myapp", "1.0.0")
    assert result == {"express": "^4.18.0"}
    assert "typescript" not in result


def test_get_version_dependencies_npm_excludes_optional_deps(tmp_path):
    resolver = _make_resolver()
    resolver._client.get_requirements.return_value = _npm_requirements_response(
        ("express", "^4.18.0"),
        optional_deps=[("colors", "^1.0.0")],
    )

    result = resolver.get_version_dependencies("myapp", "1.0.0")
    assert result == {"express": "^4.18.0"}
    assert "colors" not in result


def test_get_version_dependencies_npm_empty(tmp_path):
    resolver = _make_resolver()
    resolver._client.get_requirements.return_value = _npm_requirements_response()

    result = resolver.get_version_dependencies("express", "4.18.0")
    assert result == {}


def test_get_version_dependencies_pypi_runtime_deps(tmp_path):
    resolver = _make_resolver(system="PYPI", package="mypackage")
    resolver._client.get_requirements.return_value = _pypi_requirements_response(
        ("click", ">=7.0", ""),
        ("numpy", "", ""),
        ("scipy", ">=1.0", ""),
    )

    result = resolver.get_version_dependencies("mypackage", "1.0.0")
    assert result == {"click": ">=7.0", "numpy": "", "scipy": ">=1.0"}


def test_get_version_dependencies_pypi_excludes_extras_gated(tmp_path):
    resolver = _make_resolver(system="PYPI", package="mypackage")
    resolver._client.get_requirements.return_value = _pypi_requirements_response(
        ("click", ">=7.0", ""),
        ("pyarrow", ">=4.0", "extra == 'pandas'"),
        ("torch", ">=1.1", ""),
    )

    result = resolver.get_version_dependencies("mypackage", "1.0.0")
    assert "pyarrow" not in result
    assert result == {"click": ">=7.0", "torch": ">=1.1"}


def test_get_version_dependencies_pypi_includes_version_conditional(tmp_path):
    resolver = _make_resolver(system="PYPI", package="mypackage")
    resolver._client.get_requirements.return_value = _pypi_requirements_response(
        ("typing-extensions", ">=4.0", "python_version < '3.10'"),
        ("numpy", "", ""),
    )

    result = resolver.get_version_dependencies("mypackage", "1.0.0")
    assert "typing-extensions" in result  # conditional but still required
    assert "numpy" in result


def test_get_version_dependencies_pypi_empty(tmp_path):
    resolver = _make_resolver(system="PYPI", package="mypackage")
    resolver._client.get_requirements.return_value = {"pypi": {"dependencies": []}}

    result = resolver.get_version_dependencies("mypackage", "1.0.0")
    assert result == {}


def test_get_version_dependencies_cargo_normal_deps(tmp_path):
    resolver = _make_resolver(system="CARGO", package="mycrate")
    resolver._client.get_requirements.return_value = _cargo_requirements_response(
        ("libc", "^0.2", "normal"),
        ("serde", "^1.0", "dev"),
        ("tokio", "^1.0", "build"),
    )

    result = resolver.get_version_dependencies("mycrate", "1.0.0")
    assert "libc" in result
    assert "serde" not in result  # dev dep excluded
    assert "tokio" not in result  # build dep excluded


def test_get_version_dependencies_cargo_excludes_optional(tmp_path):
    resolver = _make_resolver(system="CARGO", package="mycrate")
    resolver._client.get_requirements.return_value = {
        "cargo": {
            "dependencies": [
                {"name": "libc", "requirement": "^0.2", "kind": "normal", "optional": False},
                {"name": "serde", "requirement": "^1.0", "kind": "normal", "optional": True},
            ]
        }
    }

    result = resolver.get_version_dependencies("mycrate", "1.0.0")
    assert "libc" in result
    assert "serde" not in result  # optional dep excluded


# ---------------------------------------------------------------------------
# DepsDevResolver.resolve_dependency_version
# ---------------------------------------------------------------------------


def test_resolve_dependency_version_npm_caret(tmp_path):
    resolver = _make_resolver("NPM")
    resolver._client.get_package.return_value = _package_response(
        ("4.17.0", "2021-01-01T00:00:00Z"),
        ("4.18.2", "2022-01-01T00:00:00Z"),
        ("5.0.0", "2022-06-01T00:00:00Z"),
    )

    result = resolver.resolve_dependency_version("express", "^4.0.0", _utc(2022, 3, 1))
    assert result == "4.18.2"


def test_resolve_dependency_version_filters_future_releases(tmp_path):
    resolver = _make_resolver("NPM")
    resolver._client.get_package.return_value = _package_response(
        ("1.0.0", "2020-01-01T00:00:00Z"),
        ("2.0.0", "2025-01-01T00:00:00Z"),  # future
    )

    result = resolver.resolve_dependency_version("pkg", "^1.0.0", _utc(2021, 1, 1))
    assert result == "1.0.0"


def test_resolve_dependency_version_returns_none_when_no_match(tmp_path):
    resolver = _make_resolver("NPM")
    resolver._client.get_package.return_value = _package_response(
        ("1.0.0", "2020-01-01T00:00:00Z"),
    )

    result = resolver.resolve_dependency_version("pkg", "^2.0.0", _utc(2022, 1, 1))
    assert result is None


def test_resolve_dependency_version_memoises_result(tmp_path):
    resolver = _make_resolver("NPM")
    resolver._client.get_package.return_value = _package_response(
        ("1.0.0", "2020-01-01T00:00:00Z"),
    )

    resolver.resolve_dependency_version("pkg", "*", _utc(2021, 1, 1))
    resolver.resolve_dependency_version("pkg", "*", _utc(2021, 1, 1))

    # get_package only called once for the same dep (in-memory cache)
    assert resolver._client.get_package.call_count == 1


def test_resolve_dependency_version_pypi_specifier(tmp_path):
    resolver = _make_resolver("PYPI", "requests")
    resolver._client.get_package.return_value = _package_response(
        ("2.27.0", "2021-01-01T00:00:00Z"),
        ("2.28.0", "2022-01-01T00:00:00Z"),
        ("3.0.0", "2022-06-01T00:00:00Z"),
    )

    result = resolver.resolve_dependency_version("urllib3", ">=1.21.1,<3", _utc(2022, 3, 1))
    # All three pass >=1.21.1,<3 but 3.0.0 is excluded by <3
    assert result == "2.28.0"


def test_resolve_dependency_version_cargo_caret(tmp_path):
    resolver = _make_resolver("CARGO", "serde")
    resolver._client.get_package.return_value = _package_response(
        ("1.0.100", "2021-01-01T00:00:00Z"),
        ("1.0.196", "2022-01-01T00:00:00Z"),
        ("2.0.0", "2022-06-01T00:00:00Z"),
    )

    result = resolver.resolve_dependency_version("serde_json", "^1.0", _utc(2022, 3, 1))
    assert result == "1.0.196"


# ---------------------------------------------------------------------------
# DepsDevResolver.get_highest_semver_version_at_date
# ---------------------------------------------------------------------------


def test_get_highest_semver_version_at_date_returns_latest_stable(tmp_path):
    resolver = _make_resolver("NPM")
    resolver._client.get_package.return_value = _package_response(
        ("1.0.0", "2020-01-01T00:00:00Z"),
        ("2.0.0", "2021-01-01T00:00:00Z"),
        ("3.0.0-rc1", "2021-06-01T00:00:00Z"),
        ("4.0.0", "2025-01-01T00:00:00Z"),  # after query date
    )

    result = resolver.get_highest_semver_version_at_date("express", _utc(2022, 1, 1))
    assert result == "2.0.0"


def test_get_highest_semver_version_at_date_falls_back_to_prerelease(tmp_path):
    resolver = _make_resolver("NPM")
    resolver._client.get_package.return_value = _package_response(
        ("1.0.0-beta", "2020-01-01T00:00:00Z"),
    )

    result = resolver.get_highest_semver_version_at_date("pkg", _utc(2022, 1, 1))
    assert result == "1.0.0-beta"


def test_get_highest_semver_version_at_date_returns_none_when_nothing_before_date(tmp_path):
    resolver = _make_resolver("NPM")
    resolver._client.get_package.return_value = _package_response(
        ("1.0.0", "2025-01-01T00:00:00Z"),
    )

    result = resolver.get_highest_semver_version_at_date("pkg", _utc(2022, 1, 1))
    assert result is None


def test_get_highest_semver_version_at_date_uses_provided_metadata(tmp_path):
    resolver = _make_resolver("NPM")
    preloaded_metadata = _package_response(
        ("1.0.0", "2020-01-01T00:00:00Z"),
        ("2.0.0", "2021-01-01T00:00:00Z"),
    )

    result = resolver.get_highest_semver_version_at_date(
        "express", _utc(2022, 1, 1), metadata=preloaded_metadata
    )

    resolver._client.get_package.assert_not_called()
    assert result == "2.0.0"


# ---------------------------------------------------------------------------
# System / ecosystem attributes
# ---------------------------------------------------------------------------


def test_resolver_ecosystem_attribute_is_lowercase():
    resolver = _make_resolver("NPM")
    assert resolver.ecosystem == "npm"


def test_resolver_system_attribute_is_uppercase():
    resolver = _make_resolver("cargo")
    assert resolver.system == "CARGO"
    assert resolver.ecosystem == "cargo"


def test_resolver_pypi_ecosystem():
    resolver = _make_resolver("PYPI")
    assert resolver.ecosystem == "pypi"
    assert resolver.system == "PYPI"


# ---------------------------------------------------------------------------
# Cross-worker cache sharing via fetch_package_metadata
# ---------------------------------------------------------------------------


def _make_shared_client() -> MagicMock:
    """Return a mock DepsDevClient whose _cache is a real shared ResolverCache."""
    from dependency_metrics.resolvers import ResolverCache

    client = MagicMock(spec=DepsDevClient)
    client._cache = ResolverCache(cache_dir=None)
    return client


def _make_resolver_with_client(client, system="NPM", package="express") -> DepsDevResolver:
    return DepsDevResolver(
        system=system,
        package=package,
        start_date=_utc(2020, 1, 1),
        end_date=_utc(2023, 1, 1),
        client=client,
    )


def test_fetch_package_metadata_populates_shared_metadata_cache():
    """After a network fetch, the result is stored in the shared metadata_cache."""
    client = _make_shared_client()
    client.get_package.return_value = _package_response(("1.0.0", "2021-01-01T00:00:00Z"))
    resolver = _make_resolver_with_client(client)

    resolver.fetch_package_metadata("lodash")

    assert ("NPM", "lodash") in client._cache.metadata_cache


def test_second_resolver_reads_from_shared_cache_without_api_call():
    """A second resolver sharing the same client cache skips the network call."""
    client = _make_shared_client()
    client.get_package.return_value = _package_response(("1.0.0", "2021-01-01T00:00:00Z"))

    resolver1 = _make_resolver_with_client(client, package="pkg-a")
    resolver2 = _make_resolver_with_client(client, package="pkg-b")

    # Worker 1 fetches lodash — triggers one API call
    result1 = resolver1.fetch_package_metadata("lodash")

    # Worker 2 fetches lodash — should hit the shared metadata_cache, not the API
    result2 = resolver2.fetch_package_metadata("lodash")

    assert client.get_package.call_count == 1
    assert result1 == result2


def test_instance_cache_takes_priority_over_shared_cache():
    """Once a result is in the per-instance cache, the shared cache is not consulted."""
    client = _make_shared_client()
    instance_data = {"versions": [{"from": "instance_cache"}]}
    resolver = _make_resolver_with_client(client)

    # Pre-populate the per-instance cache directly
    resolver._package_cache["lodash"] = instance_data

    # Also put different data in the shared cache to confirm it is not checked
    client._cache.metadata_set(("NPM", "lodash"), {"versions": [{"from": "shared_cache"}]})

    result = resolver.fetch_package_metadata("lodash")

    assert result == instance_data
    client.get_package.assert_not_called()


def test_fetch_package_metadata_on_error_stores_empty_in_both_caches():
    """On API failure, an empty versions dict is stored in both caches."""
    client = _make_shared_client()
    client.get_package.side_effect = RuntimeError("network error")
    resolver = _make_resolver_with_client(client)

    result = resolver.fetch_package_metadata("broken-pkg")

    assert result == {"versions": []}
    # Per-instance cache should have it
    assert resolver._package_cache["broken-pkg"] == {"versions": []}
    # Shared metadata_cache should also have it (prevents other workers retrying)
    assert client._cache.metadata_cache.get(("NPM", "broken-pkg")) == {"versions": []}


def test_concurrent_workers_make_only_one_api_call_for_shared_dependency():
    """When N workers concurrently fetch the same dependency, get_package is called once.

    The first thread to resolve the key populates the shared metadata_cache; all
    subsequent threads find the cached entry and skip the network call.
    """
    client = _make_shared_client()
    client.get_package.return_value = _package_response(("4.17.21", "2021-01-01T00:00:00Z"))

    num_workers = 8
    barrier = threading.Barrier(num_workers)
    results = [None] * num_workers
    errors = []

    def worker(idx):
        resolver = _make_resolver_with_client(client, package=f"pkg-{idx}")
        barrier.wait()  # all threads start fetching simultaneously
        try:
            results[idx] = resolver.fetch_package_metadata("lodash")
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    # All workers should get the correct data
    expected = _package_response(("4.17.21", "2021-01-01T00:00:00Z"))
    for r in results:
        assert r == expected
    # Only one real API call should have been made (the rest hit shared cache)
    assert client.get_package.call_count == 1


def test_shared_cache_is_keyed_by_system_and_package_name():
    """Entries in the shared metadata_cache use (system, package_name) as the key."""
    client = _make_shared_client()
    npm_data = _package_response(("1.0.0", "2021-01-01T00:00:00Z"))
    pypi_data = _package_response(("2.0.0", "2021-01-01T00:00:00Z"))

    client.get_package.return_value = npm_data
    npm_resolver = _make_resolver_with_client(client, system="NPM")
    npm_resolver.fetch_package_metadata("requests")

    client.get_package.return_value = pypi_data
    pypi_resolver = _make_resolver_with_client(client, system="PYPI")
    pypi_resolver.fetch_package_metadata("requests")

    # Different systems → different cache entries, no cross-contamination
    assert client._cache.metadata_cache[("NPM", "requests")] == npm_data
    assert client._cache.metadata_cache[("PYPI", "requests")] == pypi_data
