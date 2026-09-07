"""Tests for native crates.io resolver support."""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from dependency_metrics.analyzer import DependencyAnalyzer
from dependency_metrics.resolvers import CratesResolver, ResolverCache


def _utc(year: int, month: int, day: int) -> datetime:
    return datetime(year, month, day, tzinfo=timezone.utc)


def _make_resolver(tmp_path: Path) -> CratesResolver:
    return CratesResolver(
        package="mycrate",
        start_date=_utc(2020, 1, 1),
        end_date=_utc(2024, 1, 1),
        registry_urls={"cargo": "https://crates.io"},
        cache=ResolverCache(cache_dir=tmp_path / "cache"),
    )


def _metadata() -> dict:
    return {
        "crate": {"id": "mycrate"},
        "versions": [
            {"num": "0.9.0", "created_at": "2019-01-01T00:00:00Z", "yanked": False},
            {"num": "1.0.0", "created_at": "2020-01-01T00:00:00Z", "yanked": False},
            {"num": "1.1.0", "created_at": "2021-01-01T00:00:00Z", "yanked": False},
            {"num": "2.0.0", "created_at": "2025-01-01T00:00:00Z", "yanked": False},
            {"num": "3.0.0", "created_at": "2022-01-01T00:00:00Z", "yanked": True},
        ],
    }


def test_crates_resolver_selects_version_at_end_date(tmp_path: Path) -> None:
    resolver = _make_resolver(tmp_path)

    version, version_data = resolver.get_package_version_at_date(_metadata())

    assert version == "1.1.0"
    assert version_data == {"_package": "mycrate", "_version": "1.1.0"}


def test_crates_resolver_lists_versions_in_window_and_skips_yanked(tmp_path: Path) -> None:
    resolver = _make_resolver(tmp_path)

    versions = list(resolver.get_all_versions_with_dates(_metadata(), package_name="mycrate"))

    assert [v.version for v in versions] == ["1.0.0", "1.1.0"]


def test_crates_resolver_parses_normal_non_optional_dependencies(tmp_path: Path) -> None:
    resolver = _make_resolver(tmp_path)
    data = {
        "dependencies": [
            {"crate_id": "libc", "req": "^0.2", "kind": "normal", "optional": False},
            {"crate_id": "serde", "req": "^1.0", "kind": "normal", "optional": True},
            {"crate_id": "cc", "req": "^1.0", "kind": "build", "optional": False},
            {"crate_id": "tokio-test", "req": "^0.4", "kind": "dev", "optional": False},
        ]
    }

    assert resolver._parse_dependencies(data) == {"libc": "^0.2"}


def test_crates_resolver_resolves_cargo_constraint_before_date(tmp_path: Path) -> None:
    resolver = _make_resolver(tmp_path)
    dep_metadata = {
        "crate": {"id": "dep"},
        "versions": [
            {"num": "1.0.0", "created_at": "2020-01-01T00:00:00Z", "yanked": False},
            {"num": "1.5.0", "created_at": "2021-01-01T00:00:00Z", "yanked": False},
            {"num": "2.0.0", "created_at": "2022-01-01T00:00:00Z", "yanked": False},
        ],
    }

    with patch.object(resolver, "fetch_package_metadata", return_value=dep_metadata):
        result = resolver.resolve_dependency_version("dep", "^1.0.0", _utc(2021, 6, 1))

    assert result == "1.5.0"


def test_crates_resolver_accepts_comma_separated_range(tmp_path: Path) -> None:
    resolver = _make_resolver(tmp_path)

    result = resolver._match_cargo_constraint(["1.0.0", "1.5.0", "2.0.0"], ">=1.0.0, <2.0.0")

    assert result == "1.5.0"


def test_dependency_analyzer_constructs_native_cargo_resolver(tmp_path: Path) -> None:
    analyzer = DependencyAnalyzer(
        ecosystem="crates.io",
        package="mycrate",
        start_date=_utc(2020, 1, 1),
        end_date=_utc(2024, 1, 1),
        output_dir=tmp_path,
    )

    assert analyzer.ecosystem == "cargo"
    assert isinstance(analyzer.resolver, CratesResolver)


# ---------------------------------------------------------------------------
# fetch_package_metadata — disk-cache staleness
# ---------------------------------------------------------------------------


def _mock_crates_http_response(data: dict):
    from unittest.mock import MagicMock

    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json.return_value = data
    resp.__enter__ = MagicMock(return_value=resp)
    resp.__exit__ = MagicMock(return_value=False)
    return resp


def test_fetch_package_metadata_refetches_when_end_date_exceeds_cached_versions(
    tmp_path: Path,
) -> None:
    """A cached crates.io listing whose newest known release predates end_date
    might be missing later releases published since — it must be refreshed."""
    resolver = _make_resolver(tmp_path)
    resolver.end_date = _utc(2026, 1, 1)
    stale = {
        "crate": {"id": "mycrate"},
        "versions": [{"num": "1.0.0", "created_at": "2020-01-01T00:00:00Z", "yanked": False}],
    }
    resolver.cache.save_json("metadata", "cargo:mycrate", stale)
    fresh = _metadata()

    with patch.object(
        resolver.cache, "get", return_value=_mock_crates_http_response(fresh)
    ) as mock_get:
        result = resolver.fetch_package_metadata("mycrate")
        mock_get.assert_called_once()

    assert result == fresh
    assert resolver.cache.load_json("metadata", "cargo:mycrate") == fresh


def test_fetch_package_metadata_disk_cache_hit_when_end_date_already_covered(
    tmp_path: Path,
) -> None:
    resolver = _make_resolver(tmp_path)
    resolver.end_date = _utc(2019, 6, 1)
    cached = {
        "crate": {"id": "mycrate"},
        "versions": [{"num": "1.0.0", "created_at": "2020-01-01T00:00:00Z", "yanked": False}],
    }
    resolver.cache.save_json("metadata", "cargo:mycrate", cached)

    with patch.object(resolver.cache, "get") as mock_get:
        result = resolver.fetch_package_metadata("mycrate")
        mock_get.assert_not_called()

    assert result == cached
