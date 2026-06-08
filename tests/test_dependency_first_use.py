"""Tests that dependency interval frames are bounded by the first release that
actually DECLARED the dependency — not by the package's first release ever, nor
by the dependency's own first release.

Mirrors the real npm/import-meta-resolve scenario that motivated this fix:
0.1.0 (the package's first-ever release) declared zero production dependencies;
1.0.0 was the first release to declare `builtins`. Interval frames for
`builtins` must start at 1.0.0's release date — not 0.1.0's (package exists but
hasn't adopted the dep yet) and not `builtins`' own first release (predates the
package entirely).

Covers the shared `_first_use_dates` helper plus its two single-dependency
consumers, `analyze_dependency` and `analyze_bulk_rows`
(`analyze_at_release_points` is covered separately in test_per_release.py).
"""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from dependency_metrics.analyzer import DependencyAnalyzer
from dependency_metrics.resolvers import ResolverCache


def _utc(year: int, month: int, day: int) -> datetime:
    return datetime(year, month, day, tzinfo=timezone.utc)


def _make_analyzer(tmp_path: Path) -> DependencyAnalyzer:
    return DependencyAnalyzer(
        ecosystem="npm",
        package="mypkg",
        start_date=_utc(2019, 1, 1),
        end_date=_utc(2024, 1, 1),
        weighting_type="disable",
        output_dir=tmp_path,
        resolver_cache=ResolverCache(cache_dir=tmp_path / "cache"),
    )


# Shared scenario: mirrors import-meta-resolve@0.1.0 (no prod deps) -> @1.0.0
# (introduces depA, mirroring `builtins`). depA itself pre-dates mypkg entirely.
PKG_RELEASES = [
    ("0.1.0", _utc(2020, 9, 1)),  # declares nothing
    ("1.0.0", _utc(2021, 5, 14)),  # first release to declare depA
    ("1.1.0", _utc(2022, 1, 1)),
]
DEP_RELEASES = [
    ("4.0.0", _utc(2014, 2, 11)),  # pre-dates mypkg entirely
    ("4.1.0", _utc(2023, 1, 1)),
]


def _versions(meta, *, package_name=None):
    return PKG_RELEASES if package_name == "mypkg" else DEP_RELEASES


def _deps(package, ver):
    return {} if ver == "0.1.0" else {"depA": "^4.0.0"}


# ---------------------------------------------------------------------------
# 1. _first_use_dates — the shared helper
# ---------------------------------------------------------------------------


def test_first_use_dates_skips_versions_that_dont_declare_dep(tmp_path):
    analyzer = _make_analyzer(tmp_path)

    with patch.object(analyzer.resolver, "get_version_dependencies", side_effect=_deps):
        first_use = analyzer._first_use_dates(PKG_RELEASES, ["depA"])

    # First USE is 1.0.0 (2021-05-14) — not 0.1.0 (package's first release, no
    # prod deps) and not depA's own first release (2014-02-11, predates mypkg).
    assert first_use == {"depA": _utc(2021, 5, 14)}


def test_first_use_dates_short_circuits_once_all_found(tmp_path):
    analyzer = _make_analyzer(tmp_path)
    seen_versions = []

    def _multi_deps(package, ver):
        seen_versions.append(ver)
        return {"depA": "^1.0.0", "depB": "^2.0.0"}

    with patch.object(analyzer.resolver, "get_version_dependencies", side_effect=_multi_deps):
        first_use = analyzer._first_use_dates(PKG_RELEASES, ["depA", "depB"])

    assert first_use == {"depA": _utc(2020, 9, 1), "depB": _utc(2020, 9, 1)}
    # Both deps found at the very first version — must not inspect later ones.
    assert seen_versions == ["0.1.0"]


def test_first_use_dates_missing_dep_omitted(tmp_path):
    analyzer = _make_analyzer(tmp_path)

    with patch.object(analyzer.resolver, "get_version_dependencies", side_effect=_deps):
        first_use = analyzer._first_use_dates(PKG_RELEASES, ["depA", "never-declared"])

    assert first_use == {"depA": _utc(2021, 5, 14)}
    assert "never-declared" not in first_use


# ---------------------------------------------------------------------------
# 2. analyze_dependency — single-dependency analysis
# ---------------------------------------------------------------------------


def test_analyze_dependency_bounds_intervals_by_first_use(tmp_path):
    analyzer = _make_analyzer(tmp_path)

    with (
        patch.object(analyzer, "get_all_versions_with_dates", side_effect=_versions),
        patch.object(analyzer.resolver, "get_version_dependencies", side_effect=_deps),
        patch.object(analyzer.resolver, "get_highest_semver_version_at_date", return_value="4.1.0"),
        patch.object(analyzer, "resolve_dependency_version", return_value="4.0.0"),
    ):
        df = analyzer.analyze_dependency(
            "depA", pkg_metadata={}, dep_metadata={}, osv_df=pd.DataFrame()
        )

    assert not df.empty
    earliest_start = df["interval_start"].min()
    assert earliest_start == _utc(2021, 5, 14)
    # Not bound by mypkg's own first release (no prod deps yet)...
    assert earliest_start != _utc(2020, 9, 1)
    # ...nor by depA's own first release (predates mypkg entirely).
    assert earliest_start != _utc(2014, 2, 11)


# ---------------------------------------------------------------------------
# 3. analyze_bulk_rows — bulk multi-row analysis (constant "latest deps" snapshot)
# ---------------------------------------------------------------------------


def test_analyze_bulk_rows_bounds_intervals_by_first_use(tmp_path):
    analyzer = _make_analyzer(tmp_path)

    row = {
        "row_num": 1,
        "start_date": _utc(2019, 1, 1),
        "end_date": _utc(2024, 1, 1),
    }

    with (
        patch.object(analyzer, "fetch_package_metadata", return_value={}),
        patch.object(analyzer, "_get_latest_package_version_data", return_value=("1.1.0", {})),
        patch.object(analyzer, "extract_dependencies", return_value={"depA": "^4.0.0"}),
        patch.object(analyzer, "get_all_versions_with_dates", side_effect=_versions),
        patch.object(analyzer.resolver, "get_version_dependencies", side_effect=_deps),
        patch.object(analyzer.resolver, "get_highest_semver_version_at_date", return_value="4.1.0"),
        patch.object(analyzer, "resolve_dependency_version", return_value="4.0.0"),
        patch.object(analyzer, "_check_remediation", return_value=True),
    ):
        results = analyzer.analyze_bulk_rows([row], osv_df=pd.DataFrame())

    assert len(results) == 1
    dep_frames = results[0]["dependency_frames"]
    assert len(dep_frames) == 1
    earliest_start = dep_frames[0]["interval_start"].min()

    # The latest version's manifest declares depA (the constant snapshot this
    # mode uses), but it must still only be tracked from the release that first
    # introduced it — not mypkg's first-ever release nor depA's own history.
    assert earliest_start == _utc(2021, 5, 14)
    assert earliest_start != _utc(2020, 9, 1)
    assert earliest_start != _utc(2014, 2, 11)
