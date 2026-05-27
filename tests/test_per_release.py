"""Tests for analyze_at_release_points and _calculate_weight_with_window."""

import math
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from dependency_metrics.analyzer import DependencyAnalyzer
from dependency_metrics.resolvers import ResolverCache


def _make_analyzer(tmp_path: Path, ecosystem: str = "pypi") -> DependencyAnalyzer:
    return DependencyAnalyzer(
        ecosystem=ecosystem,
        package="mypackage",
        start_date=datetime(2020, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2021, 1, 1, tzinfo=timezone.utc),
        weighting_type="disable",
        output_dir=tmp_path,
        resolver_cache=ResolverCache(cache_dir=tmp_path / "cache"),
    )


def _utc(year: int, month: int, day: int) -> datetime:
    return datetime(year, month, day, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# 1. One result row per release in window
# ---------------------------------------------------------------------------


def test_analyze_at_release_points_returns_one_row_per_release(tmp_path):
    analyzer = _make_analyzer(tmp_path)
    row = {
        "row_num": 1,
        "start_date": _utc(2020, 1, 1),
        "end_date": _utc(2021, 1, 1),
    }

    releases = [
        ("1.0.0", _utc(2020, 3, 1)),
        ("1.1.0", _utc(2020, 6, 1)),
        ("2.0.0", _utc(2020, 9, 1)),
    ]

    with (
        patch.object(analyzer, "fetch_package_metadata", return_value={}),
        patch.object(analyzer, "get_all_versions_with_dates", return_value=releases),
        patch.object(analyzer.resolver, "get_version_dependencies", return_value={}),
    ):
        results = analyzer.analyze_at_release_points(row, osv_df=pd.DataFrame())

    assert len(results) == len(releases)


# ---------------------------------------------------------------------------
# 2. window_end equals release date for each result
# ---------------------------------------------------------------------------


def test_window_end_equals_release_date(tmp_path):
    analyzer = _make_analyzer(tmp_path)
    row = {
        "row_num": 2,
        "start_date": _utc(2020, 1, 1),
        "end_date": _utc(2021, 1, 1),
    }

    releases = [
        ("1.0.0", _utc(2020, 4, 1)),
        ("1.1.0", _utc(2020, 8, 1)),
    ]

    with (
        patch.object(analyzer, "fetch_package_metadata", return_value={}),
        patch.object(analyzer, "get_all_versions_with_dates", return_value=releases),
        patch.object(analyzer.resolver, "get_version_dependencies", return_value={}),
    ):
        results = analyzer.analyze_at_release_points(row, osv_df=pd.DataFrame())

    release_dates = [r[1].date().isoformat() for r in releases]
    window_ends = [r["summary"]["window_end"] for r in results]
    assert window_ends == release_dates


# ---------------------------------------------------------------------------
# 3. No releases in window → empty list
# ---------------------------------------------------------------------------


def test_no_releases_returns_empty(tmp_path):
    analyzer = _make_analyzer(tmp_path)
    row = {
        "row_num": 3,
        "start_date": _utc(2020, 1, 1),
        "end_date": _utc(2021, 1, 1),
    }

    with (
        patch.object(analyzer, "fetch_package_metadata", return_value={}),
        patch.object(analyzer, "get_all_versions_with_dates", return_value=[]),
    ):
        results = analyzer.analyze_at_release_points(row, osv_df=pd.DataFrame())

    assert results == []


# ---------------------------------------------------------------------------
# 4. Package version with no deps → mttu=0, mttr=0, num_dependencies=0
# ---------------------------------------------------------------------------


def test_no_deps_returns_zero_metrics(tmp_path):
    analyzer = _make_analyzer(tmp_path)
    row = {
        "row_num": 4,
        "start_date": _utc(2020, 1, 1),
        "end_date": _utc(2021, 1, 1),
    }

    releases = [("1.0.0", _utc(2020, 6, 1))]

    with (
        patch.object(analyzer, "fetch_package_metadata", return_value={}),
        patch.object(analyzer, "get_all_versions_with_dates", return_value=releases),
        patch.object(analyzer.resolver, "get_version_dependencies", return_value={}),
    ):
        results = analyzer.analyze_at_release_points(row, osv_df=pd.DataFrame())

    assert len(results) == 1
    summary = results[0]["summary"]
    assert summary["mttu"] == 0.0
    assert summary["mttr"] == 0.0
    assert summary["num_dependencies"] == 0


# ---------------------------------------------------------------------------
# 5. _calculate_weight_with_window linear formula
# ---------------------------------------------------------------------------


def test_calculate_weight_with_window_linear(tmp_path):
    analyzer = _make_analyzer(tmp_path)
    analyzer.weighting_type = "linear"

    window_start = _utc(2020, 1, 1)
    window_end = _utc(2020, 1, 11)  # 10-day window
    # age=0 → weight 1.0
    assert analyzer._calculate_weight_with_window(0, window_start, window_end) == pytest.approx(1.0)
    # age=5 → weight 0.5
    assert analyzer._calculate_weight_with_window(5, window_start, window_end) == pytest.approx(0.5)
    # age=10 → weight 0.0
    assert analyzer._calculate_weight_with_window(10, window_start, window_end) == pytest.approx(
        0.0
    )

    # disable weighting always returns 1.0
    analyzer.weighting_type = "disable"
    assert analyzer._calculate_weight_with_window(999, window_start, window_end) == pytest.approx(
        1.0
    )

    # inverse weighting
    analyzer.weighting_type = "inverse"
    assert analyzer._calculate_weight_with_window(0, window_start, window_end) == pytest.approx(1.0)
    assert analyzer._calculate_weight_with_window(1, window_start, window_end) == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# 6. MTTU and MTTR correctness for exponential weighting (cafeteria-like scenario)
# ---------------------------------------------------------------------------


def test_mttu_mttr_exponential_two_releases(tmp_path):
    """Per-release MTTU/MTTR and dep-frame weights are correct with exponential weighting.

    Package mypkg has 2 releases in the analysis window:
      0.21.0 (2022-07-01) and 0.22.0 (2024-01-01).
    Both declare dependency depA with constraint ">=0.9.0,<1.1.0".
    depA release history: 1.0.0 (2021-06-01) and 1.1.0 (2023-01-01).
    The resolver always returns dep_version="1.0.0" (satisfies constraint).
    Highest dep version at date:
      - before 2023-01-01: "1.0.0"  → updated=True   (dep is on latest)
      - from   2023-01-01: "1.1.0"  → updated=False  (dep lags behind)

    Interval structure (built from dep_dates_cache + start/end boundaries):
      All 4 intervals in base_df_cache (start_date skipped as usual):
        i=0  2021-06-01 .. 2022-07-01  (395 d)  updated=True
        i=1  2022-07-01 .. 2023-01-01  (184 d)  updated=True
        i=2  2023-01-01 .. 2024-01-01  (365 d)  updated=False
        i=3  2024-01-01 .. 2024-03-05  ( 64 d)  updated=False

    Expected MTTU (exponential half-life=80):
      v0.21.0 — bisect k=1: only interval i=0 (updated) → MTTU=0
      v0.22.0 — bisect k=3: intervals i=0..2; only i=2 non-updated (365 d) → MTTU=365

    MTTR=0 for both releases (empty osv_df → no vulnerabilities).

    Dep-frame ages are measured from window_end (per-release), NOT from today:
      v0.21.0 ages: [395]         (window_end = 2022-07-01)
      v0.22.0 ages: [944, 549, 365] (window_end = 2024-01-01)
    """
    lam = math.log(2) / 80.0

    analyzer = DependencyAnalyzer(
        ecosystem="npm",
        package="mypkg",
        start_date=_utc(2021, 1, 1),
        end_date=_utc(2024, 3, 5),
        weighting_type="exponential",
        half_life=80.0,
        output_dir=tmp_path,
        resolver_cache=ResolverCache(cache_dir=tmp_path / "cache"),
    )

    row = {
        "row_num": 1,
        "start_date": _utc(2021, 1, 1),
        "end_date": _utc(2024, 3, 5),
    }

    pkg_releases = [("0.21.0", _utc(2022, 7, 1)), ("0.22.0", _utc(2024, 1, 1))]
    dep_releases = [("1.0.0", _utc(2021, 6, 1)), ("1.1.0", _utc(2023, 1, 1))]

    def _versions(meta, *, package_name=None):
        return pkg_releases if package_name == "mypkg" else dep_releases

    def _highest(package_name, at_date, metadata=None):
        return "1.1.0" if at_date >= _utc(2023, 1, 1) else "1.0.0"

    with (
        patch.object(analyzer, "fetch_package_metadata", return_value={}),
        patch.object(analyzer, "get_all_versions_with_dates", side_effect=_versions),
        patch.object(
            analyzer.resolver,
            "get_version_dependencies",
            return_value={"depA": ">=0.9.0,<1.1.0"},
        ),
        patch.object(analyzer, "get_highest_semver_version_at_date", side_effect=_highest),
        patch.object(analyzer, "resolve_dependency_version", return_value="1.0.0"),
    ):
        results = analyzer.analyze_at_release_points(
            row, osv_df=pd.DataFrame(), generate_dep_frames=True
        )

    assert len(results) == 2
    r21 = next(r for r in results if r["summary"]["package_version"] == "0.21.0")
    r22 = next(r for r in results if r["summary"]["package_version"] == "0.22.0")

    # --- MTTU / MTTR ---
    # v0.21.0: k=1, only interval i=0 (updated) → no non-updated intervals → MTTU=0
    assert r21["summary"]["mttu"] == pytest.approx(0.0)
    assert r21["summary"]["mttr"] == pytest.approx(0.0)

    # v0.22.0: k=3, only interval i=2 is non-updated (365 d); weight cancels → MTTU=365
    assert r22["summary"]["mttu"] == pytest.approx(365.0, rel=1e-6)
    assert r22["summary"]["mttr"] == pytest.approx(0.0)

    # --- Dep frames: ages are measured from window_end (per-release), not from today ---

    # v0.21.0: 1 frame with 1 row (k=1); age from window_end=2022-07-01
    assert len(r21["dependency_frames"]) == 1
    df21 = r21["dependency_frames"][0]
    assert len(df21) == 1
    assert bool(df21.iloc[0]["updated"]) is True
    assert df21.iloc[0]["age_of_interval"] == 395  # (2022-07-01 - 2021-06-01).days
    assert df21.iloc[0]["weight"] == pytest.approx(math.exp(-lam * 395), rel=1e-6)

    # v0.22.0: 1 frame with 3 rows (k=3); ages from window_end=2024-01-01
    #   i=0: 2024-01-01 - 2021-06-01 = 944 d
    #   i=1: 2024-01-01 - 2022-07-01 = 549 d
    #   i=2: 2024-01-01 - 2023-01-01 = 365 d  (non-updated)
    assert len(r22["dependency_frames"]) == 1
    df22 = r22["dependency_frames"][0]
    assert len(df22) == 3
    assert df22["age_of_interval"].tolist() == [944, 549, 365]
    non_updated = df22[~df22["updated"]]
    assert len(non_updated) == 1
    assert non_updated.iloc[0]["age_of_interval"] == 365
    assert non_updated.iloc[0]["weight"] == pytest.approx(math.exp(-lam * 365), rel=1e-6)
