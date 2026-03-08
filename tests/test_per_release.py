"""Tests for analyze_at_release_points and _calculate_weight_with_window."""

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
    assert analyzer._calculate_weight_with_window(10, window_start, window_end) == pytest.approx(0.0)

    # disable weighting always returns 1.0
    analyzer.weighting_type = "disable"
    assert analyzer._calculate_weight_with_window(999, window_start, window_end) == pytest.approx(1.0)

    # inverse weighting
    analyzer.weighting_type = "inverse"
    assert analyzer._calculate_weight_with_window(0, window_start, window_end) == pytest.approx(1.0)
    assert analyzer._calculate_weight_with_window(1, window_start, window_end) == pytest.approx(0.5)
