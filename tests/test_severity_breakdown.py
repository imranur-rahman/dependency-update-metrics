"""Tests for --severity-breakdown feature.

All tests are fully stubbed — no network calls.
"""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from dependency_metrics.osv_builder import _normalize_severity
from dependency_metrics.osv_service import OSVService, SEVERITY_LEVELS
from dependency_metrics.analyzer import DependencyAnalyzer
from dependency_metrics.resolvers import ResolverCache

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utc(year: int, month: int, day: int) -> datetime:
    return datetime(year, month, day, tzinfo=timezone.utc)


def _make_analyzer(
    tmp_path: Path,
    ecosystem: str = "pypi",
    severity_breakdown: bool = False,
) -> DependencyAnalyzer:
    return DependencyAnalyzer(
        ecosystem=ecosystem,
        package="mypackage",
        start_date=_utc(2020, 1, 1),
        end_date=_utc(2021, 1, 1),
        weighting_type="disable",
        output_dir=tmp_path,
        resolver_cache=ResolverCache(cache_dir=tmp_path / "cache"),
        severity_breakdown=severity_breakdown,
    )


def _make_osv_service() -> OSVService:
    return OSVService()


# ---------------------------------------------------------------------------
# 1. _normalize_severity maps correctly
# ---------------------------------------------------------------------------


def test_severity_normalization():
    assert _normalize_severity("CRITICAL") == "Critical"
    assert _normalize_severity("critical") == "Critical"  # case-insensitive
    assert _normalize_severity("HIGH") == "High"
    assert _normalize_severity("high") == "High"
    assert _normalize_severity("MODERATE") == "Medium"
    assert _normalize_severity("MEDIUM") == "Medium"
    assert _normalize_severity("moderate") == "Medium"
    assert _normalize_severity("LOW") == "Low"
    assert _normalize_severity("low") == "Low"
    assert _normalize_severity("") == "None"
    assert _normalize_severity("UNKNOWN") == "None"
    assert _normalize_severity("INFO") == "None"


# ---------------------------------------------------------------------------
# 2. is_remediated_by_severity — dep with no OSV entries → all True
# ---------------------------------------------------------------------------


def test_is_remediated_by_severity_no_vulns():
    svc = _make_osv_service()
    result = svc.is_remediated_by_severity(
        dependency="requests",
        dependency_version="2.28.0",
        interval_start=_utc(2022, 1, 1),
        osv_df=pd.DataFrame(),
        dependency_metadata={},
        ecosystem="pypi",
        osv_index={},  # empty index → no vulns
    )
    assert result["Critical"] is True
    assert result["High"] is True
    assert result["Medium"] is True
    assert result["Low"] is True
    assert result["all_severities"] is True


# ---------------------------------------------------------------------------
# 3. is_remediated_by_severity — Critical unpatched vuln → Critical=False
# ---------------------------------------------------------------------------


def test_is_remediated_by_severity_critical_unpatched():
    svc = _make_osv_service()

    # The vuln affects 2.0.0 <= version < 3.0.0.
    # The fix version "3.0.0" was released on 2021-01-01 (in metadata).
    # interval_start is 2022-01-01 → fix IS available before interval_start
    # → condition: fixed_date <= interval_start → True → package is NOT remediated.
    osv_index = {
        "requests": [
            {
                "vul_id": "CVE-TEST-001",
                "vul_introduced": "2.0.0",
                "vul_fixed": "3.0.0",
                "severity": "Critical",
            }
        ]
    }

    # PyPI metadata: "3.0.0" release was on 2021-01-01
    dep_metadata = {
        "releases": {
            "3.0.0": [{"upload_time": "2021-01-01T00:00:00"}],
        }
    }

    # dep_version 2.28.0 is in [2.0.0, 3.0.0) → affected
    # fixed_date = 2021-01-01, interval_start = 2022-01-01 → fixed_date <= interval_start → NOT remediated
    result = svc.is_remediated_by_severity(
        dependency="requests",
        dependency_version="2.28.0",
        interval_start=_utc(2022, 1, 1),
        osv_df=pd.DataFrame(),
        dependency_metadata=dep_metadata,
        ecosystem="pypi",
        osv_index=osv_index,
    )
    assert result["Critical"] is False
    assert result["all_severities"] is False
    # Other severities unaffected
    assert result["High"] is True
    assert result["Medium"] is True
    assert result["Low"] is True


# ---------------------------------------------------------------------------
# 4. is_remediated_by_severity — High vuln only → High=False, others True
# ---------------------------------------------------------------------------


def test_is_remediated_by_severity_strict_buckets():
    svc = _make_osv_service()

    osv_index = {
        "flask": [
            {
                "vul_id": "CVE-TEST-002",
                "vul_introduced": "1.0.0",
                "vul_fixed": "2.0.0",
                "severity": "High",
            }
        ]
    }

    # PyPI metadata: "2.0.0" release was on 2021-01-01
    dep_metadata = {
        "releases": {
            "2.0.0": [{"upload_time": "2021-01-01T00:00:00"}],
        }
    }

    # dep_version 1.5.0 is in [1.0.0, 2.0.0) → affected by High vuln
    # fixed_date = 2021-01-01, interval_start = 2022-06-01 → fixed_date <= interval_start → NOT remediated
    result = svc.is_remediated_by_severity(
        dependency="flask",
        dependency_version="1.5.0",
        interval_start=_utc(2022, 6, 1),
        osv_df=pd.DataFrame(),
        dependency_metadata=dep_metadata,
        ecosystem="pypi",
        osv_index=osv_index,
    )
    assert result["High"] is False
    assert result["all_severities"] is False
    assert result["Critical"] is True
    assert result["Medium"] is True
    assert result["Low"] is True


# ---------------------------------------------------------------------------
# 5. DependencyAnalyzer with severity_breakdown=True, no Critical vuln →
#    mttr_critical == 0.0 in summary
# ---------------------------------------------------------------------------


def test_no_vuln_for_severity_zero_mttr(tmp_path):
    analyzer = _make_analyzer(tmp_path, severity_breakdown=True)

    pkg_releases = [("1.0.0", _utc(2020, 1, 1))]
    dep_releases = [("0.1.0", _utc(2020, 1, 1))]

    row = {
        "row_num": 1,
        "start_date": _utc(2020, 1, 1),
        "end_date": _utc(2020, 6, 1),
    }

    # No vulns → all severity buckets are True → not_remediated is empty → mttr == 0.0
    all_true = {
        "Critical": True,
        "High": True,
        "Medium": True,
        "Low": True,
        "all_severities": True,
    }

    with (
        patch.object(analyzer, "fetch_package_metadata", return_value={}),
        patch.object(analyzer, "_get_latest_package_version_data", return_value=("1.0.0", {})),
        patch.object(
            analyzer,
            "get_all_versions_with_dates",
            side_effect=lambda meta, package_name=None: (
                pkg_releases if package_name == "mypackage" else dep_releases
            ),
        ),
        patch.object(
            analyzer.resolver, "get_version_dependencies", return_value={"dep-a": ">=0.1.0"}
        ),
        patch.object(analyzer, "_check_remediation_by_severity", return_value=all_true),
        patch.object(analyzer, "get_highest_semver_version_at_date", return_value="0.1.0"),
    ):
        results = analyzer.analyze_bulk_rows([row], osv_df=pd.DataFrame())

    assert len(results) == 1
    summary = results[0]["summary"]
    assert summary["mttr_critical"] == 0.0


# ---------------------------------------------------------------------------
# 6. analyze_bulk_rows with severity_breakdown=True → summary has correct keys
# ---------------------------------------------------------------------------


def test_severity_breakdown_summary_keys(tmp_path):
    analyzer = _make_analyzer(tmp_path, severity_breakdown=True)

    pkg_releases = [("1.0.0", _utc(2020, 1, 1))]
    dep_releases = [("0.1.0", _utc(2020, 1, 1))]

    row = {
        "row_num": 1,
        "start_date": _utc(2020, 1, 1),
        "end_date": _utc(2020, 6, 1),
    }

    all_true = {
        "Critical": True,
        "High": True,
        "Medium": True,
        "Low": True,
        "all_severities": True,
    }

    with (
        patch.object(analyzer, "fetch_package_metadata", return_value={}),
        patch.object(analyzer, "_get_latest_package_version_data", return_value=("1.0.0", {})),
        patch.object(
            analyzer,
            "get_all_versions_with_dates",
            side_effect=lambda meta, package_name=None: (
                pkg_releases if package_name == "mypackage" else dep_releases
            ),
        ),
        patch.object(
            analyzer.resolver, "get_version_dependencies", return_value={"dep-a": ">=0.1.0"}
        ),
        patch.object(analyzer, "_check_remediation_by_severity", return_value=all_true),
        patch.object(analyzer, "get_highest_semver_version_at_date", return_value="0.1.0"),
    ):
        results = analyzer.analyze_bulk_rows([row], osv_df=pd.DataFrame())

    assert len(results) == 1
    summary = results[0]["summary"]
    assert "mttr_critical" in summary
    assert "mttr_high" in summary
    assert "mttr_medium" in summary
    assert "mttr_low" in summary
    assert "mttr_all_severities" in summary
    assert "mttr" not in summary


# ---------------------------------------------------------------------------
# 7. analyze_at_release_points with severity_breakdown=True → correct keys
# ---------------------------------------------------------------------------


def test_severity_breakdown_with_per_release(tmp_path):
    analyzer = _make_analyzer(tmp_path, severity_breakdown=True)

    releases = [("1.0.0", _utc(2020, 6, 1))]
    dep_releases = [("0.1.0", _utc(2020, 1, 1))]

    row = {
        "row_num": 1,
        "start_date": _utc(2020, 1, 1),
        "end_date": _utc(2021, 1, 1),
    }

    all_true = {
        "Critical": True,
        "High": True,
        "Medium": True,
        "Low": True,
        "all_severities": True,
    }

    with (
        patch.object(analyzer, "fetch_package_metadata", return_value={}),
        patch.object(
            analyzer,
            "get_all_versions_with_dates",
            side_effect=lambda meta, package_name=None: (
                releases if package_name == "mypackage" else dep_releases
            ),
        ),
        patch.object(
            analyzer.resolver,
            "get_version_dependencies",
            return_value={"dep-a": ">=0.1.0"},
        ),
        patch.object(analyzer, "_check_remediation_by_severity", return_value=all_true),
        patch.object(analyzer, "get_highest_semver_version_at_date", return_value="0.1.0"),
    ):
        results = analyzer.analyze_at_release_points(row, osv_df=pd.DataFrame())

    assert len(results) == 1
    summary = results[0]["summary"]
    assert "package_version" in summary
    assert summary["package_version"] == "1.0.0"
    assert "mttr_critical" in summary
    assert "mttr_high" in summary
    assert "mttr_medium" in summary
    assert "mttr_low" in summary
    assert "mttr_all_severities" in summary
    assert "mttr" not in summary
