"""Tests for bulk and per-release resume logic in cli.main()."""

from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from dependency_metrics.cli import main

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BULK_SUMMARY_HEADER = (
    "ecosystem,package_name,start_date,end_date,mttu,mttr,num_dependencies,status,error\n"
)
_PR_SUMMARY_HEADER = (
    "ecosystem,package_name,package_version,package_release_date,"
    "window_start,window_end,mttu,mttr,num_dependencies,status,error\n"
)
_LEDGER_HEADER = "ecosystem,package_name,window_start,window_end,status\n"


def _bulk_result(ecosystem="pypi", package_name="pkg", row_num=2):
    return {
        "summary": {
            "ecosystem": ecosystem,
            "package_name": package_name,
            "start_date": "1900-01-01",
            "end_date": "2024-01-01",
            "mttu": 0.0,
            "mttr": 0.0,
            "num_dependencies": 0,
            "status": "ok",
            "error": "",
        },
        "dependency_frames": [],
        "row_num": row_num,
    }


def _pr_result(
    ecosystem="pypi",
    package_name="pkg",
    version="1.0.0",
    window_start="1900-01-01",
    window_end="2024-01-01",
    row_num=2,
):
    return {
        "summary": {
            "ecosystem": ecosystem,
            "package_name": package_name,
            "package_version": version,
            "package_release_date": "2020-01-01",
            "window_start": window_start,
            "window_end": window_end,
            "mttu": 0.0,
            "mttr": 0.0,
            "num_dependencies": 0,
            "status": "ok",
            "error": "",
        },
        "dependency_frames": [],
        "row_num": row_num,
    }


def _fake_osv_df():
    return pd.DataFrame(columns=["package", "ecosystem", "severity"])


# ---------------------------------------------------------------------------
# Bulk resume tests
# ---------------------------------------------------------------------------


def test_bulk_resume_skips_ok_rows(tmp_path: Path) -> None:
    input_csv = tmp_path / "input.csv"
    input_csv.write_text(
        "ecosystem,package_name,end_date\n" "pypi,requests,2024-01-01\n" "pypi,flask,2024-01-01\n"
    )
    summary = tmp_path / "input_bulk_results.csv"
    summary.write_text(_BULK_SUMMARY_HEADER + "pypi,requests,1900-01-01,2024-01-01,0.0,0.0,0,ok,\n")

    with (
        patch("dependency_metrics.cli.OSVBuilder") as mock_osv,
        patch("dependency_metrics.cli.DependencyAnalyzer") as mock_analyzer_cls,
        patch(
            "sys.argv",
            ["cli", "--input-csv", str(input_csv), "--output-dir", str(tmp_path), "--resume"],
        ),
    ):
        mock_osv.return_value.build_database.return_value = _fake_osv_df()
        mock_inst = mock_analyzer_cls.return_value
        mock_inst.analyze_bulk_rows.return_value = [_bulk_result("pypi", "flask")]

        main()

    assert mock_inst.analyze_bulk_rows.call_count == 1


def test_bulk_resume_retries_error_rows(tmp_path: Path) -> None:
    input_csv = tmp_path / "input.csv"
    input_csv.write_text(
        "ecosystem,package_name,end_date\n" "pypi,requests,2024-01-01\n" "pypi,flask,2024-01-01\n"
    )
    summary = tmp_path / "input_bulk_results.csv"
    summary.write_text(
        _BULK_SUMMARY_HEADER + "pypi,requests,1900-01-01,2024-01-01,0.0,0.0,0,error,some error\n"
    )

    with (
        patch("dependency_metrics.cli.OSVBuilder") as mock_osv,
        patch("dependency_metrics.cli.DependencyAnalyzer") as mock_analyzer_cls,
        patch(
            "sys.argv",
            ["cli", "--input-csv", str(input_csv), "--output-dir", str(tmp_path), "--resume"],
        ),
    ):
        mock_osv.return_value.build_database.return_value = _fake_osv_df()
        mock_inst = mock_analyzer_cls.return_value
        mock_inst.analyze_bulk_rows.return_value = [_bulk_result()]

        main()

    assert mock_inst.analyze_bulk_rows.call_count == 2


def test_bulk_resume_all_done_exits(tmp_path: Path) -> None:
    input_csv = tmp_path / "input.csv"
    input_csv.write_text(
        "ecosystem,package_name,end_date\n" "pypi,requests,2024-01-01\n" "pypi,flask,2024-01-01\n"
    )
    summary = tmp_path / "input_bulk_results.csv"
    summary.write_text(
        _BULK_SUMMARY_HEADER
        + "pypi,requests,1900-01-01,2024-01-01,0.0,0.0,0,ok,\n"
        + "pypi,flask,1900-01-01,2024-01-01,0.0,0.0,0,ok,\n"
    )

    with (
        patch("dependency_metrics.cli.OSVBuilder") as mock_osv,
        patch("dependency_metrics.cli.DependencyAnalyzer") as mock_analyzer_cls,
        patch(
            "sys.argv",
            ["cli", "--input-csv", str(input_csv), "--output-dir", str(tmp_path), "--resume"],
        ),
    ):
        mock_osv.return_value.build_database.return_value = _fake_osv_df()

        with pytest.raises(SystemExit) as exc_info:
            main()

    assert exc_info.value.code == 0


# ---------------------------------------------------------------------------
# Per-release ledger tests
# ---------------------------------------------------------------------------


def test_per_release_creates_ledger(tmp_path: Path) -> None:
    input_csv = tmp_path / "input.csv"
    input_csv.write_text("ecosystem,package_name,end_date\npypi,pkg_a,2024-01-01\n")

    with (
        patch("dependency_metrics.cli.OSVBuilder") as mock_osv,
        patch("dependency_metrics.cli.DependencyAnalyzer") as mock_analyzer_cls,
        patch(
            "sys.argv",
            [
                "cli",
                "--input-csv",
                str(input_csv),
                "--output-dir",
                str(tmp_path),
                "--per-release",
            ],
        ),
    ):
        mock_osv.return_value.build_database.return_value = _fake_osv_df()
        mock_inst = mock_analyzer_cls.return_value
        mock_inst.analyze_at_release_points.return_value = [
            _pr_result("pypi", "pkg_a", window_start="1900-01-01", window_end="2024-01-01")
        ]

        main()

    ledger_path = tmp_path / "input_per_release_completed.csv"
    assert ledger_path.exists()
    df = pd.read_csv(ledger_path)
    assert len(df) == 1
    assert df.iloc[0]["ecosystem"] == "pypi"
    assert df.iloc[0]["package_name"] == "pkg_a"


def test_per_release_resume_skips_ledger_entries(tmp_path: Path) -> None:
    input_csv = tmp_path / "input.csv"
    input_csv.write_text(
        "ecosystem,package_name,end_date\n" "pypi,pkg_a,2024-01-01\n" "pypi,pkg_b,2024-01-01\n"
    )
    # Pre-populate ledger: pkg_a is already complete
    ledger_path = tmp_path / "input_per_release_completed.csv"
    ledger_path.write_text(_LEDGER_HEADER + "pypi,pkg_a,1900-01-01,2024-01-01,ok\n")

    with (
        patch("dependency_metrics.cli.OSVBuilder") as mock_osv,
        patch("dependency_metrics.cli.DependencyAnalyzer") as mock_analyzer_cls,
        patch(
            "sys.argv",
            [
                "cli",
                "--input-csv",
                str(input_csv),
                "--output-dir",
                str(tmp_path),
                "--per-release",
                "--resume",
            ],
        ),
    ):
        mock_osv.return_value.build_database.return_value = _fake_osv_df()
        mock_inst = mock_analyzer_cls.return_value
        mock_inst.analyze_at_release_points.return_value = [
            _pr_result("pypi", "pkg_b", window_start="1900-01-01", window_end="2024-01-01")
        ]

        main()

    assert mock_inst.analyze_at_release_points.call_count == 1


def test_per_release_resume_all_done_exits(tmp_path: Path) -> None:
    input_csv = tmp_path / "input.csv"
    input_csv.write_text("ecosystem,package_name,end_date\npypi,pkg_a,2024-01-01\n")
    ledger_path = tmp_path / "input_per_release_completed.csv"
    ledger_path.write_text(_LEDGER_HEADER + "pypi,pkg_a,1900-01-01,2024-01-01,ok\n")

    with (
        patch("dependency_metrics.cli.OSVBuilder") as mock_osv,
        patch("dependency_metrics.cli.DependencyAnalyzer") as mock_analyzer_cls,
        patch(
            "sys.argv",
            [
                "cli",
                "--input-csv",
                str(input_csv),
                "--output-dir",
                str(tmp_path),
                "--per-release",
                "--resume",
            ],
        ),
    ):
        mock_osv.return_value.build_database.return_value = _fake_osv_df()

        with pytest.raises(SystemExit) as exc_info:
            main()

    assert exc_info.value.code == 0
