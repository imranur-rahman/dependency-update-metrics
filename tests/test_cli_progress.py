"""Tests for progress counter and resume improvements in dependency_metrics/cli.py."""

import logging
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from dependency_metrics.cli import main

# ---------------------------------------------------------------------------
# Shared helpers (mirrors test_cli_resume.py)
# ---------------------------------------------------------------------------

_LEDGER_HEADER = "ecosystem,package_name,window_start,window_end,status\n"


def _pr_result(
    ecosystem="npm",
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


def _fake_osv_df():
    return pd.DataFrame(columns=["package", "ecosystem", "severity"])


@pytest.fixture(autouse=True)
def cleanup_dm_logger():
    """Remove any handlers added to 'dependency_metrics' logger between tests."""
    yield
    dm_logger = logging.getLogger("dependency_metrics")
    dm_logger.handlers.clear()


# ---------------------------------------------------------------------------
# Per-release resume: package-level skipping (bug fix regression tests)
# ---------------------------------------------------------------------------


def test_per_release_resume_skips_all_rows_for_completed_package(tmp_path: Path):
    """All 3 rows for the same package should be skipped when it's in the ledger,
    regardless of their individual end_dates."""
    input_csv = tmp_path / "input.csv"
    input_csv.write_text(
        "ecosystem,package_name,end_date\n"
        "npm,lodash,2021-01-01\n"
        "npm,lodash,2022-01-01\n"
        "npm,lodash,2023-01-01\n"
    )
    # Ledger uses the max window_end — only the last row would have matched the old key
    ledger_path = tmp_path / "input_per_release_completed.csv"
    ledger_path.write_text(_LEDGER_HEADER + "npm,lodash,1900-01-01,2023-01-01,ok\n")

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

        with pytest.raises(SystemExit) as exc_info:
            main()

    assert exc_info.value.code == 0
    mock_inst.analyze_at_release_points.assert_not_called()


def test_per_release_resume_package_level_not_row_level(tmp_path: Path):
    """Regression: old code matched at row level (end_date), so only the row whose
    end_date == ledger window_end was skipped.  Now all rows for the package are
    skipped and only the other package is analysed."""
    input_csv = tmp_path / "input.csv"
    input_csv.write_text(
        "ecosystem,package_name,end_date\n"
        "npm,lodash,2021-01-01\n"  # different end_date from ledger — old bug: NOT skipped
        "npm,lodash,2022-01-01\n"  # same end_date as ledger — old: skipped
        "npm,express,2022-01-01\n"  # different package — should be analysed
    )
    ledger_path = tmp_path / "input_per_release_completed.csv"
    ledger_path.write_text(_LEDGER_HEADER + "npm,lodash,1900-01-01,2022-01-01,ok\n")

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
            _pr_result("npm", "express", window_start="1900-01-01", window_end="2022-01-01")
        ]

        main()

    # Only express should be analysed — lodash's two rows must both be skipped
    assert mock_inst.analyze_at_release_points.call_count == 1


def test_per_release_resume_different_packages_partially_done(tmp_path: Path):
    """One package done, one not done → only the undone package is analysed."""
    input_csv = tmp_path / "input.csv"
    input_csv.write_text(
        "ecosystem,package_name,end_date\n" "npm,lodash,2022-01-01\n" "npm,express,2022-01-01\n"
    )
    ledger_path = tmp_path / "input_per_release_completed.csv"
    ledger_path.write_text(_LEDGER_HEADER + "npm,lodash,1900-01-01,2022-01-01,ok\n")

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
            _pr_result("npm", "express", window_start="1900-01-01", window_end="2022-01-01")
        ]

        main()

    assert mock_inst.analyze_at_release_points.call_count == 1


# ---------------------------------------------------------------------------
# Log file creation
# ---------------------------------------------------------------------------


def test_log_file_created_when_specified(tmp_path: Path):
    input_csv = tmp_path / "input.csv"
    input_csv.write_text("ecosystem,package_name,end_date\npypi,requests,2024-01-01\n")
    log_file = tmp_path / "run.log"

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
                "--log-file",
                str(log_file),
            ],
        ),
    ):
        mock_osv.return_value.build_database.return_value = _fake_osv_df()
        mock_inst = mock_analyzer_cls.return_value
        mock_inst.analyze_bulk_rows.return_value = [_bulk_result()]

        main()

    assert log_file.exists()
    assert log_file.stat().st_size > 0


def test_log_file_default_location(tmp_path: Path):
    """Without --log-file, run.log is created inside the output dir."""
    input_csv = tmp_path / "input.csv"
    input_csv.write_text("ecosystem,package_name,end_date\npypi,requests,2024-01-01\n")

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
            ],
        ),
    ):
        mock_osv.return_value.build_database.return_value = _fake_osv_df()
        mock_analyzer_cls.return_value.analyze_bulk_rows.return_value = [_bulk_result()]

        main()

    assert (tmp_path / "run.log").exists()
