"""Integration tests for single-package CLI runs (``--package``) with deps.dev data.

The CLI and ``analyze()`` share one code path, so these tests check that the
CLI output files keep their content. They use the deps.dev client stub and the
scenario of ``test_api.py``, so they run offline.
"""

import json
import logging
import sys
from contextlib import contextmanager
from unittest.mock import patch

import pandas as pd
import pytest

from dependency_metrics.cli import main
import requests

from tests.test_api import VULN_A, _depsdev_client


@pytest.fixture(autouse=True)
def _restore_logger():
    dm_logger = logging.getLogger("dependency_metrics")
    handlers = list(dm_logger.handlers)
    yield
    for handler in dm_logger.handlers[len(handlers) :]:
        handler.close()
    dm_logger.handlers = handlers


@contextmanager
def _stub_depsdev(client=None):
    client = client or _depsdev_client()
    with (
        patch("dependency_metrics.cli.DepsDevClient", return_value=client),
        patch("dependency_metrics.api.DepsDevClient", return_value=client),
    ):
        yield


def _run_cli(tmp_path, *extra, client=None):
    pd.DataFrame([VULN_A]).to_parquet(tmp_path / "osv_database.parquet")
    argv = [
        "dependency-metrics",
        "--ecosystem",
        "pypi",
        "--package",
        "mypkg",
        "--depsdev",
        "--start-date",
        "2020-01-01",
        "--end-date",
        "2023-01-01",
        "--output-dir",
        str(tmp_path),
        *extra,
    ]
    with _stub_depsdev(client), patch.object(sys, "argv", argv):
        main()


def test_cli_window_summary_writes_results_json(tmp_path):
    _run_cli(tmp_path)

    data = json.loads((tmp_path / "mypkg_results.json").read_text())
    assert data["package"] == "mypkg"
    assert data["version"] == "1.1.0"
    assert data["ttu"] == pytest.approx(579.0)
    assert data["ttr"] == pytest.approx(579.0)
    assert data["num_dependencies"] == 1
    assert "depA" in data["dependency_data"]


def test_cli_per_release_writes_results_csv(tmp_path):
    _run_cli(tmp_path, "--per-release")

    df = pd.read_csv(tmp_path / "mypkg_per_release_results.csv", dtype={"package_version": str})
    assert list(df.columns) == [
        "ecosystem",
        "package_name",
        "package_version",
        "package_release_date",
        "window_start",
        "window_end",
        "mttu",
        "mttr",
        "num_dependencies",
        "status",
        "error",
    ]
    assert list(df["package_version"]) == ["1.0.0", "1.1.0"]
    assert list(df["mttu"]) == [pytest.approx(0.0), pytest.approx(214.0)]
    assert list(df["mttr"]) == [pytest.approx(0.0), pytest.approx(214.0)]


def test_cli_analysis_error_exits_with_status_1(tmp_path):
    client = _depsdev_client()
    client.get_package.side_effect = ValueError("Package not found in deps.dev")
    argv = [
        "dependency-metrics",
        "--ecosystem",
        "pypi",
        "--package",
        "mypkg",
        "--depsdev",
        "--output-dir",
        str(tmp_path),
    ]
    with (
        patch("dependency_metrics.cli.DepsDevClient", return_value=client),
        patch("dependency_metrics.api.DepsDevClient", return_value=client),
        patch.object(sys, "argv", argv),
        pytest.raises(SystemExit) as exc,
    ):
        main()

    assert exc.value.code == 1


def test_cli_per_release_log_shows_a_failed_release_as_error(tmp_path, caplog, capsys):
    """A failed release has no MTTU or MTTR (None). The summary log must not fail on it."""
    caplog.set_level(logging.INFO, logger="dependency_metrics")
    client = _depsdev_client(requirement_errors={"1.1.0": requests.ReadTimeout("timed out")})

    _run_cli(tmp_path, "--per-release", client=client)

    assert "Logging error" not in capsys.readouterr().err
    lines = [r.getMessage() for r in caplog.records if "v1.1.0" in r.getMessage()]
    assert len(lines) == 1
    assert "MTTU=error" in lines[0] and "MTTR=error" in lines[0]
    assert "status=error" in lines[0]
