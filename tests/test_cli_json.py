"""CLI tests for ``--json``: one analysis result on stdout, in the Host API result shape.

The expected values come from the result bodies in ``contract/examples/``, which the Host
tests and the Webapp tests also read. The tests use the deps.dev client stub and the
scenario of ``test_api.py``, so they run offline.
"""

import json
import logging
import sys
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
import requests

from dependency_metrics.cli import main
from tests.test_api import VULN_A, _depsdev_client

CONTRACT_DIR = Path(__file__).resolve().parents[1] / "contract" / "examples"


def _contract_result(name, package="mypkg"):
    body = json.loads((CONTRACT_DIR / f"{name}.json").read_text(encoding="utf-8"))
    return next(r for r in body["results"] if r["package"] == package)


@pytest.fixture(autouse=True)
def _restore_logger():
    dm_logger = logging.getLogger("dependency_metrics")
    handlers = list(dm_logger.handlers)
    yield
    for handler in dm_logger.handlers[len(handlers) :]:
        handler.close()
    dm_logger.handlers = handlers


def _run_json(tmp_path, capsys, *extra, client=None, package="mypkg"):
    """Run the CLI with ``--json`` and return (exit code, stdout, stderr)."""
    pd.DataFrame([VULN_A]).to_parquet(tmp_path / "osv_database.parquet")
    client = client or _depsdev_client()
    argv = [
        "dependency-metrics",
        "--ecosystem",
        "pypi",
        "--package",
        package,
        "--depsdev",
        "--start-date",
        "2020-01-01",
        "--end-date",
        "2023-01-01",
        "--output-dir",
        str(tmp_path),
        "--json",
        *extra,
    ]
    code = 0
    with (
        patch("dependency_metrics.cli.DepsDevClient", return_value=client),
        patch("dependency_metrics.api.DepsDevClient", return_value=client),
        patch.object(sys, "argv", argv),
    ):
        try:
            main()
        except SystemExit as exc:
            code = exc.code
    out, err = capsys.readouterr()
    return code, out, err


def test_json_window_summary_matches_the_contract_result(
    tmp_path, capsys, in_process_executor
):
    code, out, _ = _run_json(tmp_path, capsys)

    assert code == 0
    assert json.loads(out) == _contract_result("job_done_window_summary")


def test_json_per_release_matches_the_contract_shape(tmp_path, capsys, in_process_executor):
    code, out, _ = _run_json(tmp_path, capsys, "--per-release")

    expected = _contract_result("job_done_per_release")
    expected["releases"] = [
        {
            "package_version": "1.0.0",
            "package_release_date": "2021-01-01",
            "mttu": 0.0,
            "mttr": 0.0,
            "num_dependencies": 1,
            "status": "ok",
            "error": "",
        },
        {
            "package_version": "1.1.0",
            "package_release_date": "2022-01-01",
            "mttu": 214.0,
            "mttr": 214.0,
            "num_dependencies": 1,
            "status": "ok",
            "error": "",
        },
    ]
    assert code == 0
    assert json.loads(out) == expected


def test_json_failed_release_keeps_the_other_releases(tmp_path, capsys, in_process_executor):
    client = _depsdev_client(requirement_errors={"1.1.0": requests.ReadTimeout("timed out")})

    code, out, _ = _run_json(tmp_path, capsys, "--per-release", client=client)

    releases = json.loads(out)["releases"]
    assert code == 0
    assert [r["status"] for r in releases] == ["ok", "error"]
    assert releases[1]["mttu"] is None and releases[1]["mttr"] is None
    assert releases[1]["error"]


@pytest.mark.parametrize("extra", [(), ("--per-release",)])
def test_json_error_matches_the_contract_error_result(tmp_path, capsys, in_process_executor, extra):
    client = _depsdev_client(package_errors={"mypkg": ValueError("Package not found in deps.dev")})

    code, out, err = _run_json(tmp_path, capsys, *extra, client=client)

    expected = _contract_result("job_done_with_package_error", package="nope")
    expected["package"] = "mypkg"
    if extra:
        expected["mode"] = "per_release"
        expected["summary"] = None
    assert code == 1
    assert json.loads(out) == expected
    assert "Package not found in deps.dev" in err


def test_json_package_without_dependencies_has_status_no_deps(
    tmp_path, capsys, in_process_executor
):
    code, out, _ = _run_json(tmp_path, capsys, package="depA")

    result = json.loads(out)
    assert code == 0
    assert result["package"] == "depA"
    assert result["summary"] == {
        "mttu": 0.0,
        "mttr": 0.0,
        "num_dependencies": 0,
        "status": "no_deps",
        "error": "",
    }


def test_json_sends_logs_and_stray_prints_to_stderr(tmp_path, capsys, in_process_executor):
    with patch("dependency_metrics.cli.print_summary", side_effect=lambda **_: print("noise")):
        code, out, err = _run_json(tmp_path, capsys, "--verbose")

    assert code == 0
    assert json.loads(out)["package"] == "mypkg"
    assert "noise" in err
    assert "Analyzing pypi package: mypkg" in err


@pytest.mark.parametrize(
    "extra, result_file",
    [((), "mypkg_results.json"), (("--per-release",), "mypkg_per_release_results.csv")],
)
def test_json_still_writes_the_result_files(
    tmp_path, capsys, in_process_executor, extra, result_file
):
    code, _, _ = _run_json(tmp_path, capsys, *extra)

    assert code == 0
    assert (tmp_path / result_file).exists()


def test_json_needs_package(tmp_path, capsys):
    argv = ["dependency-metrics", "--build-osv", "--json", "--output-dir", str(tmp_path)]
    with patch.object(sys, "argv", argv), pytest.raises(SystemExit) as exc:
        main()

    assert exc.value.code == 2
    assert capsys.readouterr().out == ""


def test_json_per_release_row_of_a_package_without_dependencies_has_0_dependencies(
    tmp_path, capsys, in_process_executor
):
    # depA has no dependencies, so each Per-release row has the status "no_deps" and 0
    # dependencies. The Webapp and the Extension show such a row as "has no dependencies".
    code, out, _ = _run_json(tmp_path, capsys, "--per-release", package="depA")

    result = json.loads(out)
    assert code == 0
    assert result["mode"] == "per_release"
    assert result["summary"] is None
    assert [r["package_version"] for r in result["releases"]] == ["1.0.0", "2.0.0"]
    for release in result["releases"]:
        assert (release["status"], release["num_dependencies"], release["error"]) == ("no_deps", 0, "")
