"""The two environment values that the container image sets (#51).

- ``DEPENDENCY_METRICS_OUTPUT_DIR`` is the default of ``--output-dir`` (``/data`` in the image).
- ``DEPENDENCY_METRICS_OSV_INDEX`` is the path of the OSV index file. The ``-osv-`` image
  keeps its index outside ``/data``, so that a mount on ``/data`` does not hide it.
"""

import json
import sys
from datetime import datetime, timezone
from unittest.mock import patch

import pandas as pd
import pytest

from dependency_metrics.cli import main
from dependency_metrics.osv_builder import OSVBuilder
from tests.test_api import VULN_A
from tests.test_cli_single_package import _stub_depsdev
from tests.test_osv import _fixture_fetcher


def _run(argv):
    with patch.object(sys, "argv", ["dependency-metrics", *argv]):
        main()


def _print_index_date(capsys, *extra):
    with pytest.raises(SystemExit) as exc_info:
        _run(["--osv-index-date", *extra])
    assert exc_info.value.code == 0
    return json.loads(capsys.readouterr().out)["osv_index_date"]


def _build_index(output_dir):
    OSVBuilder(
        output_dir,
        fetch_zip=_fixture_fetcher([]),
        clock=lambda: datetime(2026, 10, 21, tzinfo=timezone.utc),
    ).build_database()


def test_output_dir_defaults_to_the_environment_value(tmp_path, monkeypatch, capsys):
    data = tmp_path / "data"
    _build_index(data)
    monkeypatch.setenv("DEPENDENCY_METRICS_OUTPUT_DIR", str(data))
    monkeypatch.chdir(tmp_path)

    assert _print_index_date(capsys) == "2026-10-21"
    assert not (tmp_path / "output").exists()


def test_output_dir_flag_wins_over_the_environment_value(tmp_path, monkeypatch, capsys):
    _build_index(tmp_path / "data")
    monkeypatch.setenv("DEPENDENCY_METRICS_OUTPUT_DIR", str(tmp_path / "data"))

    assert _print_index_date(capsys, "--output-dir", str(tmp_path / "other")) is None


def test_osv_index_date_reads_the_index_that_the_environment_names(tmp_path, monkeypatch, capsys):
    _build_index(tmp_path / "image")
    monkeypatch.setenv(
        "DEPENDENCY_METRICS_OSV_INDEX", str(tmp_path / "image" / "osv_database.parquet")
    )

    assert _print_index_date(capsys, "--output-dir", str(tmp_path / "data")) == "2026-10-21"


@pytest.mark.parametrize(
    "extra, expected_mttr",
    [((), [579.0]), (("--per-release",), [0.0, 214.0])],
)
def test_mttr_uses_the_index_that_the_environment_names(
    tmp_path, monkeypatch, extra, expected_mttr
):
    """The index sits outside --output-dir, as in the -osv- image with a mount on /data."""
    index = tmp_path / "image" / "osv_database.parquet"
    index.parent.mkdir()
    pd.DataFrame([VULN_A]).to_parquet(index)
    monkeypatch.setenv("DEPENDENCY_METRICS_OSV_INDEX", str(index))
    data = tmp_path / "data"

    with _stub_depsdev():
        _run(
            [
                "--ecosystem", "pypi", "--package", "mypkg", "--depsdev",
                "--start-date", "2020-01-01", "--end-date", "2023-01-01",
                "--output-dir", str(data), *extra,
            ]
        )  # fmt: skip

    if extra:
        mttr = list(pd.read_csv(data / "mypkg_per_release_results.csv")["mttr"])
    else:
        mttr = [json.loads((data / "mypkg_results.json").read_text())["ttr"]]
    assert mttr == [pytest.approx(v) for v in expected_mttr]
    assert not (data / "osv_database.parquet").exists()


def test_build_osv_writes_the_index_that_the_environment_names(tmp_path, monkeypatch):
    from dependency_metrics import osv_builder

    index = tmp_path / "image" / "osv_database.parquet"
    monkeypatch.setenv("DEPENDENCY_METRICS_OSV_INDEX", str(index))
    monkeypatch.setattr(osv_builder, "download_file", _fixture_fetcher([]))

    with pytest.raises(SystemExit) as exc_info:
        _run(["--build-osv", "--output-dir", str(tmp_path / "data")])

    assert exc_info.value.code == 0
    assert OSVBuilder(tmp_path / "data").index_date() is not None
    assert sorted(p.name for p in index.parent.iterdir()) == ["osv_database.parquet"]
