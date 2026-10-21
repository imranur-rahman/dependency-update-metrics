"""Tests for the public ``analyze()`` function (the Tool seam that the Host uses).

All data comes from a fixture resolver that is injected with ``resolver=``,
so the tests run offline.

Scenario (PyPI), the same as in ``test_metamorphic.py``:
  mypkg 1.0.0 (2021-01-01) and 1.1.0 (2022-01-01) both declare depA "<2.0".
  depA 1.0.0 (2020-06-01) and 2.0.0 (2021-06-01). The constraint always selects
  1.0.0, so depA is outdated from 2021-06-01 on.
  VULN-A affects depA < 2.0.0 and is fixed in 2.0.0 (published 2021-06-01),
  so depA is also unremediated from 2021-06-01 on.
"""

import json
import socket
from datetime import datetime, timezone
from typing import Dict, Optional
from unittest.mock import MagicMock

import pandas as pd
import pytest
import requests

from dependency_metrics import analyze
from dependency_metrics.depsdev_client import DepsDevClient
from dependency_metrics.errors import DataSourceError, PackageNotFoundError
from dependency_metrics.models import PackageVersion
from dependency_metrics.resolvers import ResolverCache


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    def _blocked(*args, **kwargs):
        raise RuntimeError("network access is not allowed in api tests")

    monkeypatch.setattr(socket.socket, "connect", _blocked)


def _utc(year: int, month: int, day: int) -> datetime:
    return datetime(year, month, day, tzinfo=timezone.utc)


PKG_RELEASES = {"1.0.0": _utc(2021, 1, 1), "1.1.0": _utc(2022, 1, 1)}
DEP_RELEASES = {"1.0.0": _utc(2020, 6, 1), "2.0.0": _utc(2021, 6, 1)}
VULN_A = {
    "vul_id": "VULN-A",
    "ecosystem": "PYPI",
    "package": "depA",
    "vul_introduced": "0",
    "vul_fixed": "2.0.0",
    "severity": "High",
}


class FixtureResolver:
    """A PackageResolver that serves the scenario above from memory."""

    ecosystem = "pypi"

    def __init__(self, pkg_releases=None, dep_deps=None, fail_on: Optional[str] = None):
        self.start_date = _utc(2020, 1, 1)
        self.end_date = _utc(2023, 1, 1)
        self._releases = {
            "mypkg": PKG_RELEASES if pkg_releases is None else pkg_releases,
            "depA": DEP_RELEASES,
        }
        self._deps = {"depA": "<2.0"} if dep_deps is None else dep_deps
        self._fail_on = fail_on

    def fetch_package_metadata(self, package_name: str) -> Dict:
        if package_name == self._fail_on:
            raise ValueError(f"Package not found: {package_name}")
        releases = self._releases[package_name]
        return {
            "_name": package_name,
            "releases": {
                v: [{"upload_time": t.strftime("%Y-%m-%dT%H:%M:%S")}] for v, t in releases.items()
            },
        }

    def get_package_version_at_date(self, metadata: Dict):
        releases = self._releases[metadata["_name"]]
        candidates = [(t, v) for v, t in releases.items() if t <= self.end_date]
        version = max(candidates)[1]
        return version, {"_version": version}

    def get_all_versions_with_dates(self, metadata: Dict, package_name: Optional[str] = None):
        name = package_name or metadata["_name"]
        return [PackageVersion(name, v, t) for v, t in self._releases[name].items()]

    def resolve_dependency_version(self, dependency, constraint, before_date):
        return "1.0.0"

    def get_highest_semver_version_at_date(self, package_name, at_date, metadata=None):
        released = [(t, v) for v, t in self._releases[package_name].items() if t <= at_date]
        return max(released)[1] if released else None

    def extract_dependencies(self, version_data: Dict) -> Dict[str, str]:
        return dict(self._deps)

    def get_version_dependencies(self, package: str, version: str) -> Dict[str, str]:
        return dict(self._deps)


def _analyze(tmp_path, **kwargs):
    args = dict(
        ecosystem="pypi",
        package="mypkg",
        start_date=_utc(2020, 1, 1),
        end_date=_utc(2023, 1, 1),
        output_dir=tmp_path,
        resolver=FixtureResolver(),
        osv_df=pd.DataFrame([VULN_A]),
    )
    args.update(kwargs)
    return analyze(**args)


def test_window_summary_returns_mttu_mttr_and_dependency_count(tmp_path):
    # Hand computation: the analysis version is 1.1.0. depA is outdated and
    # unremediated from 2021-06-01 to 2023-01-01 = 214 + 365 = 579 days.
    result = _analyze(tmp_path, mode="window_summary")

    assert result.summary.mttu == pytest.approx(579.0)
    assert result.summary.mttr == pytest.approx(579.0)
    assert result.summary.num_dependencies == 1
    assert result.summary.status == "ok"
    assert result.summary.error == ""


def test_per_release_returns_one_row_per_release_with_version_and_date(tmp_path):
    # At 1.0.0 (2021-01-01) depA is up to date: 0 days. At 1.1.0 (2022-01-01)
    # depA is outdated from 2021-06-01: 214 days (the test_metamorphic.py value).
    result = _analyze(tmp_path, mode="per_release")

    assert result.summary is None
    assert [(r.package_version, r.package_release_date) for r in result.releases] == [
        ("1.0.0", "2021-01-01"),
        ("1.1.0", "2022-01-01"),
    ]
    first, second = result.releases
    assert (first.mttu, first.mttr) == (pytest.approx(0.0), pytest.approx(0.0))
    assert (second.mttu, second.mttr) == (pytest.approx(214.0), pytest.approx(214.0))
    assert all(
        r.num_dependencies == 1 and r.status == "ok" and r.error == "" for r in result.releases
    )


def test_per_release_with_no_release_in_window_returns_no_rows(tmp_path):
    result = _analyze(
        tmp_path, mode="per_release", start_date=_utc(2023, 6, 1), end_date=_utc(2024, 1, 1)
    )

    assert result.releases == []


def test_window_summary_to_dict_is_plain_json(tmp_path):
    result = _analyze(tmp_path, mode="window_summary")

    data = json.loads(json.dumps(result.to_dict()))

    assert data["ecosystem"] == "pypi"
    assert data["package"] == "mypkg"
    assert data["mode"] == "window_summary"
    assert data["start_date"] == "2020-01-01"
    assert data["end_date"] == "2023-01-01"
    assert data["weighting_type"] == "disable"
    assert data["half_life"] is None
    assert data["summary"] == {
        "mttu": pytest.approx(579.0),
        "mttr": pytest.approx(579.0),
        "num_dependencies": 1,
        "status": "ok",
        "error": "",
    }
    assert data["releases"] == []
    assert data["error"] == ""


def test_window_summary_failure_gives_error_status_not_exception(tmp_path):
    result = _analyze(tmp_path, resolver=FixtureResolver(fail_on="mypkg"))

    assert result.summary.status == "error"
    assert "Package not found: mypkg" in result.summary.error
    assert "Package not found: mypkg" in result.error
    assert result.to_dict()["summary"]["status"] == "error"


def test_per_release_failure_gives_error_and_no_rows(tmp_path):
    result = _analyze(tmp_path, mode="per_release", resolver=FixtureResolver(fail_on="mypkg"))

    assert result.releases == []
    assert "Package not found: mypkg" in result.error


def test_window_summary_progress_reports_each_dependency(tmp_path):
    two_deps = FixtureResolver(dep_deps={"depA": "<2.0", "depB": "<2.0"})
    two_deps._releases["depB"] = DEP_RELEASES
    events = []

    result = _analyze(tmp_path, resolver=two_deps, progress=events.append)

    assert [(e.stage, e.done, e.total) for e in events] == [
        ("dependencies", 1, 2),
        ("dependencies", 2, 2),
    ]
    assert sorted(e.item for e in events) == ["depA", "depB"]
    assert result.summary.num_dependencies == 2


def test_per_release_progress_reports_each_release(tmp_path):
    events = []

    _analyze(tmp_path, mode="per_release", progress=events.append)

    assert [(e.stage, e.done, e.total, e.item) for e in events] == [
        ("releases", 1, 2, "1.0.0"),
        ("releases", 2, 2, "1.1.0"),
    ]


def test_progress_callback_does_not_change_the_result(tmp_path):
    without = _analyze(tmp_path, mode="per_release").to_dict()
    with_cb = _analyze(tmp_path, mode="per_release", progress=lambda e: None).to_dict()

    assert with_cb == without


def _http_error(status):
    response = MagicMock(spec=requests.Response)
    response.status_code = status
    return requests.HTTPError(f"{status} Error", response=response)


def _depsdev_client(package_errors=None, requirement_errors=None):
    """A DepsDevClient stub that serves the same scenario in deps.dev's response shape.

    ``package_errors`` maps a package name to the exception of its GetPackage call, and
    ``requirement_errors`` maps a version of mypkg to the exception of its GetRequirements.
    """
    package_errors = package_errors or {}
    requirement_errors = requirement_errors or {}

    def _iso(t):
        return t.strftime("%Y-%m-%dT%H:%M:%SZ")

    def _get_package(system, name, end_date=None):
        if name in package_errors:
            raise package_errors[name]
        releases = {"mypkg": PKG_RELEASES, "depA": DEP_RELEASES}[name]
        return {
            "packageKey": {"system": system, "name": name},
            "versions": [
                {
                    "versionKey": {"system": system, "name": name, "version": v},
                    "publishedAt": _iso(t),
                }
                for v, t in releases.items()
            ],
        }

    def _get_requirements(system, name, version):
        if name == "mypkg" and version in requirement_errors:
            raise requirement_errors[version]
        deps = [{"projectName": "depA", "versionSpecifier": "<2.0", "environmentMarker": ""}]
        return {"pypi": {"dependencies": deps if name == "mypkg" else []}}

    client = MagicMock(spec=DepsDevClient)
    client._cache = ResolverCache(cache_dir=None)
    client.get_package.side_effect = _get_package
    client.get_requirements.side_effect = _get_requirements
    return client


@pytest.mark.parametrize(
    "mode, expected",
    [("window_summary", [579.0]), ("per_release", [0.0, 214.0])],
)
def test_depsdev_data_source_gives_the_same_values(tmp_path, mode, expected):
    result = _analyze(
        tmp_path,
        mode=mode,
        resolver=None,
        data_source="depsdev",
        depsdev_client=_depsdev_client(),
    )

    rows = [result.summary] if mode == "window_summary" else result.releases
    assert [r.mttu for r in rows] == [pytest.approx(v) for v in expected]
    assert [r.mttr for r in rows] == [pytest.approx(v) for v in expected]
    assert result.data_source == "depsdev"
    assert result.to_dict()["data_source"] == "depsdev"


def test_unknown_data_source_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="data source"):
        _analyze(tmp_path, data_source="maven-central")


def test_raise_errors_lets_the_exception_through(tmp_path):
    with pytest.raises(ValueError, match="Package not found: mypkg"):
        _analyze(tmp_path, resolver=FixtureResolver(fail_on="mypkg"), raise_errors=True)


def test_per_release_to_dict_is_plain_json(tmp_path):
    result = _analyze(tmp_path, mode="per_release")

    data = json.loads(json.dumps(result.to_dict()))

    assert data["summary"] is None
    assert data["releases"][1] == {
        "package_version": "1.1.0",
        "package_release_date": "2022-01-01",
        "mttu": pytest.approx(214.0),
        "mttr": pytest.approx(214.0),
        "num_dependencies": 1,
        "status": "ok",
        "error": "",
    }


class FailingReleaseResolver(FixtureResolver):
    """The scenario above with a release 1.1.0 between the two. Its manifest cannot be read."""

    def __init__(self, failing_version: str):
        super().__init__(
            pkg_releases={
                "1.0.0": _utc(2021, 1, 1),
                "1.1.0": _utc(2021, 9, 1),
                "1.2.0": _utc(2022, 1, 1),
            }
        )
        self._failing_version = failing_version

    def get_version_dependencies(self, package: str, version: str) -> Dict[str, str]:
        if package == "mypkg" and version == self._failing_version:
            raise RuntimeError(f"Manifest of {package}@{version} is not available")
        return super().get_version_dependencies(package, version)


def test_per_release_release_that_fails_gives_an_error_row_and_the_others_stay(tmp_path):
    # 1.0.0: depA is up to date, 0 days. 1.2.0 (2022-01-01): depA is outdated and
    # unremediated from 2021-06-01, 214 days. 1.1.0 has no values, only its error.
    result = _analyze(tmp_path, mode="per_release", resolver=FailingReleaseResolver("1.1.0"))

    assert result.error == ""
    rows = [(r.package_version, r.status, r.mttu, r.mttr) for r in result.releases]
    assert rows == [
        ("1.0.0", "ok", pytest.approx(0.0), pytest.approx(0.0)),
        ("1.1.0", "error", None, None),
        ("1.2.0", "ok", pytest.approx(214.0), pytest.approx(214.0)),
    ]
    failed = result.releases[1]
    assert failed.error == "Manifest of mypkg@1.1.0 is not available"
    assert failed.package_release_date == "2021-09-01"
    assert failed.num_dependencies == 0
    assert json.loads(json.dumps(result.to_dict()))["releases"][1]["mttu"] is None


def _depsdev_analyze(tmp_path, client, **kwargs):
    return _analyze(tmp_path, resolver=None, data_source="depsdev", depsdev_client=client, **kwargs)


@pytest.mark.parametrize("mode", ["window_summary", "per_release"])
def test_depsdev_unknown_package_raises_package_not_found(tmp_path, mode):
    client = _depsdev_client(package_errors={"mypkg": _http_error(404)})

    with pytest.raises(PackageNotFoundError):
        _depsdev_analyze(tmp_path, client, mode=mode, raise_errors=True)


def test_depsdev_unknown_package_gives_an_error_result(tmp_path):
    client = _depsdev_client(package_errors={"mypkg": _http_error(404)})

    result = _depsdev_analyze(tmp_path, client)

    assert result.summary.status == "error"
    assert result.summary.mttu is None
    assert "not found" in result.error


@pytest.mark.parametrize("mode", ["window_summary", "per_release"])
def test_depsdev_failed_dependency_lookup_is_an_error_not_a_zero(tmp_path, mode):
    """A deps.dev failure for a dependency (after retries) is an error, never MTTU 0."""
    client = _depsdev_client(package_errors={"depA": _http_error(503)})

    result = _depsdev_analyze(tmp_path, client, mode=mode)

    assert "depA" in result.error
    assert result.releases == []
    if mode == "window_summary":
        assert result.summary.status == "error"
    with pytest.raises(DataSourceError):
        _depsdev_analyze(
            tmp_path,
            _depsdev_client(package_errors={"depA": _http_error(503)}),
            mode=mode,
            raise_errors=True,
        )


def test_depsdev_failed_manifest_gives_an_error_row_for_that_release(tmp_path):
    client = _depsdev_client(requirement_errors={"1.1.0": requests.ReadTimeout("timed out")})

    result = _depsdev_analyze(tmp_path, client, mode="per_release")

    statuses = {r.package_version: r.status for r in result.releases}
    assert statuses == {"1.0.0": "ok", "1.1.0": "error"}
    failed = [r for r in result.releases if r.status == "error"][0]
    assert failed.mttu is None


def test_depsdev_failed_manifest_of_the_analyzed_version_is_an_error(tmp_path):
    client = _depsdev_client(requirement_errors={"1.1.0": requests.ReadTimeout("timed out")})

    result = _depsdev_analyze(tmp_path, client)

    assert result.summary.status == "error"
    assert "GetRequirements" in result.error


def test_depsdev_failed_dependency_lookup_stops_a_bulk_row(tmp_path):
    """The bulk CLI turns this exception into an error row, which --resume retries."""
    from dependency_metrics.analyzer import DependencyAnalyzer
    from dependency_metrics.depsdev_resolver import DepsDevResolver

    client = _depsdev_client(package_errors={"depA": _http_error(503)})
    resolver = DepsDevResolver("PYPI", "mypkg", _utc(2020, 1, 1), _utc(2023, 1, 1), client)
    analyzer = DependencyAnalyzer(
        ecosystem="pypi",
        package="mypkg",
        start_date=_utc(2020, 1, 1),
        end_date=_utc(2023, 1, 1),
        output_dir=tmp_path,
        resolver=resolver,
    )
    row = {"start_date": _utc(2020, 1, 1), "end_date": _utc(2023, 1, 1), "row_num": 2}

    with pytest.raises(DataSourceError, match="depA"):
        analyzer.analyze_bulk_rows([row], osv_df=pd.DataFrame([VULN_A]))
