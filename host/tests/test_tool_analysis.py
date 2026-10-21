"""The Host runs the real Tool with deps.dev data, from submit to result.

A deps.dev client stub serves this scenario (the same as in ``tests/test_api.py``):
  mypkg 1.0.0 (2021-01-01) and 1.1.0 (2022-01-01) both declare depA "<2.0".
  depA 1.0.0 (2020-06-01) and 2.0.0 (2021-06-01). VULN-A affects depA < 2.0.0 and is
  fixed in 2.0.0. depA is outdated and unremediated from 2021-06-01 to 2023-01-01,
  which is 214 + 365 = 579 days.
"""

import logging
from datetime import datetime, timezone
from unittest.mock import MagicMock
from urllib.parse import quote

import pandas as pd
import pytest
import requests
from fastapi.testclient import TestClient

from contract_examples import load_example, with_job_id
from dependency_metrics.depsdev_client import DepsDevClient
from dependency_metrics.resolvers import ResolverCache
from dmhost import JobQueue, ToolAnalysis, Worker, create_app


def _utc(year, month, day):
    return datetime(year, month, day, tzinfo=timezone.utc)


RELEASES = {
    "mypkg": {"1.0.0": _utc(2021, 1, 1), "1.1.0": _utc(2022, 1, 1)},
    "depA": {"1.0.0": _utc(2020, 6, 1), "2.0.0": _utc(2021, 6, 1)},
}
VULN_A = {
    "vul_id": "VULN-A",
    "ecosystem": "PYPI",
    "package": "depA",
    "vul_introduced": "0",
    "vul_fixed": "2.0.0",
    "severity": "High",
}


def depsdev_client_stub():
    """A DepsDevClient stub that serves the scenario in deps.dev's response shape."""

    def get_package(system, name, end_date=None):
        return {
            "packageKey": {"system": system, "name": name},
            "versions": [
                {
                    "versionKey": {"system": system, "name": name, "version": v},
                    "publishedAt": t.strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
                for v, t in RELEASES[name].items()
            ],
        }

    def get_requirements(system, name, version):
        deps = [{"projectName": "depA", "versionSpecifier": "<2.0", "environmentMarker": ""}]
        return {"pypi": {"dependencies": deps if name == "mypkg" else []}}

    client = MagicMock(spec=DepsDevClient)
    client._cache = ResolverCache(cache_dir=None)
    client.get_package.side_effect = get_package
    client.get_requirements.side_effect = get_requirements
    return client


def test_submitted_comparison_gets_the_tool_result_from_depsdev_data(tmp_path):
    queue = JobQueue(tmp_path / "jobs.db")
    analysis = ToolAnalysis(
        output_dir=tmp_path,
        depsdev_client=depsdev_client_stub(),
        osv_df=pd.DataFrame([VULN_A]),
    )
    client = TestClient(
        create_app(queue=queue, analysis=analysis, turnstile_check=lambda token, ip: True)
    )

    job_id = client.post("/api/jobs", json=load_example("create_job_request")).json()["job_id"]
    Worker(queue, analysis).run_once()
    body = client.get(f"/api/jobs/{job_id}").json()

    assert body == with_job_id(load_example("job_done_window_summary"), job_id)
    assert body["results"][0]["data_source"] == "depsdev"


def test_the_tool_gets_the_weighting_type_and_the_half_life_of_the_comparison(tmp_path):
    queue = JobQueue(tmp_path / "jobs.db")
    analysis = ToolAnalysis(
        output_dir=tmp_path,
        depsdev_client=depsdev_client_stub(),
        osv_df=pd.DataFrame([VULN_A]),
    )
    client = TestClient(
        create_app(queue=queue, analysis=analysis, turnstile_check=lambda token, ip: True)
    )
    request = load_example("create_job_request")
    request.update(weighting_type="exponential", half_life=180)

    job_id = client.post("/api/jobs", json=request).json()["job_id"]
    Worker(queue, analysis).run_once()
    result = client.get(f"/api/jobs/{job_id}").json()["results"][0]

    assert result["weighting_type"] == "exponential"
    assert result["half_life"] == 180
    assert result["summary"]["status"] == "ok"
    # A weighted mean of days is not the sum of 579 days that "disable" gives.
    assert result["summary"]["mttu"] != 579.0


def not_found_error(system, name):
    """Return the error that ``DepsDevClient.get_package`` raises for an unknown package."""
    response = requests.Response()
    response.status_code = 404
    response.reason = "Not Found"
    response.url = f"https://api.deps.dev/v3/systems/{system}/packages/{quote(name, safe='')}"
    try:
        response.raise_for_status()
    except requests.HTTPError as error:
        return error
    raise AssertionError("raise_for_status did not raise")


def depsdev_client_stub_without(missing):
    """The scenario stub, but deps.dev does not know the package ``missing``."""
    client = depsdev_client_stub()
    known = client.get_package.side_effect

    def get_package(system, name, end_date=None):
        if name == missing:
            raise not_found_error(system, name)
        return known(system, name, end_date)

    client.get_package.side_effect = get_package
    return client


def run_comparison(tmp_path, depsdev_client, **changes):
    queue = JobQueue(tmp_path / "jobs.db")
    analysis = ToolAnalysis(
        output_dir=tmp_path, depsdev_client=depsdev_client, osv_df=pd.DataFrame([VULN_A])
    )
    client = TestClient(
        create_app(queue=queue, analysis=analysis, turnstile_check=lambda token, ip: True)
    )
    request = {**load_example("create_job_request"), **changes}
    job_id = client.post("/api/jobs", json=request).json()["job_id"]
    Worker(queue, analysis).run_once()
    return job_id, client.get(f"/api/jobs/{job_id}").json()


def test_package_that_deps_dev_does_not_know_shows_package_not_found(tmp_path, caplog):
    caplog.set_level(logging.INFO)

    job_id, body = run_comparison(
        tmp_path, depsdev_client_stub_without("nope"), packages=["mypkg", "nope"]
    )

    assert body == with_job_id(load_example("job_done_with_package_error"), job_id)
    assert body["results"][1]["error"] == "Package not found in deps.dev"
    assert body["results"][0]["summary"]["mttu"] == 579.0
    logged = [r.getMessage() for r in caplog.records if job_id in r.getMessage()]
    assert any("404 Client Error" in line and "nope" in line for line in logged)


@pytest.mark.parametrize("mode", ["window_summary", "per_release"])
def test_window_without_a_release_shows_no_releases_in_this_window(tmp_path, caplog, mode):
    caplog.set_level(logging.INFO)

    # mypkg has its first release on 2021-01-01, so this window holds no release.
    job_id, body = run_comparison(
        tmp_path,
        depsdev_client_stub(),
        start_date="2019-01-01",
        end_date="2020-01-01",
        mode=mode,
    )

    # The only package failed, so the Job itself fails with the message of the package.
    assert body["state"] == "error"
    assert body["message"] == "No releases in this window"
    assert any(job_id in r.getMessage() and "mypkg" in r.getMessage() for r in caplog.records)


def test_failed_deps_dev_request_is_not_reported_as_a_window_without_releases(tmp_path):
    # deps.dev fails the first request for mypkg and answers the next one. The analysis
    # saw no release, but mypkg has releases in the window.
    client = depsdev_client_stub()
    known = client.get_package.side_effect
    calls = []

    def flaky_get_package(system, name, end_date=None):
        calls.append(name)
        if name == "mypkg" and calls.count("mypkg") == 1:
            raise requests.ConnectionError("connection reset")
        return known(system, name, end_date)

    client.get_package.side_effect = flaky_get_package

    job_id, body = run_comparison(tmp_path, client)

    assert body["state"] == "error"
    assert body["message"] == f"Something went wrong (ref: {job_id})"


def test_unknown_package_is_known_from_the_tool_outcome_without_a_second_request(tmp_path):
    # The Tool says "package not found" itself, so the Host does not ask deps.dev again.
    client = depsdev_client_stub_without("nope")

    job_id, body = run_comparison(tmp_path, client, packages=["nope"])

    assert body["message"] == "Package not found in deps.dev"
    asked = [call.args[1] for call in client.get_package.call_args_list]
    assert asked.count("nope") == 1


@pytest.mark.parametrize("mode", ["window_summary", "per_release"])
def test_failed_dependency_lookup_shows_something_went_wrong_not_a_zero(tmp_path, mode):
    client = depsdev_client_stub()
    known = client.get_package.side_effect

    def get_package(system, name, end_date=None):
        if name == "depA":
            raise requests.ConnectionError("connection reset")
        return known(system, name, end_date)

    client.get_package.side_effect = get_package

    job_id, body = run_comparison(tmp_path, client, mode=mode)

    assert body["state"] == "error"
    assert body["message"] == f"Something went wrong (ref: {job_id})"
