"""The Host runs the real Tool with deps.dev data, from submit to result.

A deps.dev client stub serves this scenario (the same as in ``tests/test_api.py``):
  mypkg 1.0.0 (2021-01-01) and 1.1.0 (2022-01-01) both declare depA "<2.0".
  depA 1.0.0 (2020-06-01) and 2.0.0 (2021-06-01). VULN-A affects depA < 2.0.0 and is
  fixed in 2.0.0. depA is outdated and unremediated from 2021-06-01 to 2023-01-01,
  which is 214 + 365 = 579 days.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pandas as pd
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
