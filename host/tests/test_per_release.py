"""Per-release Comparisons on the Host: the mode field, and the real Tool from submit to result.

A deps.dev client stub serves this scenario:
  mypkg 1.0.0 (2021-01-01), 1.1.0 (2021-09-01), and 1.2.0 (2022-01-01) declare depA "<2.0".
  deps.dev cannot give the manifest of mypkg 1.1.0, so that release fails.
  yourpkg 1.0.0 (2021-03-01) and 2.0.0 (2021-12-01) declare depA "<2.0".
  depA 1.0.0 (2020-06-01) and 2.0.0 (2021-06-01). VULN-A affects depA < 2.0.0 and is
  fixed in 2.0.0. So depA is outdated and unremediated from 2021-06-01 on.

``contract/examples/job_done_per_release.json`` is the output of this test. To make it
again after a change of the Tool or the Host, run the test with
``DM_REGENERATE_CONTRACT=1``, and then check the diff.
"""

import json
import os
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from contract_examples import CONTRACT_DIR, load_example, with_job_id
from dependency_metrics.depsdev_client import DepsDevClient
from dependency_metrics.depsdev_resolver import DepsDevResolver
from dependency_metrics.resolvers import ResolverCache
from dmhost import JobQueue, ToolAnalysis, Worker, create_app

PER_RELEASE_EXAMPLE = "job_done_per_release"


def _utc(year, month, day):
    return datetime(year, month, day, tzinfo=timezone.utc)


RELEASES = {
    "mypkg": {"1.0.0": _utc(2021, 1, 1), "1.1.0": _utc(2021, 9, 1), "1.2.0": _utc(2022, 1, 1)},
    "yourpkg": {"1.0.0": _utc(2021, 3, 1), "2.0.0": _utc(2021, 12, 1)},
    "depA": {"1.0.0": _utc(2020, 6, 1), "2.0.0": _utc(2021, 6, 1)},
}
FAILED_RELEASE = ("mypkg", "1.1.0")
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
        return {"pypi": {"dependencies": [] if name == "depA" else deps}}

    client = MagicMock(spec=DepsDevClient)
    client._cache = ResolverCache(cache_dir=None)
    client.get_package.side_effect = get_package
    client.get_requirements.side_effect = get_requirements
    return client


class ResolverWithOneMissingManifest(DepsDevResolver):
    """The deps.dev resolver, but the manifest of ``FAILED_RELEASE`` is not available.

    ``DepsDevResolver`` reads a failed manifest as "no dependencies". This resolver lets
    the failure through, so that the Tool reports the release as failed.
    """

    def get_version_dependencies(self, package, version):
        if (package, version) == FAILED_RELEASE:
            raise RuntimeError(f"deps.dev has no manifest for {package} {version}")
        return super().get_version_dependencies(package, version)


def make_client(queue, analysis):
    return TestClient(
        create_app(queue=queue, analysis=analysis, turnstile_check=lambda token, ip: True)
    )


def per_release_request():
    return {
        **load_example("create_job_request"),
        "packages": ["mypkg", "yourpkg"],
        "mode": "per_release",
    }


def test_comparison_with_an_unknown_mode_is_refused(tmp_path):
    queue = JobQueue(tmp_path / "jobs.db")
    client = make_client(queue, analysis=lambda comparison, package: {})

    response = client.post("/api/jobs", json={**per_release_request(), "mode": "timeline"})

    assert response.status_code == 422


def test_per_release_comparison_gets_one_row_for_each_release_from_the_tool(tmp_path):
    client_stub = depsdev_client_stub()
    queue = JobQueue(tmp_path / "jobs.db")
    analysis = ToolAnalysis(
        output_dir=tmp_path,
        depsdev_client=client_stub,
        osv_df=pd.DataFrame([VULN_A]),
        resolver_factory=lambda comparison, package, start, end: ResolverWithOneMissingManifest(
            system="PYPI", package=package, start_date=start, end_date=end, client=client_stub
        ),
    )
    client = make_client(queue, analysis)

    job_id = client.post("/api/jobs", json=per_release_request()).json()["job_id"]
    Worker(queue, analysis).run_once()
    body = client.get(f"/api/jobs/{job_id}").json()

    if os.environ.get("DM_REGENERATE_CONTRACT"):
        example = json.loads(json.dumps(body).replace(job_id, "example-job-id"))
        path = CONTRACT_DIR / f"{PER_RELEASE_EXAMPLE}.json"
        path.write_text(json.dumps(example, indent=2) + "\n", encoding="utf-8")
    assert body == with_job_id(load_example(PER_RELEASE_EXAMPLE), job_id)

    # Hand checks of the example, so that a regenerated file cannot hide a wrong value.
    mypkg, yourpkg = body["results"]
    rows = [(r["package_version"], r["status"], r["mttu"]) for r in mypkg["releases"]]
    # 1.0.0: depA is up to date. 1.2.0 (2022-01-01): outdated from 2021-06-01, 214 days.
    assert rows == [("1.0.0", "ok", 0.0), ("1.1.0", "error", None), ("1.2.0", "ok", 214.0)]
    assert mypkg["releases"][1]["error"] == "deps.dev has no manifest for mypkg 1.1.0"
    assert mypkg["error"] == ""
    # yourpkg 2.0.0 (2021-12-01): outdated from 2021-06-01, 183 days.
    assert [(r["package_version"], r["mttu"]) for r in yourpkg["releases"]] == [
        ("1.0.0", 0.0),
        ("2.0.0", 183.0),
    ]


@pytest.mark.parametrize("mode", ["window_summary", "per_release"])
def test_the_tool_gets_the_mode_of_the_comparison(tmp_path, mode):
    queue = JobQueue(tmp_path / "jobs.db")
    analysis = ToolAnalysis(
        output_dir=tmp_path, depsdev_client=depsdev_client_stub(), osv_df=pd.DataFrame([VULN_A])
    )
    client = make_client(queue, analysis)

    job_id = client.post("/api/jobs", json={**per_release_request(), "mode": mode}).json()["job_id"]
    Worker(queue, analysis).run_once()
    result = client.get(f"/api/jobs/{job_id}").json()["results"][1]

    assert result["mode"] == mode
    if mode == "per_release":
        assert result["summary"] is None
        assert len(result["releases"]) == 2
    else:
        assert result["summary"]["status"] == "ok"
        assert result["releases"] == []
