"""Tests for result reuse (72 hours) and Job retention (30 days), with a controllable clock."""

import sqlite3
from datetime import date

import pytest
from fastapi.testclient import TestClient

from contract_examples import load_example, with_job_id
from dmhost import JobQueue, Worker, create_app
from dmhost.errors import AnalysisTimedOut, NoReleasesInWindow, PackageNotFound

HOUR = 3600.0
DAY = 24 * HOUR
OSV_DATE = date(2026, 10, 1)


class FakeClock:
    """A clock that moves only when the test calls ``advance``."""

    def __init__(self, start=1_800_000_000.0):
        self.now = start

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def fake_analysis(comparison, package):
    results = load_example("job_done_window_summary")["results"]
    return {**results[0], "package": package}


class OsvIndex:
    """A fake OSV index whose build date the test can change."""

    def __init__(self, built_on=OSV_DATE):
        self.built_on = built_on

    def __call__(self):
        return self.built_on


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def osv_index():
    return OsvIndex()


@pytest.fixture
def queue(tmp_path, clock):
    return JobQueue(tmp_path / "jobs.db", clock=clock)


@pytest.fixture
def client(queue, osv_index):
    app = create_app(
        queue=queue,
        analysis=fake_analysis,
        turnstile_check=lambda token, remote_ip: True,
        osv_index_date=osv_index,
    )
    return TestClient(app)


def submit(client, **changes):
    return client.post("/api/jobs", json={**load_example("create_job_request"), **changes})


def run_a_done_job(client, queue, **changes):
    response = submit(client, **changes)
    assert response.status_code == 202
    assert Worker(queue, fake_analysis).run_once() is True
    return response.json()["job_id"]


def test_matching_comparison_within_72_hours_returns_the_stored_result(client, queue, clock):
    job_id = run_a_done_job(client, queue)
    clock.advance(71 * HOUR)

    response = submit(client)

    assert response.status_code == 200
    assert response.json() == with_job_id(load_example("job_done_window_summary"), job_id)
    assert Worker(queue, fake_analysis).run_once() is False


@pytest.mark.parametrize(
    "changes",
    [
        pytest.param({"ecosystem": "npm"}, id="ecosystem"),
        pytest.param({"packages": ["mypkg", "otherpkg"]}, id="package set"),
        pytest.param({"start_date": "2020-01-02"}, id="start date"),
        pytest.param({"end_date": "2022-12-31"}, id="end date"),
        pytest.param({"mode": "per_release"}, id="mode"),
        pytest.param({"weighting_type": "linear"}, id="weighting type"),
    ],
)
def test_comparison_that_differs_in_a_key_field_starts_a_new_job(client, queue, changes):
    run_a_done_job(client, queue)

    response = submit(client, **changes)

    assert response.status_code == 202
    assert Worker(queue, fake_analysis).run_once() is True


def test_comparison_with_another_half_life_starts_a_new_job(client, queue):
    run_a_done_job(client, queue, weighting_type="exponential", half_life=30)

    same = submit(client, weighting_type="exponential", half_life=30)
    other = submit(client, weighting_type="exponential", half_life=60)

    assert same.status_code == 200
    assert other.status_code == 202


def test_rebuilt_osv_index_stops_the_reuse_of_older_results(client, queue, osv_index):
    run_a_done_job(client, queue)
    osv_index.built_on = date(2026, 10, 8)

    response = submit(client)

    assert response.status_code == 202


def test_package_names_that_differ_only_in_order_case_or_separators_match(client, queue):
    job_id = run_a_done_job(client, queue, packages=["zope.interface", "Requests"])

    response = submit(client, packages=["requests", "Zope_Interface"])

    assert response.status_code == 200
    assert response.json()["job_id"] == job_id


def test_stored_result_older_than_72_hours_is_not_reused(client, queue, clock):
    run_a_done_job(client, queue)
    clock.advance(72 * HOUR + 1)

    response = submit(client)

    assert response.status_code == 202


def test_72_hours_count_from_the_end_of_the_job(client, queue, clock):
    submit(client)
    clock.advance(10 * HOUR)
    Worker(queue, fake_analysis).run_once()
    clock.advance(70 * HOUR)

    assert submit(client).status_code == 200


def test_job_that_did_not_finish_as_done_is_not_reused(client, queue):
    def broken_analysis(comparison, package):
        raise RuntimeError("deps.dev is down")

    submit(client)
    Worker(queue, broken_analysis).run_once()

    assert submit(client).status_code == 202


def analysis_where_otherpkg(failure):
    """A fake analysis in which the package ``otherpkg`` raises ``failure``."""

    def analysis(comparison, package):
        if package == "otherpkg":
            raise failure
        return fake_analysis(comparison, package)

    return analysis


def run_a_done_job_with(client, queue, analysis, **changes):
    response = submit(client, **changes)
    assert response.status_code == 202
    assert Worker(queue, analysis).run_once() is True
    assert client.get(f"/api/jobs/{response.json()['job_id']}").json()["state"] == "done"


@pytest.mark.parametrize(
    "failure",
    [RuntimeError("deps.dev is down"), AnalysisTimedOut("900 s")],
    ids=["something_went_wrong", "took_too_long"],
)
def test_done_job_with_a_package_that_failed_on_the_server_is_not_reused(client, queue, failure):
    packages = ["mypkg", "otherpkg"]
    run_a_done_job_with(client, queue, analysis_where_otherpkg(failure), packages=packages)

    assert submit(client, packages=packages).status_code == 202


@pytest.mark.parametrize(
    "failure",
    [PackageNotFound("404"), NoReleasesInWindow("none")],
    ids=["package_not_found", "no_releases"],
)
def test_done_job_with_a_package_error_of_the_user_input_is_reused(client, queue, failure):
    packages = ["mypkg", "otherpkg"]
    run_a_done_job_with(client, queue, analysis_where_otherpkg(failure), packages=packages)

    assert submit(client, packages=packages).status_code == 200


def test_done_job_with_a_failed_release_is_not_reused(client, queue):
    def analysis_with_a_failed_release(comparison, package):
        release = {
            "package_version": "1.1.0",
            "package_release_date": "2022-01-01",
            "mttu": None,
            "mttr": None,
            "num_dependencies": 0,
            "status": "error",
            "error": "deps.dev GetRequirements failed",
        }
        return {**fake_analysis(comparison, package), "summary": None, "releases": [release]}

    run_a_done_job_with(client, queue, analysis_with_a_failed_release)

    assert submit(client).status_code == 202


def test_reuse_skips_a_newer_job_with_a_server_error_and_takes_an_older_clean_one(
    client, queue, clock
):
    packages = ["mypkg", "otherpkg"]
    clean_job = run_a_done_job(client, queue, packages=packages)
    clock.advance(HOUR)
    # A second Job with the same key (for example, one that two users submitted at once)
    # ends later, with a server error for one package.
    queue.add(queue.get(clean_job).comparison, reuse_key=queue_reuse_key(queue, clean_job))
    Worker(queue, analysis_where_otherpkg(RuntimeError("down"))).run_once()

    response = submit(client, packages=packages)

    assert response.status_code == 200
    assert response.json()["job_id"] == clean_job


def queue_reuse_key(queue, job_id):
    with sqlite3.connect(queue.db_path) as conn:
        return conn.execute("SELECT reuse_key FROM jobs WHERE job_id = ?", (job_id,)).fetchone()[0]


def test_cancelled_job_is_not_reused(client, queue):
    job_id = submit(client).json()["job_id"]
    assert client.delete(f"/api/jobs/{job_id}").status_code == 200

    assert submit(client).status_code == 202


def test_job_records_older_than_30_days_are_deleted(client, queue, clock):
    old_job = run_a_done_job(client, queue)
    clock.advance(29 * DAY)
    recent_job = run_a_done_job(client, queue, packages=["otherpkg"])
    clock.advance(1 * DAY + 1)

    submit(client, packages=["thirdpkg"])

    assert client.get(f"/api/jobs/{old_job}").status_code == 404
    assert client.get(f"/api/jobs/{recent_job}").status_code == 200


def test_queue_file_from_before_reuse_still_reuses_results(tmp_path):
    db = tmp_path / "jobs.db"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "CREATE TABLE jobs (job_id TEXT PRIMARY KEY, state TEXT NOT NULL, "
            "comparison TEXT NOT NULL, results TEXT NOT NULL DEFAULT '[]', "
            "message TEXT NOT NULL DEFAULT '', created_at REAL NOT NULL, started_at REAL, "
            "finished_at REAL)"
        )
    conn.close()
    queue = JobQueue(db)
    app = create_app(queue=queue, analysis=fake_analysis, turnstile_check=lambda t, ip: True)
    client = TestClient(app)

    run_a_done_job(client, queue)

    assert submit(client).status_code == 200
