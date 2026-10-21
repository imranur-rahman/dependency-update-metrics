"""Tests for result reuse (72 hours) and Job retention (30 days), with a controllable clock."""

import sqlite3
from datetime import date

import pytest
from fastapi.testclient import TestClient

from contract_examples import load_example, with_job_id
from dmhost import JobQueue, Worker, create_app

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
