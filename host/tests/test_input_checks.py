"""The Host checks the package names and the dates of a Comparison before the queue.

A request that fails a check gets ``422`` and no Job enters the queue.
"""

from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from contract_examples import load_example
from dmhost import JobQueue, Worker, create_app


def analysis_that_must_not_run(comparison, package):
    raise AssertionError("A refused Comparison must not reach the worker")


@pytest.fixture
def queue(tmp_path):
    return JobQueue(tmp_path / "jobs.db")


@pytest.fixture
def client(queue):
    app = create_app(
        queue=queue, analysis=analysis_that_must_not_run, turnstile_check=lambda t, ip: True
    )
    return TestClient(app)


def submit(client, **changes):
    return client.post("/api/jobs", json={**load_example("create_job_request"), **changes})


@pytest.mark.parametrize(
    "ecosystem, name",
    [
        ("npm", "express"),
        ("npm", "@babel/core"),
        ("npm", "lodash.merge"),
        ("npm", "JSONStream"),
        ("pypi", "requests"),
        ("pypi", "zope.interface"),
        ("pypi", "Flask_SQLAlchemy"),
        ("pypi", "a"),
        ("cargo", "serde"),
        ("cargo", "tokio-util"),
        ("cargo", "winapi_x86"),
    ],
)
def test_package_names_that_match_the_rules_of_the_ecosystem_are_accepted(client, ecosystem, name):
    response = submit(client, ecosystem=ecosystem, packages=[name])

    assert response.status_code == 202


@pytest.mark.parametrize(
    "ecosystem, name",
    [
        pytest.param("npm", "has space", id="npm space"),
        pytest.param("npm", ".hidden", id="npm leading dot"),
        pytest.param("npm", "_private", id="npm leading underscore"),
        pytest.param("npm", "@scope", id="npm scope without name"),
        pytest.param("npm", "a/b", id="npm slash without scope"),
        pytest.param("npm", "x" * 215, id="npm longer than 214"),
        pytest.param("pypi", "-requests", id="pypi leading dash"),
        pytest.param("pypi", "requests.", id="pypi trailing dot"),
        pytest.param("pypi", "re quests", id="pypi space"),
        pytest.param("pypi", "@babel/core", id="pypi npm scope"),
        pytest.param("cargo", "1serde", id="cargo leading digit"),
        pytest.param("cargo", "serde.json", id="cargo dot"),
        pytest.param("cargo", "x" * 65, id="cargo longer than 64"),
    ],
)
def test_package_names_that_break_the_rules_of_the_ecosystem_are_refused(
    client, queue, ecosystem, name
):
    response = submit(client, ecosystem=ecosystem, packages=[name])

    assert response.status_code == 422
    assert Worker(queue, analysis_that_must_not_run).run_once() is False


def utc_today():
    return datetime.now(timezone.utc).date()


@pytest.mark.parametrize(
    "start_date, end_date",
    [
        pytest.param("2020-01-01", "2020-01-02", id="one day"),
        pytest.param("1990-01-01", "2023-01-01", id="long window"),
        pytest.param("2020-01-01", utc_today().isoformat(), id="end date is today"),
    ],
)
def test_window_whose_end_date_is_after_the_start_date_is_accepted(client, start_date, end_date):
    response = submit(client, start_date=start_date, end_date=end_date)

    assert response.status_code == 202


@pytest.mark.parametrize(
    "start_date, end_date",
    [
        pytest.param("2023-01-01", "2020-01-01", id="end before start"),
        pytest.param("2023-01-01", "2023-01-01", id="end on start"),
        pytest.param(
            "2020-01-01", (utc_today() + timedelta(days=2)).isoformat(), id="end in the future"
        ),
    ],
)
def test_window_with_a_wrong_end_date_is_refused(client, queue, start_date, end_date):
    response = submit(client, start_date=start_date, end_date=end_date)

    assert response.status_code == 422
    assert Worker(queue, analysis_that_must_not_run).run_once() is False
