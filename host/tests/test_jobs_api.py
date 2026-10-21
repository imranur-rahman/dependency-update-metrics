"""Tests for the Job API of the Host: ``POST /api/jobs`` and ``GET /api/jobs/{id}``."""

import pytest
from fastapi.testclient import TestClient

from contract_examples import load_example, with_job_id
from dmhost import JobQueue, Worker, create_app


def fake_analysis_from_example(comparison, package):
    """Return the result of the done example for ``package``."""
    results = load_example("job_done_window_summary")["results"]
    return next(r for r in results if r["package"] == package)


def accept_all_tokens(token, remote_ip):
    return True


@pytest.fixture
def queue(tmp_path):
    return JobQueue(tmp_path / "jobs.db")


def make_client(queue, analysis=fake_analysis_from_example, turnstile_check=accept_all_tokens):
    app = create_app(queue=queue, analysis=analysis, turnstile_check=turnstile_check)
    return TestClient(app)


def submit(client):
    response = client.post("/api/jobs", json=load_example("create_job_request"))
    assert response.status_code == 202
    return response.json()["job_id"]


def test_submitted_job_waits_in_the_queue(queue):
    client = make_client(queue)

    job_id = submit(client)
    response = client.get(f"/api/jobs/{job_id}")

    assert response.status_code == 200
    assert response.json() == with_job_id(load_example("job_queued"), job_id)


def test_worker_runs_the_job_and_the_result_is_done(queue):
    client = make_client(queue)
    job_id = submit(client)

    assert Worker(queue, fake_analysis_from_example).run_once() is True
    response = client.get(f"/api/jobs/{job_id}")

    assert response.status_code == 200
    assert response.json() == with_job_id(load_example("job_done_window_summary"), job_id)


def test_running_job_shows_which_package_the_worker_analyzes(queue):
    client = make_client(queue)
    request = load_example("create_job_request")
    request["packages"] = ["mypkg", "otherpkg"]
    job_id = client.post("/api/jobs", json=request).json()["job_id"]
    seen = {}

    def analysis_that_reads_the_job(comparison, package):
        seen[package] = client.get(f"/api/jobs/{job_id}").json()
        return {"package": package}

    Worker(queue, analysis_that_reads_the_job).run_once()

    assert seen["otherpkg"] == with_job_id(load_example("job_running"), job_id)
    assert (seen["mypkg"]["package_index"], seen["mypkg"]["package_count"]) == (1, 2)


def test_job_whose_analysis_raises_shows_a_message_with_the_job_id(queue):
    def broken_analysis(comparison, package):
        raise RuntimeError("deps.dev is down")

    client = make_client(queue)
    job_id = submit(client)

    Worker(queue, broken_analysis).run_once()
    body = client.get(f"/api/jobs/{job_id}").json()

    assert body == with_job_id(load_example("job_error"), job_id)
    assert body["message"] == f"Something went wrong (ref: {job_id})"
    assert "deps.dev is down" not in str(body)


def test_submit_with_a_token_that_fails_the_turnstile_check_is_refused(queue):
    seen_tokens = []

    def reject_all_tokens(token, remote_ip):
        seen_tokens.append(token)
        return False

    client = make_client(queue, turnstile_check=reject_all_tokens)

    response = client.post("/api/jobs", json=load_example("create_job_request"))

    assert response.status_code == 403
    assert seen_tokens == ["example-token"]
    assert Worker(queue, fake_analysis_from_example).run_once() is False


def test_browser_requests_from_the_webapp_origin_are_allowed(queue):
    app = create_app(
        queue=queue,
        analysis=fake_analysis_from_example,
        turnstile_check=accept_all_tokens,
        allowed_origins=["http://localhost:8000"],
    )
    client = TestClient(app)

    allowed = client.get("/api/jobs/x", headers={"Origin": "http://localhost:8000"})
    other = client.get("/api/jobs/x", headers={"Origin": "https://example.com"})

    assert allowed.headers.get("access-control-allow-origin") == "http://localhost:8000"
    assert "access-control-allow-origin" not in other.headers


def test_unknown_job_id_returns_404(queue):
    assert make_client(queue).get("/api/jobs/no-such-job").status_code == 404


def test_worker_with_an_empty_queue_does_nothing(queue):
    assert Worker(queue, fake_analysis_from_example).run_once() is False
