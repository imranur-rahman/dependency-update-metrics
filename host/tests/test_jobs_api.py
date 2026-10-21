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


def submit_comparison(client, **changes):
    body = {**load_example("create_job_request"), **changes}
    return client.post("/api/jobs", json=body)


def renamed_example_analysis(comparison, package):
    """Return the done example result, under the name ``package``."""
    return {**fake_analysis_from_example(comparison, "mypkg"), "package": package}


def test_comparison_of_five_packages_is_accepted(queue):
    client = make_client(queue)

    response = submit_comparison(client, packages=["a", "b", "c", "d", "e"])

    assert response.status_code == 202


def comparison_with(**fields):
    body = load_example("create_job_request")
    body.update(fields)
    return body


@pytest.mark.parametrize("weighting_type", ["disable", "linear", "inverse"])
def test_weighting_types_without_a_half_life_are_accepted(queue, weighting_type):
    client = make_client(queue)

    response = client.post("/api/jobs", json=comparison_with(weighting_type=weighting_type))

    assert response.status_code == 202


@pytest.mark.parametrize(
    "packages",
    [
        pytest.param([], id="no package"),
        pytest.param(["a", "b", "c", "d", "e", "f"], id="six packages"),
        pytest.param(["a", "b", "a"], id="duplicate"),
        pytest.param(["Requests", "requests"], id="duplicate in another case"),
        pytest.param(["a", "  "], id="empty name"),
    ],
)
def test_comparison_with_a_wrong_package_list_is_refused(queue, packages):
    client = make_client(queue)

    response = submit_comparison(client, packages=packages)

    assert response.status_code == 422
    assert Worker(queue, fake_analysis_from_example).run_once() is False


def test_pypi_names_that_normalize_to_one_name_are_duplicates(queue):
    client = make_client(queue)

    response = submit_comparison(
        client, ecosystem="pypi", packages=["zope.interface", "Zope_Interface"]
    )

    assert response.status_code == 422


@pytest.mark.parametrize(
    "changes",
    [
        pytest.param({"ecosystem": ["npm", "pypi"]}, id="two ecosystems"),
        pytest.param(
            {"packages": [{"ecosystem": "npm", "name": "a"}, {"ecosystem": "pypi", "name": "b"}]},
            id="one ecosystem for each package",
        ),
        pytest.param({"ecosystem": "maven"}, id="unknown ecosystem"),
    ],
)
def test_comparison_with_packages_from_more_than_one_ecosystem_is_refused(queue, changes):
    client = make_client(queue)

    response = submit_comparison(client, **changes)

    assert response.status_code == 422
    assert Worker(queue, fake_analysis_from_example).run_once() is False


def test_worker_runs_each_package_in_sequence_and_one_failure_does_not_stop_the_others(queue):
    calls = []

    def analysis(comparison, package):
        calls.append(package)
        if package == "bad":
            raise RuntimeError("deps.dev is down")
        return renamed_example_analysis(comparison, package)

    client = make_client(queue)
    job_id = submit_comparison(client, packages=["first", "bad", "last"]).json()["job_id"]

    Worker(queue, analysis).run_once()
    body = client.get(f"/api/jobs/{job_id}").json()

    assert calls == ["first", "bad", "last"]
    assert body["state"] == "done"
    assert [r["package"] for r in body["results"]] == ["first", "bad", "last"]
    assert [r["summary"]["mttu"] for r in body["results"]] == [579.0, None, 579.0]
    failed = body["results"][1]
    assert failed["error"] == f"Something went wrong (ref: {job_id})"
    assert failed["summary"]["status"] == "error"
    assert "deps.dev is down" not in str(body)


@pytest.mark.parametrize("half_life", [1, 180, 3650])
def test_exponential_weighting_accepts_a_half_life_from_1_to_3650_days(queue, half_life):
    client = make_client(queue)

    response = client.post(
        "/api/jobs", json=comparison_with(weighting_type="exponential", half_life=half_life)
    )

    assert response.status_code == 202
    job = client.get(f"/api/jobs/{response.json()['job_id']}").json()
    assert job["comparison"]["weighting_type"] == "exponential"
    assert job["comparison"]["half_life"] == half_life


@pytest.mark.parametrize("half_life", [None, 0, 0.5, 3651, -10])
def test_exponential_weighting_without_a_valid_half_life_is_refused(queue, half_life):
    client = make_client(queue)

    response = client.post(
        "/api/jobs", json=comparison_with(weighting_type="exponential", half_life=half_life)
    )

    assert response.status_code == 422
    assert Worker(queue, fake_analysis_from_example).run_once() is False


def test_unknown_weighting_type_is_refused(queue):
    client = make_client(queue)

    response = client.post("/api/jobs", json=comparison_with(weighting_type="quadratic"))

    assert response.status_code == 422


def test_half_life_is_dropped_when_the_weighting_is_not_exponential(queue):
    client = make_client(queue)

    response = client.post("/api/jobs", json=comparison_with(weighting_type="linear", half_life=90))

    job = client.get(f"/api/jobs/{response.json()['job_id']}").json()
    assert job["comparison"]["half_life"] is None
