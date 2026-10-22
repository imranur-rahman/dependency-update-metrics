"""Tests for ``GET /api/health``, the endpoint that the uptime monitor checks."""

import shutil

from fastapi.testclient import TestClient

from dmhost import JobQueue, create_app


def make_client(queue):
    app = create_app(
        queue=queue,
        analysis=lambda comparison, package: {},
        turnstile_check=lambda token, remote_ip: True,
    )
    return TestClient(app)


def test_health_is_ok_when_the_job_queue_answers(tmp_path):
    client = make_client(JobQueue(tmp_path / "jobs.db"))

    response = client.get("/api/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_health_is_unavailable_and_shows_no_detail_when_the_job_queue_fails(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    client = make_client(JobQueue(data_dir / "jobs.db"))
    # The volume of the queue file is gone, so SQLite cannot open the file.
    shutil.rmtree(data_dir)

    response = client.get("/api/health")

    assert response.status_code == 503
    # The URL is public, so the body names no path, error text, or version.
    assert response.json() == {"status": "unavailable"}
