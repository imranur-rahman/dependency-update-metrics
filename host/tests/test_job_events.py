"""Tests for the event stream of a Job: ``GET /api/jobs/{id}/events`` (server-sent events)."""

import json
import threading
import time

import pytest
from fastapi.testclient import TestClient

from contract_examples import load_example, with_job_id
from dmhost import EventStreamSettings, JobQueue, Worker, create_app

FAST = EventStreamSettings(poll_seconds=0.01, heartbeat_seconds=15.0, max_seconds=10.0)


def accept_all_tokens(token, remote_ip):
    return True


def slow_analysis(comparison, package):
    # Each state lasts much longer than one poll, so the stream sees every state.
    time.sleep(0.15)
    return {"package": package}


@pytest.fixture
def queue(tmp_path):
    return JobQueue(tmp_path / "jobs.db")


def make_client(queue, settings=FAST):
    app = create_app(
        queue=queue,
        analysis=slow_analysis,
        turnstile_check=accept_all_tokens,
        event_stream=settings,
    )
    return TestClient(app)


def submit(client, packages=("mypkg",)):
    request = load_example("create_job_request")
    request["packages"] = list(packages)
    response = client.post("/api/jobs", json=request)
    assert response.status_code == 202
    return response.json()["job_id"]


def parse_stream(text):
    """Return the events of a server-sent event stream as ``(event, data)`` pairs, and the
    number of comment lines (heartbeats)."""
    events, comments = [], 0
    for block in text.split("\n\n"):
        name, data = "message", None
        for line in block.split("\n"):
            if line.startswith(":"):
                comments += 1
            elif line.startswith("event: "):
                name = line.removeprefix("event: ")
            elif line.startswith("data: "):
                data = json.loads(line.removeprefix("data: "))
        if data is not None:
            events.append((name, data))
    return events, comments


def run_worker_later(queue, delay=0.3, jobs=1):
    def run():
        time.sleep(delay)
        worker = Worker(queue, slow_analysis)
        for _ in range(jobs):
            worker.run_once()

    thread = threading.Thread(target=run)
    thread.start()
    return thread


def test_stream_sends_the_states_in_order_with_position_and_package_index(queue):
    client = make_client(queue)
    submit(client)
    job_id = submit(client, packages=["mypkg", "otherpkg"])
    worker = run_worker_later(queue, jobs=2)

    response = client.get(f"/api/jobs/{job_id}/events")
    worker.join()

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    events, _ = parse_stream(response.text)
    steps = [
        (name, data.get("position"), data.get("package_index"), data.get("package_count"))
        for name, data in events
    ]
    assert steps == [
        ("queued", 2, None, None),
        ("queued", 1, None, None),
        ("running", None, 1, 2),
        ("running", None, 2, 2),
        ("done", None, None, None),
    ]
    assert all(data["state"] == name for name, data in events)


def test_each_event_holds_the_same_body_as_get(queue):
    client = make_client(queue)
    job_id = submit(client)
    Worker(queue, slow_analysis).run_once()

    events, _ = parse_stream(client.get(f"/api/jobs/{job_id}/events").text)

    assert events == [("done", client.get(f"/api/jobs/{job_id}").json())]


def test_stream_of_a_job_that_failed_ends_with_the_error_message(queue):
    def broken_analysis(comparison, package):
        raise RuntimeError("deps.dev is down")

    client = make_client(queue)
    job_id = submit(client)
    Worker(queue, broken_analysis).run_once()

    events, _ = parse_stream(client.get(f"/api/jobs/{job_id}/events").text)

    assert events == [("error", with_job_id(load_example("job_error"), job_id))]


def test_quiet_stream_sends_a_heartbeat_comment(queue):
    settings = EventStreamSettings(poll_seconds=0.01, heartbeat_seconds=0.05, max_seconds=0.3)
    client = make_client(queue, settings)
    job_id = submit(client)

    events, heartbeats = parse_stream(client.get(f"/api/jobs/{job_id}/events").text)

    assert [name for name, _ in events] == ["queued"]
    assert heartbeats >= 3


def test_default_heartbeat_is_every_15_seconds():
    assert EventStreamSettings().heartbeat_seconds == 15.0


def test_client_that_reconnects_gets_the_current_state_first(queue):
    settings = EventStreamSettings(poll_seconds=0.01, heartbeat_seconds=15.0, max_seconds=0.1)
    client = make_client(queue, settings)
    job_id = submit(client, packages=["mypkg", "otherpkg"])

    first, _ = parse_stream(client.get(f"/api/jobs/{job_id}/events").text)
    queue.claim_next()
    queue.set_progress(job_id, 2, 2)
    second, _ = parse_stream(client.get(f"/api/jobs/{job_id}/events").text)

    assert [name for name, _ in first] == ["queued"]
    assert second[0] == ("running", with_job_id(load_example("job_running"), job_id))


def test_stream_of_an_unknown_job_returns_404(queue):
    assert make_client(queue).get("/api/jobs/no-such-job/events").status_code == 404
