"""Tests for the Job lifecycle: cancel, the timeout, the memory limit, and the workers.

The tests that need a Job to stop in the middle of an analysis use isolated workers: each
Job runs in its own child process. Their fake analysis functions are at module level, so
that the child process can import them. The limits are short, so the tests stay fast.
"""

import threading
import time

from fastapi.testclient import TestClient

from contract_examples import load_example, with_job_id
from dmhost import EventStreamSettings, JobLimits, JobQueue, Worker, WorkerPool, create_app
from dmhost.settings import HostSettings

# Read the child process often, so that the tests stay fast.
FAST = {"poll_seconds": 0.02}


def accept_all_tokens(token, remote_ip):
    return True


def quick_analysis(comparison, package):
    return {"package": package}


def make_client(queue, analysis=quick_analysis):
    return TestClient(create_app(queue=queue, analysis=analysis, turnstile_check=accept_all_tokens))


def submit(client, packages=("mypkg",)):
    request = load_example("create_job_request")
    request["packages"] = list(packages)
    response = client.post("/api/jobs", json=request)
    assert response.status_code == 202
    return response.json()["job_id"]


def test_cancel_removes_a_waiting_job_from_the_queue(tmp_path):
    queue = JobQueue(tmp_path / "jobs.db")
    client = make_client(queue)
    job_id = submit(client)

    response = client.delete(f"/api/jobs/{job_id}")

    assert response.status_code == 200
    assert response.json() == with_job_id(load_example("job_cancelled"), job_id)
    assert client.get(f"/api/jobs/{job_id}").json() == response.json()
    assert Worker(queue, quick_analysis).run_once() is False


def test_cancel_of_a_running_job_stops_it_before_the_next_package(tmp_path):
    queue = JobQueue(tmp_path / "jobs.db")
    client = make_client(queue)
    job_id = submit(client, packages=["first", "second"])
    calls = []

    def analysis_that_cancels(comparison, package):
        calls.append(package)
        assert client.delete(f"/api/jobs/{job_id}").json()["state"] == "cancelled"
        return {"package": package}

    Worker(queue, analysis_that_cancels).run_once()

    assert calls == ["first"]
    assert client.get(f"/api/jobs/{job_id}").json()["state"] == "cancelled"


def endless_analysis(comparison, package):
    while True:
        time.sleep(0.05)


def test_job_that_runs_longer_than_the_timeout_stops_with_the_timeout_message(tmp_path):
    queue = JobQueue(tmp_path / "jobs.db")
    client = make_client(queue)
    job_id = submit(client)
    worker = Worker(queue, endless_analysis, limits=JobLimits(timeout_seconds=1.0, **FAST))

    started = time.monotonic()
    assert worker.run_once() is True

    assert time.monotonic() - started < 10
    body = client.get(f"/api/jobs/{job_id}").json()
    assert body["state"] == "error"
    assert body["message"] == "The analysis took too long"


def run_in_thread(worker):
    thread = threading.Thread(target=worker.run_once)
    thread.start()
    return thread


def wait_for_state(client, job_id, state, seconds=10.0):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        body = client.get(f"/api/jobs/{job_id}").json()
        if body["state"] == state:
            return body
        time.sleep(0.02)
    raise AssertionError(f"Job {job_id} did not reach {state}: {body}")


def test_cancel_stops_a_running_job_in_the_middle_of_an_analysis(tmp_path):
    queue = JobQueue(tmp_path / "jobs.db")
    client = make_client(queue)
    job_id = submit(client)
    worker = Worker(queue, endless_analysis, limits=JobLimits(timeout_seconds=60.0, **FAST))
    thread = run_in_thread(worker)
    wait_for_state(client, job_id, "running")

    response = client.delete(f"/api/jobs/{job_id}")
    thread.join(timeout=10)

    assert response.json()["state"] == "cancelled"
    assert not thread.is_alive()
    assert client.get(f"/api/jobs/{job_id}").json()["state"] == "cancelled"


def memory_hungry_analysis(comparison, package):
    if package == "hungry":
        # Write each page, so that the memory counts in the RSS.
        held = bytearray(b"\x01") * (600 * 1024 * 1024)
        while True:
            time.sleep(0.05)
            held[0] = 1
    return {"package": package}


def test_job_over_the_memory_limit_stops_with_an_error_and_the_next_job_runs(tmp_path):
    queue = JobQueue(tmp_path / "jobs.db")
    client = make_client(queue)
    hungry = submit(client, packages=["hungry"])
    next_job = submit(client, packages=["small"])
    limits = JobLimits(timeout_seconds=60.0, max_rss_mb=400, **FAST)
    worker = Worker(queue, memory_hungry_analysis, limits=limits)

    assert worker.run_once() is True
    assert worker.run_once() is True

    body = client.get(f"/api/jobs/{hungry}").json()
    assert body["state"] == "error"
    assert body["message"] == f"Something went wrong (ref: {hungry})"
    after = client.get(f"/api/jobs/{next_job}").json()
    assert after["state"] == "done"
    assert after["results"] == [{"package": "small"}]


def stream_client(queue):
    app = create_app(
        queue=queue,
        analysis=quick_analysis,
        turnstile_check=accept_all_tokens,
        event_stream=EventStreamSettings(poll_seconds=0.01, max_seconds=10.0),
    )
    return TestClient(app)


def test_event_stream_sends_cancelled_when_the_user_cancels_the_job(tmp_path):
    queue = JobQueue(tmp_path / "jobs.db")
    client = stream_client(queue)
    job_id = submit(client)
    events = []

    with client.stream("GET", f"/api/jobs/{job_id}/events") as response:
        for line in response.iter_lines():
            if line.startswith("event: "):
                events.append(line.removeprefix("event: "))
                if events == ["queued"]:
                    client.delete(f"/api/jobs/{job_id}")

    assert events == ["queued", "cancelled"]


def test_job_continues_when_its_event_stream_closes(tmp_path):
    queue = JobQueue(tmp_path / "jobs.db")
    client = stream_client(queue)
    job_id = submit(client)

    with client.stream("GET", f"/api/jobs/{job_id}/events") as response:
        for line in response.iter_lines():
            if line.startswith("event: "):
                break
    assert Worker(queue, quick_analysis).run_once() is True

    assert client.get(f"/api/jobs/{job_id}").json()["state"] == "done"


def test_pool_runs_as_many_jobs_in_parallel_as_it_has_workers(tmp_path):
    queue = JobQueue(tmp_path / "jobs.db")
    client = make_client(queue)
    job_ids = [submit(client, packages=[f"pkg{i}"]) for i in range(3)]
    # Each analysis waits until all three run at once. Fewer parallel workers break it.
    all_running = threading.Barrier(3, timeout=10)

    def analysis_that_waits_for_the_others(comparison, package):
        all_running.wait()
        return {"package": package}

    pool = WorkerPool(queue, analysis_that_waits_for_the_others, workers=3, poll_seconds=0.01)
    pool.start()
    try:
        bodies = [wait_for_state(client, job_id, "done") for job_id in job_ids]
    finally:
        pool.stop()

    assert [b["results"] for b in bodies] == [[{"package": f"pkg{i}"}] for i in range(3)]


def test_worker_count_and_job_limits_are_configuration_values(monkeypatch):
    defaults = HostSettings.from_env()
    monkeypatch.setenv("DM_HOST_WORKERS", "2")
    monkeypatch.setenv("DM_HOST_JOB_TIMEOUT_SECONDS", "60")
    monkeypatch.setenv("DM_HOST_WORKER_MEMORY_MB", "512")
    changed = HostSettings.from_env()

    assert (defaults.workers, defaults.job_limits) == (
        6,
        JobLimits(timeout_seconds=900.0, max_rss_mb=2048),
    )
    assert (changed.workers, changed.job_limits) == (
        2,
        JobLimits(timeout_seconds=60.0, max_rss_mb=512),
    )
