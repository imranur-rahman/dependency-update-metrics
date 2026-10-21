"""Tests for the Job queue that the API and the workers share."""

import threading

from dmhost import JobQueue
from dmhost.models import Comparison


def test_api_and_workers_that_open_one_new_queue_file_at_once_all_succeed(tmp_path):
    # Regression: the API and a worker that started together failed with
    # "sqlite3.OperationalError: database is locked" on PRAGMA journal_mode=WAL.
    db_path = tmp_path / "jobs.db"
    start = threading.Barrier(8)
    errors = []

    def open_queue():
        start.wait()
        try:
            JobQueue(db_path)
        except Exception as exc:  # noqa: BLE001 - the test collects every failure
            errors.append(exc)

    for _ in range(20):
        db_path.unlink(missing_ok=True)
        threads = [threading.Thread(target=open_queue) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        start.reset()

    assert errors == []


def test_two_queue_objects_over_one_file_see_the_same_jobs(tmp_path):
    api_side = JobQueue(tmp_path / "jobs.db")
    worker_side = JobQueue(tmp_path / "jobs.db")
    comparison = Comparison(
        ecosystem="npm", packages=["express"], start_date="2023-01-01", end_date="2025-01-01"
    )

    job_id = api_side.add(comparison)
    claimed = worker_side.claim_next()

    assert claimed is not None and claimed.job_id == job_id
    assert api_side.get(job_id).state == "running"
    assert worker_side.claim_next() is None
