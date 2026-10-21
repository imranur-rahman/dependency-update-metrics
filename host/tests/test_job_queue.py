"""Tests for the Job queue that the API and the workers share."""

import sqlite3
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


def _stale_wal_files(tmp_path):
    """Return the -wal and -shm bytes of a WAL file that a live connection still holds."""
    source = tmp_path / "source.db"
    live = sqlite3.connect(source, isolation_level=None)
    live.execute("PRAGMA journal_mode=WAL")
    live.execute("PRAGMA wal_autocheckpoint=0")
    live.execute("CREATE TABLE t (x)")
    live.execute("INSERT INTO t VALUES (1)")
    wal = (tmp_path / "source.db-wal").read_bytes()
    shm = (tmp_path / "source.db-shm").read_bytes()
    live.close()
    return wal, shm


def test_api_and_workers_that_open_an_empty_queue_file_with_a_stale_wal_all_succeed(tmp_path):
    # Regression: two connections that close at once can leave jobs.db-wal behind. SQLite
    # deletes the WAL of a database with 0 pages when it opens it, and two openers that do
    # this at once made the second one fail with "disk I/O error" (SQLITE_IOERR_DELETE_NOENT).
    wal, shm = _stale_wal_files(tmp_path)
    start = threading.Barrier(8)
    errors = []

    for round_index in range(100):
        db_path = tmp_path / f"round{round_index}" / "jobs.db"
        db_path.parent.mkdir()
        db_path.write_bytes(b"")
        (db_path.parent / "jobs.db-wal").write_bytes(wal)
        (db_path.parent / "jobs.db-shm").write_bytes(shm)

        def open_queue():
            start.wait()
            try:
                JobQueue(db_path)
            except Exception as exc:  # noqa: BLE001 - the test collects every failure
                errors.append(exc)

        threads = [threading.Thread(target=open_queue) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        start.reset()

    assert errors == []


def test_queue_file_from_before_the_progress_columns_still_stores_progress(tmp_path):
    db_path = tmp_path / "jobs.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE jobs (job_id TEXT PRIMARY KEY, state TEXT NOT NULL, "
            "comparison TEXT NOT NULL, results TEXT NOT NULL DEFAULT '[]', "
            "message TEXT NOT NULL DEFAULT '', created_at REAL NOT NULL, "
            "started_at REAL, finished_at REAL)"
        )
    conn.close()
    queue = JobQueue(db_path)
    comparison = Comparison(
        ecosystem="npm", packages=["a", "b"], start_date="2023-01-01", end_date="2025-01-01"
    )
    job_id = queue.add(comparison)

    queue.claim_next()
    queue.set_progress(job_id, 2, 2)

    job = queue.get(job_id)
    assert (job.package_index, job.package_count) == (2, 2)


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
