"""The Job queue: one SQLite table that the API and the workers share.

Everything runs on one machine, so SQLite is enough (spec #1). WAL mode lets the API
read while a worker writes. Each method opens its own connection, so one ``JobQueue``
works from many threads and processes.
"""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from .models import DONE, ERROR, QUEUED, RUNNING, Comparison, Job

_BUSY_TIMEOUT_S = 10.0

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    comparison TEXT NOT NULL,
    results TEXT NOT NULL DEFAULT '[]',
    message TEXT NOT NULL DEFAULT '',
    created_at REAL NOT NULL,
    started_at REAL,
    finished_at REAL
)
"""


class JobQueue:
    """The SQLite table that holds the Jobs, in submit order."""

    def __init__(self, db_path: Path | str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._set_up()

    def _set_up(self) -> None:
        # The API and the workers can open a new file at the same time. SQLite does not
        # call the busy handler for the switch to WAL, so the loser gets "database is
        # locked" at once. Retry for as long as the busy timeout of other statements.
        deadline = time.monotonic() + _BUSY_TIMEOUT_S
        while True:
            try:
                with self._connect() as conn:
                    conn.execute("PRAGMA journal_mode=WAL")
                    conn.execute(_SCHEMA)
                return
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc) or time.monotonic() > deadline:
                    raise
                time.sleep(0.01)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path, timeout=_BUSY_TIMEOUT_S, isolation_level=None)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def add(self, comparison: Comparison) -> str:
        """Put a new Job at the end of the queue and return its job ID."""
        job_id = uuid.uuid4().hex
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO jobs (job_id, state, comparison, created_at) VALUES (?, ?, ?, ?)",
                (job_id, QUEUED, comparison.model_dump_json(), time.time()),
            )
        return job_id

    def get(self, job_id: str) -> Optional[Job]:
        """Return the Job, or ``None`` if the queue has no Job with this ID."""
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        return _job(row) if row is not None else None

    def claim_next(self) -> Optional[Job]:
        """Mark the oldest waiting Job as running and return it, or ``None`` if none waits.

        ``BEGIN IMMEDIATE`` takes the write lock first, so two workers never claim one Job.
        """
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM jobs WHERE state = ? ORDER BY rowid LIMIT 1", (QUEUED,)
            ).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return None
            conn.execute(
                "UPDATE jobs SET state = ?, started_at = ? WHERE job_id = ?",
                (RUNNING, time.time(), row["job_id"]),
            )
            conn.execute("COMMIT")
        return _job(row).model_copy(update={"state": RUNNING})

    def finish(self, job_id: str, results: List[Dict[str, Any]]) -> None:
        """Store the results of a Job and mark it as done."""
        self._end(job_id, DONE, results=json.dumps(results))

    def fail(self, job_id: str, message: str) -> None:
        """Store the user message of a Job that failed and mark it as an error."""
        self._end(job_id, ERROR, message=message)

    def _end(self, job_id: str, state: str, results: str = "[]", message: str = "") -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE jobs SET state = ?, results = ?, message = ?, finished_at = ? "
                "WHERE job_id = ?",
                (state, results, message, time.time(), job_id),
            )

    def position(self, job_id: str) -> int:
        """Return the 1-based place of a waiting Job in the queue."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM jobs WHERE state = ? AND rowid <= "
                "(SELECT rowid FROM jobs WHERE job_id = ?)",
                (QUEUED, job_id),
            ).fetchone()
        return int(row[0])


def _job(row: sqlite3.Row) -> Job:
    return Job(
        job_id=row["job_id"],
        state=row["state"],
        comparison=Comparison.model_validate_json(row["comparison"]),
        results=json.loads(row["results"]),
        message=row["message"],
    )
