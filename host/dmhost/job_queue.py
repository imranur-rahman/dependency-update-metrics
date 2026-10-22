"""The Job queue: one SQLite table that the API and the workers share.

Everything runs on one machine, so SQLite is enough (spec #1). WAL mode lets the API
read while a worker writes. Each method opens its own connection, so one ``JobQueue``
works from many threads and processes.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional

if os.name == "nt":  # pragma: no cover - the Host runs on Linux
    import msvcrt
else:
    import fcntl

from .limits import QueueFull, RateLimited, SubmitLimits
from .models import CANCELLED, DONE, ERROR, QUEUED, RUNNING, Comparison, Job

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
    finished_at REAL,
    package_index INTEGER NOT NULL DEFAULT 0,
    package_count INTEGER NOT NULL DEFAULT 0,
    reuse_key TEXT
)
"""

# ``_set_up`` creates the indexes after it adds the missing columns.
# The IP hash of each Job that came through ``admit`` (ticket #10). A row lives only for the
# rate-limit window, or until its Job ends if the Job is still active at the end of the window.
_SUBMITTERS_SCHEMA = """
CREATE TABLE IF NOT EXISTS submitters (
    job_id TEXT PRIMARY KEY,
    ip_hash TEXT NOT NULL,
    submitted_at REAL NOT NULL
)
"""

_INDEXES = (
    "CREATE INDEX IF NOT EXISTS jobs_reuse_key ON jobs (reuse_key, finished_at)",
    "CREATE INDEX IF NOT EXISTS jobs_created_at ON jobs (created_at)",
)

# Columns that later tickets added. ``_set_up`` adds each one that an existing file lacks.
_ADDED_COLUMNS = {
    "package_index": "INTEGER NOT NULL DEFAULT 0",
    "package_count": "INTEGER NOT NULL DEFAULT 0",
    "reuse_key": "TEXT",
}


@contextmanager
def _exclusive_lock(lock_path: Path) -> Iterator[None]:
    """Hold an exclusive lock on ``lock_path`` across threads and processes.

    Each call opens the file again, so two threads of one process also wait for each other.
    The operating system frees the lock when the holder ends, also after a crash, so a
    stale lock file does not block the next start.
    """
    with open(lock_path, "a+b") as lock_file:
        if os.name == "nt":  # pragma: no cover
            lock_file.seek(0)
            while True:
                try:
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_LOCK, 1)
                    break
                except OSError:  # LK_LOCK gives up after 10 seconds; wait again
                    continue
            try:
                yield
            finally:
                lock_file.seek(0)
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


# A clock gives the current time in seconds since the epoch, as ``time.time`` does.
Clock = Callable[[], float]


class JobQueue:
    """The SQLite table that holds the Jobs, in submit order.

    ``clock`` gives the time that the queue stores and compares (``time.time`` by
    default). Tests give a clock that they can move.
    """

    def __init__(self, db_path: Path | str, clock: Clock = time.time) -> None:
        self.db_path = Path(db_path)
        self.clock = clock
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._set_up()

    def _set_up(self) -> None:
        # The API and the workers can open the file at the same time, so only one of them
        # at a time runs the set-up (the WAL switch, the tables, the migration). Without
        # the lock, SQLite fails in two ways:
        # - It does not call the busy handler for the switch to WAL, so the loser gets
        #   "database is locked" at once.
        # - Two connections that close at once can both skip the last checkpoint, so
        #   jobs.db-wal stays. SQLite deletes the WAL of a database with 0 pages when it
        #   opens it, and the second of two openers gets "disk I/O error"
        #   (SQLITE_IOERR_DELETE_NOENT).
        # The lock covers only set-up. Other connections can still hold the file, so keep
        # the retry for "locked" for as long as the busy timeout of other statements.
        deadline = time.monotonic() + _BUSY_TIMEOUT_S
        with _exclusive_lock(self.db_path.with_name(self.db_path.name + ".lock")):
            while True:
                try:
                    with self._connect() as conn:
                        conn.execute("PRAGMA journal_mode=WAL")
                        conn.execute(_SCHEMA)
                        conn.execute(_SUBMITTERS_SCHEMA)
                        _add_missing_columns(conn)
                        for index in _INDEXES:
                            conn.execute(index)
                    return
                except sqlite3.OperationalError as exc:
                    if "locked" not in str(exc) or time.monotonic() > deadline:
                        raise
                    time.sleep(0.01)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path, timeout=_BUSY_TIMEOUT_S, isolation_level=None)
        conn.row_factory = sqlite3.Row
        # SQLite overwrites deleted rows with zeros, so a removed IP hash does not stay in a
        # free page of the file.
        conn.execute("PRAGMA secure_delete=ON")
        try:
            yield conn
        finally:
            conn.close()

    def add(self, comparison: Comparison, reuse_key: Optional[str] = None) -> str:
        """Put a new Job at the end of the queue and return its job ID.

        ``reuse_key`` (see ``reuse.py``) lets a later Comparison find the result of this Job.
        """
        with self._connect() as conn:
            return _insert_job(conn, comparison, self.clock(), reuse_key)

    def admit(
        self,
        comparison: Comparison,
        ip_hash: str,
        limits: SubmitLimits,
        reuse_key: Optional[str] = None,
    ) -> str:
        """Add a new Job for the user with the IP hash ``ip_hash`` if the limits allow it.

        Return the job ID. Raise :class:`RateLimited` when the user has the maximum number
        of active Jobs or of Jobs in the window, and :class:`QueueFull` when the queue holds
        the maximum number of waiting Jobs. One write transaction checks the limits and adds
        the Job, so two submits at the same time cannot both pass the last free place.

        Only Jobs that come through this method count against the limits. A submit that
        reuses a stored result does not call it.
        """
        now = self.clock()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                _forget_submitters(conn, now - limits.window_seconds)
                _check_limits(conn, ip_hash, limits, now)
                job_id = _insert_job(conn, comparison, now, reuse_key)
                conn.execute(
                    "INSERT INTO submitters (job_id, ip_hash, submitted_at) VALUES (?, ?, ?)",
                    (job_id, ip_hash, now),
                )
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")
        return job_id

    def forget_old_submitters(self, window_seconds: float) -> int:
        """Remove the IP hashes that are older than the window, and return how many.

        A hash of a Job that is still active stays until the Job ends, because the limit on
        active Jobs needs it. ``admit`` also calls this, and the API calls it on a timer, so
        that the hashes go away when no new submit comes.
        """
        with self._connect() as conn:
            removed = _forget_submitters(conn, self.clock() - window_seconds)
            if removed:
                # Removed rows can stay in old frames of the WAL file until a checkpoint.
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        return removed

    def done_jobs(self, reuse_key: str, max_age_seconds: float) -> List[Job]:
        """Return the done Jobs with ``reuse_key`` that finished at most ``max_age_seconds``
        ago, the newest first."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM jobs WHERE reuse_key = ? AND state = ? AND finished_at >= ? "
                "ORDER BY finished_at DESC",
                (reuse_key, DONE, self.clock() - max_age_seconds),
            ).fetchall()
        return [_job(row) for row in rows]

    def delete_jobs_older_than(self, max_age_seconds: float) -> int:
        """Delete the Jobs that were submitted more than ``max_age_seconds`` ago, and
        return how many the queue deleted."""
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM jobs WHERE created_at < ?", (self.clock() - max_age_seconds,)
            )
            return cursor.rowcount

    def get(self, job_id: str) -> Optional[Job]:
        """Return the Job, or ``None`` if the queue has no Job with this ID.

        A waiting Job has its ``position``. One read transaction reads the Job and the
        position, so a worker that claims the Job between the two reads cannot give a
        waiting Job with position 0.
        """
        with self._connect() as conn:
            conn.execute("BEGIN")
            row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
            position = _position(conn, job_id) if row and row["state"] == QUEUED else 0
            conn.execute("COMMIT")
        if row is None:
            return None
        return _job(row).model_copy(update={"position": position})

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
            # The Job starts at package 1, so no reader sees a running Job with no package.
            count = len(Comparison.model_validate_json(row["comparison"]).packages)
            conn.execute(
                "UPDATE jobs SET state = ?, started_at = ?, package_index = 1, package_count = ? "
                "WHERE job_id = ?",
                (RUNNING, self.clock(), count, row["job_id"]),
            )
            conn.execute("COMMIT")
        return _job(row).model_copy(
            update={"state": RUNNING, "package_index": 1, "package_count": count}
        )

    def set_progress(self, job_id: str, package_index: int, package_count: int) -> None:
        """Store that the worker now analyzes package ``package_index`` (1-based) of
        ``package_count``."""
        with self._connect() as conn:
            conn.execute(
                "UPDATE jobs SET package_index = ?, package_count = ? WHERE job_id = ?",
                (package_index, package_count, job_id),
            )

    def finish(self, job_id: str, results: List[Dict[str, Any]]) -> bool:
        """Store the results of a running Job and mark it as done.

        Return ``False`` (and change nothing) if the Job no longer runs."""
        return self._end(job_id, DONE, results=json.dumps(results))

    def fail(self, job_id: str, message: str) -> bool:
        """Store the user message of a running Job that failed and mark it as an error.

        Return ``False`` (and change nothing) if the Job no longer runs."""
        return self._end(job_id, ERROR, message=message)

    def ping(self) -> None:
        """Read the jobs table once. Raise ``sqlite3.Error`` if the queue file does not answer."""
        with self._connect() as conn:
            conn.execute("SELECT 1 FROM jobs LIMIT 1").fetchone()

    def state(self, job_id: str) -> Optional[str]:
        """Return the state of the Job, or ``None`` if the queue has no Job with this ID."""
        with self._connect() as conn:
            row = conn.execute("SELECT state FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        return None if row is None else str(row["state"])

    def cancel(self, job_id: str) -> Optional[Job]:
        """Cancel a Job that waits or runs, and return the Job after the change.

        A waiting Job leaves the queue at once. A running Job gets the state ``cancelled``,
        and its worker stops the analysis when it sees the state. A Job in a final state
        does not change. Return ``None`` if the queue has no Job with this ID.
        """
        with self._connect() as conn:
            conn.execute(
                "UPDATE jobs SET state = ?, finished_at = ? WHERE job_id = ? AND state IN (?, ?)",
                (CANCELLED, self.clock(), job_id, QUEUED, RUNNING),
            )
        return self.get(job_id)

    def _end(self, job_id: str, state: str, results: str = "[]", message: str = "") -> bool:
        # Only a running Job can end. A Job that the user cancelled, or that the worker
        # already stopped (for example on the timeout), keeps its state.
        with self._connect() as conn:
            cursor = conn.execute(
                "UPDATE jobs SET state = ?, results = ?, message = ?, finished_at = ? "
                "WHERE job_id = ? AND state = ?",
                (state, results, message, self.clock(), job_id, RUNNING),
            )
            return cursor.rowcount == 1

    def position(self, job_id: str) -> int:
        """Return the 1-based place of a waiting Job in the queue."""
        with self._connect() as conn:
            return _position(conn, job_id)


def _position(conn: sqlite3.Connection, job_id: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE state = ? AND rowid <= "
        "(SELECT rowid FROM jobs WHERE job_id = ?)",
        (QUEUED, job_id),
    ).fetchone()
    return int(row[0])


def _insert_job(
    conn: sqlite3.Connection, comparison: Comparison, now: float, reuse_key: Optional[str]
) -> str:
    job_id = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO jobs (job_id, state, comparison, created_at, reuse_key) "
        "VALUES (?, ?, ?, ?, ?)",
        (job_id, QUEUED, comparison.model_dump_json(), now, reuse_key),
    )
    return job_id


def _forget_submitters(conn: sqlite3.Connection, cutoff: float) -> int:
    cursor = conn.execute(
        "DELETE FROM submitters WHERE submitted_at <= ? AND job_id NOT IN "
        "(SELECT job_id FROM jobs WHERE state IN (?, ?))",
        (cutoff, QUEUED, RUNNING),
    )
    return cursor.rowcount


def _check_limits(conn: sqlite3.Connection, ip_hash: str, limits: SubmitLimits, now: float) -> None:
    # LEFT JOIN: a Job that the retention removed still counts for its hour.
    rows = conn.execute(
        "SELECT s.submitted_at, j.state FROM submitters s LEFT JOIN jobs j USING (job_id) "
        "WHERE s.ip_hash = ? ORDER BY s.submitted_at",
        (ip_hash,),
    ).fetchall()
    waits = []
    active = sum(1 for row in rows if row["state"] in (QUEUED, RUNNING))
    if active >= limits.max_active_jobs_per_ip:
        waits.append(limits.active_job_retry_seconds)
    cutoff = now - limits.window_seconds
    in_window = [row["submitted_at"] for row in rows if row["submitted_at"] > cutoff]
    if len(in_window) >= limits.max_jobs_per_ip_per_window:
        # The user can submit again when enough old Jobs leave the window.
        leaves = in_window[len(in_window) - limits.max_jobs_per_ip_per_window]
        waits.append(leaves + limits.window_seconds - now)
    if waits:
        raise RateLimited(max(waits))
    waiting = conn.execute("SELECT COUNT(*) FROM jobs WHERE state = ?", (QUEUED,)).fetchone()
    if int(waiting[0]) >= limits.max_waiting_jobs:
        raise QueueFull()


def _add_missing_columns(conn: sqlite3.Connection) -> None:
    present = {row["name"] for row in conn.execute("PRAGMA table_info(jobs)")}
    for name, declaration in _ADDED_COLUMNS.items():
        if name not in present:
            try:
                conn.execute(f"ALTER TABLE jobs ADD COLUMN {name} {declaration}")
            except sqlite3.OperationalError as exc:
                # Another process added the column after the check.
                if "duplicate column" not in str(exc):
                    raise


def _job(row: sqlite3.Row) -> Job:
    return Job(
        job_id=row["job_id"],
        state=row["state"],
        comparison=Comparison.model_validate_json(row["comparison"]),
        results=json.loads(row["results"]),
        message=row["message"],
        package_index=row["package_index"],
        package_count=row["package_count"],
    )
