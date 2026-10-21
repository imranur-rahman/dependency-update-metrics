"""Result reuse and Job retention (spec #1).

A Comparison that matches a done Job of the last 72 hours gets the result of that Job, and
the Host starts no new Job. The reuse key is the ecosystem, the set of packages, the window,
the mode, the weighting type, the half-life, and the date of the OSV index. A rebuild of the
OSV index therefore stops the reuse of older results. The Host deletes Job records after
30 days.

The reuse lookup is its own step (``find_reusable_job``), so that ``POST /api/jobs`` can run
the rate limits after it: a reused result does not count against the limits.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from typing import Callable, Optional

from .job_queue import JobQueue
from .models import Comparison, Job, package_key

HOUR_SECONDS = 3600.0
DAY_SECONDS = 24 * HOUR_SECONDS

# Returns the build date of the OSV index that the workers read, or None without an index.
OsvIndexDate = Callable[[], Optional[date]]


def no_osv_index() -> Optional[date]:
    """An ``OsvIndexDate`` for an app with no OSV index (tests, local runs)."""
    return None


@dataclass(frozen=True)
class ReusePolicy:
    """How long the Host reuses a result, and how long it keeps a Job record."""

    reuse_seconds: float = 72 * HOUR_SECONDS
    retention_seconds: float = 30 * DAY_SECONDS


def reuse_key(comparison: Comparison, osv_index_date: Optional[date]) -> str:
    """Return the key that two Comparisons with the same result share.

    The packages are a set: their order does not matter, and two names of one package
    (``package_key``) are the same package.
    """
    fields = {
        "ecosystem": comparison.ecosystem,
        "packages": sorted(package_key(comparison.ecosystem, p) for p in comparison.packages),
        "start_date": comparison.start_date.isoformat(),
        "end_date": comparison.end_date.isoformat(),
        "mode": comparison.mode,
        "weighting_type": comparison.weighting_type,
        "half_life": comparison.half_life,
        "osv_index_date": osv_index_date.isoformat() if osv_index_date else None,
    }
    return json.dumps(fields, sort_keys=True)


def find_reusable_job(queue: JobQueue, key: str, policy: ReusePolicy) -> Optional[Job]:
    """Return the newest done Job with ``key`` that is recent enough to reuse, or ``None``."""
    return queue.latest_done_job(key, policy.reuse_seconds)


def delete_expired_jobs(queue: JobQueue, policy: ReusePolicy) -> int:
    """Delete the Job records that are older than the retention time."""
    return queue.delete_jobs_older_than(policy.retention_seconds)
