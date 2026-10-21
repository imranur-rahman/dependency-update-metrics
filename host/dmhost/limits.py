"""The submit limits of the Host, and the hash that stands for the IP address of a user.

Spec #1 ("Abuse protection") sets three limits. All three are configuration values
(``HostSettings``):

- ``max_active_jobs_per_ip``: the waiting and running Jobs of one IP address (default 1).
- ``max_jobs_per_ip_per_window``: the Jobs of one IP address in each rate-limit window
  (default 10 in each hour).
- ``max_waiting_jobs``: the waiting Jobs of all users (default 20).

``JobQueue.admit`` checks the limits and adds the Job in one transaction. It raises
:class:`RateLimited` (the API answers ``429``) or :class:`QueueFull` (``503``).

The Host never stores a plain IP address. :class:`IpHasher` gives a keyed hash (HMAC-SHA256)
of it, so that a person who reads the queue file cannot test a guessed IP address against the
stored values without the key. ``JobQueue`` removes each hash after the rate-limit window.
"""

from __future__ import annotations

import hashlib
import hmac
import math
import secrets
import sys
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class SubmitLimits:
    max_active_jobs_per_ip: int = 1
    max_jobs_per_ip_per_window: int = 10
    max_waiting_jobs: int = 20
    # The rate-limit window. The Host also keeps each IP hash for this time only.
    window_seconds: float = 3600.0
    # The wait that the message gives a user who has an active Job. The Host cannot know when
    # the Job ends, so it gives a short wait.
    active_job_retry_seconds: float = 60.0


# Limits that never refuse a Job (the default of ``create_app`` for tests).
NO_LIMITS = SubmitLimits(
    max_active_jobs_per_ip=sys.maxsize,
    max_jobs_per_ip_per_window=sys.maxsize,
    max_waiting_jobs=sys.maxsize,
)


class LimitReached(Exception):
    """A submit limit refused a new Job."""


class RateLimited(LimitReached):
    """A per-IP limit refused the Job. The user can try again after ``retry_after_seconds``."""

    def __init__(self, retry_after_seconds: float) -> None:
        super().__init__(f"rate limited for {retry_after_seconds:.0f} s")
        self.retry_after_seconds = max(0.0, retry_after_seconds)

    @property
    def retry_after_minutes(self) -> int:
        """The wait in whole minutes, rounded up, so that the user does not try too early."""
        return math.ceil(self.retry_after_seconds / 60)


class QueueFull(LimitReached):
    """The queue holds the maximum number of waiting Jobs."""


class IpHasher:
    """Return the keyed hash of an IP address.

    Without a key, the hasher makes a random key. The hashes then change when the API
    starts again, so the API forgets the limits of each user. That is safe, because the
    worst case is a user who gets a new hourly limit.
    """

    def __init__(self, key: Optional[bytes] = None) -> None:
        self._key = key if key else secrets.token_bytes(32)

    def __call__(self, ip: str) -> str:
        return hmac.new(self._key, ip.encode("utf-8"), hashlib.sha256).hexdigest()
