"""The event stream of a Job (``GET /api/jobs/{id}/events``), as server-sent events.

The stream first sends the current state of the Job, so that a client that reconnects
gets it at once. Then it reads the Job from the queue every ``poll_seconds`` and sends a
new event each time the body of ``GET /api/jobs/{id}`` changes. Each event has the state
as its name and that body as its data::

    event: running
    data: {"job_id": "...", "state": "running", "package_index": 2, ...}

The stream ends after a final state (done, error, or cancelled). It sends a heartbeat
comment when it sent nothing for ``heartbeat_seconds``, so that a quiet connection stays
open. It also ends after ``max_seconds``; the browser ``EventSource`` then connects again
by itself and gets the current state first.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, AsyncIterator, Callable, Dict, Optional

import anyio

from .job_queue import JobQueue
from .models import FINAL_STATES, Job

HEARTBEAT = ": heartbeat\n\n"


@dataclass(frozen=True)
class EventStreamSettings:
    """The timing of the event stream. Tests give short values."""

    poll_seconds: float = 0.5
    heartbeat_seconds: float = 15.0
    max_seconds: float = 300.0
    # The wait before the browser connects again after the stream ends (the SSE ``retry``).
    retry_ms: int = 2000


def format_event(view: Dict[str, Any]) -> str:
    """Return one server-sent event for the body of ``GET /api/jobs/{id}``."""
    return f"event: {view['state']}\ndata: {json.dumps(view)}\n\n"


async def job_events(
    queue: JobQueue,
    job_id: str,
    view: Callable[[JobQueue, Job], Dict[str, Any]],
    settings: EventStreamSettings,
) -> AsyncIterator[str]:
    """Yield the text of the event stream of one Job. ``view`` builds the GET body."""
    started = last_sent = time.monotonic()
    last_view: Optional[Dict[str, Any]] = None
    yield f"retry: {settings.retry_ms}\n\n"
    while True:
        # A SQLite read can wait for a lock, so it runs off the event loop.
        job = await anyio.to_thread.run_sync(queue.get, job_id)
        if job is None:
            return
        current = await anyio.to_thread.run_sync(view, queue, job)
        now = time.monotonic()
        if current != last_view:
            yield format_event(current)
            last_view, last_sent = current, now
            if job.state in FINAL_STATES:
                return
        elif now - last_sent >= settings.heartbeat_seconds:
            yield HEARTBEAT
            last_sent = now
        if now - started >= settings.max_seconds:
            return
        await anyio.sleep(settings.poll_seconds)
