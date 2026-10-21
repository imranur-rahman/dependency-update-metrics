"""The FastAPI app of the Host.

``create_app`` takes its dependencies as arguments, so that tests can give fakes:

- ``queue``: the :class:`JobQueue` that the workers read.
- ``analysis``: the function ``(comparison, package) -> dict`` that the workers call.
  The API does not run it. It is on ``app.state.analysis``, so that an in-process
  worker (tests, local runs) can use the same function as the app.
- ``turnstile_check``: a function ``(token, remote_ip) -> bool`` that checks the bot token.
- ``allowed_origins``: the browser origins that CORS allows (the Webapp origin).
- ``event_stream``: the timing of ``GET /api/jobs/{id}/events`` (see ``events.py``).
- ``osv_index_date``: a function that returns the build date of the OSV index. It is part
  of the reuse key (see ``reuse.py``).
- ``reuse``: how long the Host reuses a result and keeps a Job record.
- ``limits``: the per-IP limits and the queue cap (see ``limits.py``). The default is
  :data:`NO_LIMITS`, so that tests of other features can submit many Jobs from one client.
  ``create_app_from_env`` gives the configured limits.
- ``ip_hasher``: the function that turns an IP address into the hash that the queue stores.
- ``trusted_proxies``: the proxies (Caddy) whose ``X-Forwarded-For`` the API reads
  (see ``client_ip.py``).

``create_app_from_env`` builds the production app from :class:`HostSettings`.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable, Dict, Sequence

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from dependency_metrics.osv_builder import OSVBuilder

from .client_ip import Network, client_ip
from .errors import SERVER_BUSY, too_many_requests
from .events import EventStreamSettings, job_events
from .job_queue import JobQueue
from .limits import NO_LIMITS, IpHasher, QueueFull, RateLimited, SubmitLimits
from .models import DONE, ERROR, QUEUED, RUNNING, CreateJobRequest, Job
from .reuse import (
    OsvIndexDate,
    ReusePolicy,
    delete_expired_jobs,
    find_reusable_job,
    no_osv_index,
    reuse_key,
)
from .settings import HostSettings
from .tool_analysis import ToolAnalysis
from .turnstile import TurnstileCheck, TurnstileVerifier
from .worker import Analysis

logger = logging.getLogger(__name__)

# How often the production API removes the IP hashes that are older than the window.
_FORGET_INTERVAL_S = 60.0


def create_app(
    *,
    queue: JobQueue,
    analysis: Analysis,
    turnstile_check: TurnstileCheck,
    allowed_origins: Sequence[str] = (),
    event_stream: EventStreamSettings = EventStreamSettings(),
    osv_index_date: OsvIndexDate = no_osv_index,
    reuse: ReusePolicy = ReusePolicy(),
    limits: SubmitLimits = NO_LIMITS,
    ip_hasher: Callable[[str], str] = IpHasher(),
    trusted_proxies: Sequence[Network] = (),
) -> FastAPI:
    """Build the Host API over ``queue``."""
    app = FastAPI(title="dependency-metrics Host")
    app.state.queue = queue
    app.state.analysis = analysis
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(allowed_origins),
        allow_methods=["GET", "POST", "DELETE"],
        allow_headers=["Content-Type"],
        expose_headers=["Retry-After"],
    )

    @app.post("/api/jobs", status_code=202)
    def create_job(body: CreateJobRequest, request: Request, response: Response) -> Dict[str, Any]:
        remote_ip = client_ip(
            request.client.host if request.client else None,
            request.headers.get("x-forwarded-for"),
            trusted_proxies,
        )
        if not turnstile_check(body.turnstile_token, remote_ip):
            raise HTTPException(status_code=403, detail="The bot check failed")
        comparison = body.comparison()
        delete_expired_jobs(queue, reuse)
        # Result reuse: a matching done Job of the last 72 hours gives its result with 200,
        # and the Host starts no Job.
        key = reuse_key(comparison, osv_index_date())
        reused = find_reusable_job(queue, key, reuse)
        if reused is not None:
            response.status_code = 200
            return job_view(queue, reused)
        # The per-IP limits and the queue cap come after the reuse lookup, so that a reused
        # result does not count against them.
        try:
            job_id = queue.admit(comparison, ip_hasher(remote_ip or ""), limits, reuse_key=key)
        except RateLimited as limited:
            raise HTTPException(
                status_code=429,
                detail=too_many_requests(limited.retry_after_minutes),
                headers={"Retry-After": str(max(1, round(limited.retry_after_seconds)))},
            )
        except QueueFull:
            raise HTTPException(status_code=503, detail=SERVER_BUSY)
        return {"job_id": job_id}

    @app.get("/api/jobs/{job_id}")
    def get_job(job_id: str) -> Dict[str, Any]:
        job = queue.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        return job_view(queue, job)

    @app.delete("/api/jobs/{job_id}")
    def cancel_job(job_id: str) -> Dict[str, Any]:
        # A Job in a final state does not change, so a second cancel gets the same body.
        job = queue.cancel(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        return job_view(queue, job)

    @app.get("/api/jobs/{job_id}/events")
    def job_event_stream(job_id: str) -> StreamingResponse:
        if queue.get(job_id) is None:
            raise HTTPException(status_code=404, detail="Job not found")
        return StreamingResponse(
            job_events(queue, job_id, job_view, event_stream),
            media_type="text/event-stream",
            # Proxies (Caddy) must pass each event on at once and not cache the stream.
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    return app


def job_view(queue: JobQueue, job: Job) -> Dict[str, Any]:
    """Return the body of ``GET /api/jobs/{id}`` for ``job``."""
    view: Dict[str, Any] = {"job_id": job.job_id, "state": job.state}
    if job.state == QUEUED:
        view["position"] = job.position
    elif job.state == RUNNING:
        view["package_index"] = job.package_index
        view["package_count"] = job.package_count
    view["comparison"] = job.comparison.model_dump(mode="json")
    if job.state == DONE:
        view["results"] = job.results
    elif job.state == ERROR:
        view["message"] = job.message
    return view


def create_app_from_env() -> FastAPI:
    """Build the app from the environment (``uvicorn --factory dmhost.app:create_app_from_env``)."""
    settings = HostSettings.from_env()
    queue = JobQueue(settings.db_path)
    _forget_submitters_on_a_timer(queue, settings.limits.window_seconds)
    return create_app(
        queue=queue,
        analysis=ToolAnalysis(settings.output_dir),
        turnstile_check=TurnstileVerifier(settings.turnstile_secret),
        allowed_origins=settings.allowed_origins,
        osv_index_date=OSVBuilder(settings.output_dir).index_date,
        limits=settings.limits,
        ip_hasher=IpHasher(settings.ip_hash_key),
        trusted_proxies=settings.trusted_proxies,
    )


def _forget_submitters_on_a_timer(queue: JobQueue, window_seconds: float) -> None:
    """Remove old IP hashes once each minute, also when no submit comes."""

    def forget_forever() -> None:
        while True:
            try:
                queue.forget_old_submitters(window_seconds)
            except Exception:  # noqa: BLE001 - the timer must not stop
                logger.exception("Could not remove old IP hashes")
            time.sleep(_FORGET_INTERVAL_S)

    threading.Thread(target=forget_forever, name="forget-ip-hashes", daemon=True).start()
