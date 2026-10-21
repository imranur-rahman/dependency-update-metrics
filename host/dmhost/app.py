"""The FastAPI app of the Host.

``create_app`` takes its dependencies as arguments, so that tests can give fakes:

- ``queue``: the :class:`JobQueue` that the workers read.
- ``analysis``: the function ``(comparison, package) -> dict`` that the workers call.
  The API does not run it. It is on ``app.state.analysis``, so that an in-process
  worker (tests, local runs) can use the same function as the app.
- ``turnstile_check``: a function ``(token, remote_ip) -> bool`` that checks the bot token.
- ``allowed_origins``: the browser origins that CORS allows (the Webapp origin).
- ``event_stream``: the timing of ``GET /api/jobs/{id}/events`` (see ``events.py``).

``create_app_from_env`` builds the production app from :class:`HostSettings`.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Optional, Sequence

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from .events import EventStreamSettings, job_events
from .job_queue import JobQueue
from .models import DONE, ERROR, QUEUED, RUNNING, CreateJobRequest, Job
from .settings import HostSettings, accept_all_tokens
from .tool_analysis import ToolAnalysis
from .worker import Analysis

TurnstileCheck = Callable[[str, Optional[str]], bool]


def create_app(
    *,
    queue: JobQueue,
    analysis: Analysis,
    turnstile_check: TurnstileCheck,
    allowed_origins: Sequence[str] = (),
    event_stream: EventStreamSettings = EventStreamSettings(),
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
    )

    @app.post("/api/jobs", status_code=202)
    def create_job(body: CreateJobRequest, request: Request) -> Dict[str, str]:
        remote_ip = request.client.host if request.client else None
        if not turnstile_check(body.turnstile_token, remote_ip):
            raise HTTPException(status_code=403, detail="The bot check failed")
        job_id = queue.add(body.comparison())
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
    return create_app(
        queue=JobQueue(settings.db_path),
        analysis=ToolAnalysis(settings.output_dir),
        # Ticket #10 replaces this with the Cloudflare Turnstile check.
        turnstile_check=accept_all_tokens,
        allowed_origins=settings.allowed_origins,
    )
