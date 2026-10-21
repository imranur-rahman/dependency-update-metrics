"""The request and response shapes of the Host API (spec #1, "API contract").

``contract/examples/`` holds example bodies. The Host tests and the Webapp tests read them.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

QUEUED = "queued"
RUNNING = "running"
DONE = "done"
ERROR = "error"


class Comparison(BaseModel):
    """One user request: packages from one ecosystem, one window, one mode, one weighting."""

    ecosystem: str
    packages: List[str]
    start_date: date
    end_date: date
    mode: str = "window_summary"
    weighting_type: str = "disable"
    half_life: Optional[float] = None


class CreateJobRequest(Comparison):
    """The body of ``POST /api/jobs``: a Comparison and the Turnstile token."""

    turnstile_token: str = ""

    def comparison(self) -> Comparison:
        return Comparison(**self.model_dump(exclude={"turnstile_token"}))


class Job(BaseModel):
    """One Job as the queue stores it."""

    job_id: str
    state: str
    comparison: Comparison
    results: List[Dict[str, Any]] = []
    message: str = ""
