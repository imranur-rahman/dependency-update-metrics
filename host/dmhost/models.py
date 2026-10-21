"""The request and response shapes of the Host API (spec #1, "API contract").

``contract/examples/`` holds example bodies. The Host tests and the Webapp tests read them.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, model_validator

QUEUED = "queued"
RUNNING = "running"
DONE = "done"
ERROR = "error"

# The weighting types of the Tool (``--weighting-type``).
WeightingType = Literal["disable", "linear", "exponential", "inverse"]
EXPONENTIAL = "exponential"
# The range of the half-life in days that exponential weighting needs (spec #1).
MIN_HALF_LIFE_DAYS = 1
MAX_HALF_LIFE_DAYS = 3650


class Comparison(BaseModel):
    """One user request: packages from one ecosystem, one window, one mode, one weighting."""

    ecosystem: str
    packages: List[str]
    start_date: date
    end_date: date
    mode: str = "window_summary"
    weighting_type: WeightingType = "disable"
    half_life: Optional[float] = None

    @model_validator(mode="after")
    def _check_half_life(self) -> "Comparison":
        """Exponential weighting needs a half-life from 1 to 3650 days.

        The other weighting types do not use a half-life, so the model drops it. Then two
        Comparisons that give the same result also look the same.
        """
        if self.weighting_type != EXPONENTIAL:
            self.half_life = None
        elif self.half_life is None or not (
            MIN_HALF_LIFE_DAYS <= self.half_life <= MAX_HALF_LIFE_DAYS
        ):
            raise ValueError(
                f"Exponential weighting needs a half-life from {MIN_HALF_LIFE_DAYS} "
                f"to {MAX_HALF_LIFE_DAYS} days"
            )
        return self


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
