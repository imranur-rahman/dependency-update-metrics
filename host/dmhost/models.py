"""The request and response shapes of the Host API (spec #1, "API contract").

``contract/examples/`` holds example bodies. The Host tests and the Webapp tests read them.
"""

from __future__ import annotations

import re
from datetime import date
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, model_validator

QUEUED = "queued"
RUNNING = "running"
DONE = "done"
ERROR = "error"
# Ticket #11 adds the cancel request. The event stream and the Webapp already treat it as final.
CANCELLED = "cancelled"
FINAL_STATES = frozenset({DONE, ERROR, CANCELLED})

# The weighting types of the Tool (``--weighting-type``).
WeightingType = Literal["disable", "linear", "exponential", "inverse"]
EXPONENTIAL = "exponential"
# The range of the half-life in days that exponential weighting needs (spec #1).
MIN_HALF_LIFE_DAYS = 1
MAX_HALF_LIFE_DAYS = 3650


MAX_PACKAGES = 5


def package_key(ecosystem: str, name: str) -> str:
    """Return the form of a package name that two names of one package share.

    Registry names do not depend on case. PyPI also treats runs of ``-``, ``_``, and ``.``
    as one ``-`` (PEP 503). The Webapp logic module uses the same rule (``packageKey``).
    """
    key = name.strip().lower()
    if ecosystem == "pypi":
        key = re.sub(r"[-_.]+", "-", key)
    return key


class Comparison(BaseModel):
    """One user request: packages from one ecosystem, one window, one mode, one weighting.

    The Comparison has one ``ecosystem`` for all its packages, so a request with packages
    from more than one ecosystem cannot pass the checks.
    """

    ecosystem: Literal["npm", "pypi", "cargo"]
    packages: List[str] = Field(min_length=1, max_length=MAX_PACKAGES)
    start_date: date
    end_date: date
    mode: str = "window_summary"
    weighting_type: WeightingType = "disable"
    half_life: Optional[float] = None

    @model_validator(mode="after")
    def _check_packages(self) -> "Comparison":
        names = [name.strip() for name in self.packages]
        if any(not name for name in names):
            raise ValueError("A package name is empty")
        keys = [package_key(self.ecosystem, name) for name in names]
        if len(set(keys)) != len(keys):
            raise ValueError("The Comparison holds the same package twice")
        self.packages = names
        return self

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
    # The package that the worker analyzes now: 1-based index, and the number of packages.
    package_index: int = 0
    package_count: int = 0
    # The 1-based place of a waiting Job in the queue (``JobQueue.get`` fills it), else 0.
    position: int = 0
