"""The request and response shapes of the Host API (spec #1, "API contract").

``contract/examples/`` holds example bodies. The Host tests and the Webapp tests read them.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
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

# The time zone that is furthest ahead of UTC (UTC+14). A user there can have a date one day
# after the UTC date, so the Host accepts an end date up to the latest date on Earth.
_LATEST_UTC_OFFSET = timedelta(hours=14)


def latest_date_now() -> date:
    """Return the latest calendar date in any time zone now (an end date may be this date)."""
    return (datetime.now(timezone.utc) + _LATEST_UTC_OFFSET).date()


# The naming rules of each ecosystem. The Webapp logic module uses the same rules
# (PACKAGE_NAME_RULES in comparison.js).
# - npm: an optional "@scope/" and a name of URL-safe characters that does not start with
#   "." or "_", at most 214 characters (validate-npm-package-name). Old names can have
#   capitals (for example "JSONStream"), so the rule accepts them.
# - PyPI: letters, digits, ".", "_", and "-", with a letter or a digit at each end (PEP 508).
# - Cargo: a letter, then letters, digits, "_", and "-", at most 64 characters (crates.io).
PACKAGE_NAME_RULES = {
    "npm": re.compile(r"(@[A-Za-z0-9~-][A-Za-z0-9._~-]*/)?[A-Za-z0-9~-][A-Za-z0-9._~-]*"),
    "pypi": re.compile(r"[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?"),
    "cargo": re.compile(r"[A-Za-z][A-Za-z0-9_-]{0,63}"),
}
MAX_PACKAGE_NAME_LENGTH = {"npm": 214}


def package_name_matches_rules(ecosystem: str, name: str) -> bool:
    """Return true if ``name`` matches the naming rules of ``ecosystem``."""
    if len(name) > MAX_PACKAGE_NAME_LENGTH.get(ecosystem, len(name)):
        return False
    return PACKAGE_NAME_RULES[ecosystem].fullmatch(name) is not None


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
        for name in names:
            if not package_name_matches_rules(self.ecosystem, name):
                raise ValueError(f"{name!r} is not a valid {self.ecosystem} package name")
        keys = [package_key(self.ecosystem, name) for name in names]
        if len(set(keys)) != len(keys):
            raise ValueError("The Comparison holds the same package twice")
        self.packages = names
        return self

    @model_validator(mode="after")
    def _check_dates(self) -> "Comparison":
        """The end date is after the start date and not in the future.

        The Host does not limit the window length, because the job timeout stops a long Job.
        """
        if self.end_date <= self.start_date:
            raise ValueError("The end date must be after the start date")
        if self.end_date > latest_date_now():
            raise ValueError("The end date must not be in the future")
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
