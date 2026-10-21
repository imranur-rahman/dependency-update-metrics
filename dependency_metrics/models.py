"""
Core data models for dependency metrics.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class PackageVersion:
    """A package version with its release date."""

    name: str
    version: str
    released_at: datetime


@dataclass(frozen=True)
class DependencyConstraint:
    """Dependency constraint as declared by a package version."""

    name: str
    constraint: str


@dataclass(frozen=True)
class Interval:
    """A time interval in the analysis timeline."""

    start: datetime
    end: datetime


@dataclass(frozen=True)
class DependencyIntervalRecord:
    """Computed interval state for a dependency."""

    dependency: str
    dependency_constraint: str
    dependency_version: Optional[str]
    dependency_highest_version: Optional[str]
    interval_start: datetime
    interval_end: datetime
    updated: bool
    remediated: bool
    age_of_interval_days: float
    weight: float


@dataclass(frozen=True)
class ProgressEvent:
    """One progress step of an analysis.

    ``stage`` is ``"dependencies"`` (Window summary: one event after each direct
    dependency) or ``"releases"`` (Per-release: one event after each release).
    ``done`` of ``total`` units are complete; ``item`` names the unit that completed.
    """

    stage: str
    done: int
    total: int
    item: str = ""
