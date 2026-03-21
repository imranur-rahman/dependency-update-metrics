"""Tests for frozen data models."""

from datetime import datetime, timezone

import pytest

from dependency_metrics.models import (
    DependencyConstraint,
    DependencyIntervalRecord,
    Interval,
    PackageVersion,
)

try:
    from dataclasses import FrozenInstanceError as _FrozenInstanceError
except ImportError:
    _FrozenInstanceError = AttributeError  # Python < 3.11


def test_package_version_fields():
    dt = datetime(2020, 6, 1, tzinfo=timezone.utc)
    pv = PackageVersion(name="requests", version="2.28.0", released_at=dt)
    assert pv.name == "requests"
    assert pv.version == "2.28.0"
    assert pv.released_at == dt


def test_package_version_frozen():
    dt = datetime(2020, 6, 1, tzinfo=timezone.utc)
    pv = PackageVersion(name="requests", version="2.28.0", released_at=dt)
    with pytest.raises(Exception):
        pv.name = "other"  # type: ignore[misc]


def test_dependency_constraint_frozen():
    dc = DependencyConstraint(name="flask", constraint=">=2.0")
    assert dc.name == "flask"
    assert dc.constraint == ">=2.0"
    with pytest.raises(Exception):
        dc.name = "other"  # type: ignore[misc]


def test_interval_fields():
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    end = datetime(2021, 1, 1, tzinfo=timezone.utc)
    iv = Interval(start=start, end=end)
    assert iv.start == start
    assert iv.end == end
    with pytest.raises(Exception):
        iv.start = end  # type: ignore[misc]
