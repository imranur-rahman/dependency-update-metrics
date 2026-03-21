"""Tests for time_utils helper functions."""

from datetime import datetime, timezone, timedelta

import pytest

from dependency_metrics.time_utils import ensure_utc, parse_timestamp


def test_ensure_utc_naive():
    dt = datetime(2020, 1, 1, 12, 0, 0)
    result = ensure_utc(dt)
    assert result.tzinfo == timezone.utc
    assert result == datetime(2020, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def test_ensure_utc_already_utc():
    dt = datetime(2020, 6, 15, 8, 30, 0, tzinfo=timezone.utc)
    result = ensure_utc(dt)
    assert result == dt
    assert result.tzinfo == timezone.utc


def test_ensure_utc_non_utc_aware():
    plus5 = timezone(timedelta(hours=5))
    dt = datetime(2020, 1, 1, 5, 0, 0, tzinfo=plus5)
    result = ensure_utc(dt)
    assert result.tzinfo == timezone.utc
    assert result == datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def test_parse_timestamp_z_suffix():
    result = parse_timestamp("2020-01-01T00:00:00Z")
    assert result is not None
    assert result == datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def test_parse_timestamp_plus_offset():
    result = parse_timestamp("2020-01-01T06:00:00+06:00")
    assert result is not None
    assert result == datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def test_parse_timestamp_invalid_returns_none():
    assert parse_timestamp("not-a-date") is None


def test_parse_timestamp_empty_returns_none():
    assert parse_timestamp("") is None
