"""
Shared datetime helpers.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, List, Optional, Tuple


def ensure_utc(dt: datetime) -> datetime:
    """Return a timezone-aware UTC datetime."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_timestamp(value: str) -> Optional[datetime]:
    """Parse an ISO 8601 timestamp and normalize it to UTC."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return ensure_utc(parsed)


def build_intervals(
    dates: Iterable[datetime],
    start: datetime,
    end: datetime,
) -> List[Tuple[datetime, datetime]]:
    """Build contiguous [start, end) intervals from unique dates."""
    all_dates = set(dates)
    all_dates.add(start)
    all_dates.add(end)
    ordered = sorted(all_dates)
    return list(zip(ordered[:-1], ordered[1:]))
