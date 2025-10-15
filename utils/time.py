"""Time-related helpers for AIS-RF fusion."""
from __future__ import annotations

from datetime import datetime, timezone


def to_utc(dt: datetime) -> datetime:
    """Ensure datetime is timezone-aware and converted to UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
