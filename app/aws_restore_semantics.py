"""AWS S3 archive-restore time semantics shared by Fujin backends."""

from __future__ import annotations

from datetime import datetime, time, timedelta, timezone


def utc_datetime(value: datetime) -> datetime:
    """Return an aware UTC datetime; persisted naive values are interpreted as UTC."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def s3_restore_expiry(completed_at: datetime, retention_days: int) -> datetime:
    """Return S3's temporary-copy expiry: completion + days, next UTC midnight.

    S3 Glacier Flexible Retrieval and S3 Glacier Deep Archive round the
    resulting timestamp forward to the following day at 00:00 UTC.
    """
    days = int(retention_days)
    if days < 1:
        raise ValueError("S3 restore retention must be at least one day")
    candidate = utc_datetime(completed_at) + timedelta(days=days)
    return datetime.combine(candidate.date() + timedelta(days=1), time.min, tzinfo=timezone.utc)
