"""Datetime validation helpers."""

from datetime import UTC, datetime


def require_utc(value: datetime) -> datetime:
    """Validate that `value` is timezone-aware and offset-equivalent to UTC."""
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError("datetime must be timezone-aware and in UTC")

    return value
