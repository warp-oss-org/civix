"""Time and clock primitives shared across the core.

Civix treats time as an injected dependency rather than ambient state.
Adapters, mappers, and any other producer of `fetched_at`-like fields
take a `Clock` so tests can pin timestamps deterministically.

UTC is the only timezone civix accepts on persisted datetimes. Every
model that carries a datetime validates it through `require_utc`.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

Clock = Callable[[], datetime]
"""A zero-argument callable returning the current UTC datetime.

The canonical default is `utc_now`. Tests pass a fixed-value lambda
to make `fetched_at` reproducible.
"""


def utc_now() -> datetime:
    """The default `Clock` implementation: real time, in UTC."""
    return datetime.now(UTC)


def require_utc(value: datetime) -> datetime:
    """Validate that `value` is timezone-aware and offset-equivalent to UTC.

    Returns the value unchanged on success. Raises `ValueError` on
    naive datetimes or non-UTC offsets. Used by pydantic validators
    on every persisted datetime field.
    """
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError("datetime must be timezone-aware and in UTC")
    return value
