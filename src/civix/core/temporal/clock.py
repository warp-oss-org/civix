"""Clock primitives for deterministic timestamp production."""

from collections.abc import Callable
from datetime import UTC, datetime

Clock = Callable[[], datetime]
"""A zero-argument callable returning the current UTC datetime.

The canonical default is `utc_now`. Tests pass a fixed-value lambda to
make `fetched_at` reproducible.
"""


def utc_now() -> datetime:
    """The default `Clock` implementation: real time, in UTC."""
    return datetime.now(UTC)
