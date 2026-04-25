from datetime import UTC, datetime, timedelta, timezone

import pytest

from civix.core.temporal import Clock, require_utc, utc_now


class TestUtcNow:
    def test_returns_tz_aware_utc_datetime(self) -> None:
        now = utc_now()
        assert now.tzinfo is not None
        assert now.utcoffset() == timedelta(0)

    def test_satisfies_clock_alias(self) -> None:
        clock: Clock = utc_now
        result = clock()
        assert isinstance(result, datetime)


class TestRequireUtc:
    def test_utc_datetime_passes_through(self) -> None:
        value = datetime(2026, 4, 25, 12, 0, tzinfo=UTC)
        assert require_utc(value) is value

    def test_zero_offset_alias_accepted(self) -> None:
        # `+00:00` is offset-equivalent to UTC even though the tz object differs.
        value = datetime(2026, 4, 25, 12, 0, tzinfo=timezone(timedelta(0)))
        assert require_utc(value) == value

    def test_naive_datetime_rejected(self) -> None:
        with pytest.raises(ValueError, match="UTC"):
            require_utc(datetime(2026, 4, 25, 12, 0))

    def test_non_utc_offset_rejected(self) -> None:
        eastern = timezone(timedelta(hours=-5))
        with pytest.raises(ValueError, match="UTC"):
            require_utc(datetime(2026, 4, 25, 12, 0, tzinfo=eastern))
