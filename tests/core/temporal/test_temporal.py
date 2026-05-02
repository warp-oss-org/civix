from datetime import UTC, date, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from civix.core.temporal import (
    Clock,
    TemporalPeriod,
    TemporalPeriodPrecision,
    TemporalTimezoneStatus,
    require_utc,
    utc_now,
)


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


class TestTemporalPeriod:
    def test_datetime_precision(self) -> None:
        period = TemporalPeriod(
            precision=TemporalPeriodPrecision.DATETIME,
            datetime_value=datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
            timezone_status=TemporalTimezoneStatus.UTC,
        )

        assert period.datetime_value == datetime(2026, 5, 1, 12, 0, tzinfo=UTC)

    def test_date_hour_precision(self) -> None:
        period = TemporalPeriod(
            precision=TemporalPeriodPrecision.DATE_HOUR,
            date_value=date(2026, 5, 1),
            hour_value=8,
            timezone_status=TemporalTimezoneStatus.NAMED_LOCAL,
            timezone="America/New_York",
        )

        assert period.hour_value == 8

    def test_month_precision(self) -> None:
        period = TemporalPeriod(
            precision=TemporalPeriodPrecision.MONTH,
            year_value=2026,
            month_value=5,
            timezone_status=TemporalTimezoneStatus.UNKNOWN,
        )

        assert period.month_value == 5

    def test_interval_precision(self) -> None:
        period = TemporalPeriod(
            precision=TemporalPeriodPrecision.INTERVAL,
            start_datetime=datetime(2026, 5, 1, 8, 0),
            end_datetime=datetime(2026, 5, 1, 8, 15),
            timezone_status=TemporalTimezoneStatus.NAMED_LOCAL,
            timezone="America/Toronto",
        )

        assert period.end_datetime == datetime(2026, 5, 1, 8, 15)

    def test_interval_end_must_follow_start(self) -> None:
        with pytest.raises(ValidationError, match="after start"):
            TemporalPeriod(
                precision=TemporalPeriodPrecision.INTERVAL,
                start_datetime=datetime(2026, 5, 1, 8, 15),
                end_datetime=datetime(2026, 5, 1, 8, 0),
            )

    def test_precision_rejects_extra_shape_fields(self) -> None:
        with pytest.raises(ValidationError, match="date precision"):
            TemporalPeriod(
                precision=TemporalPeriodPrecision.DATE,
                date_value=date(2026, 5, 1),
                hour_value=8,
            )

    def test_named_timezone_requires_timezone_metadata(self) -> None:
        with pytest.raises(ValidationError, match="timezone metadata"):
            TemporalPeriod(
                precision=TemporalPeriodPrecision.DATE,
                date_value=date(2026, 5, 1),
                timezone_status=TemporalTimezoneStatus.NAMED_LOCAL,
            )

    def test_utc_timezone_requires_zero_offset(self) -> None:
        eastern = timezone(timedelta(hours=-5))

        with pytest.raises(ValidationError, match="zero UTC offset"):
            TemporalPeriod(
                precision=TemporalPeriodPrecision.DATETIME,
                datetime_value=datetime(2026, 5, 1, 12, 0, tzinfo=eastern),
                timezone_status=TemporalTimezoneStatus.UTC,
            )

    def test_local_unspecified_rejects_timezone_aware_datetime(self) -> None:
        with pytest.raises(ValidationError, match="local_unspecified"):
            TemporalPeriod(
                precision=TemporalPeriodPrecision.DATETIME,
                datetime_value=datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
                timezone_status=TemporalTimezoneStatus.LOCAL_UNSPECIFIED,
            )
