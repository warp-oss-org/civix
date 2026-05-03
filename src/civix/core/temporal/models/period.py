"""Source-published temporal period model."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_FROZEN_MODEL = ConfigDict(frozen=True, extra="forbid", strict=True)


class TemporalPeriodPrecision(StrEnum):
    """The amount of time detail published by a source."""

    DATETIME = "datetime"
    DATE = "date"
    DATE_HOUR = "date_hour"
    MONTH = "month"
    YEAR = "year"
    INTERVAL = "interval"


class TemporalTimezoneStatus(StrEnum):
    """How timezone context was represented by the source."""

    UTC = "utc"
    OFFSET = "offset"
    NAMED_LOCAL = "named_local"
    LOCAL_UNSPECIFIED = "local_unspecified"
    UNKNOWN = "unknown"


class TemporalPeriod(BaseModel):
    """Source-published period with precision and timezone context."""

    model_config = _FROZEN_MODEL

    precision: TemporalPeriodPrecision
    datetime_value: datetime | None = None
    date_value: date | None = None
    year_value: Annotated[int | None, Field(ge=1)] = None
    month_value: Annotated[int | None, Field(ge=1, le=12)] = None
    hour_value: Annotated[int | None, Field(ge=0, le=23)] = None
    start_datetime: datetime | None = None
    end_datetime: datetime | None = None
    timezone_status: TemporalTimezoneStatus = TemporalTimezoneStatus.UNKNOWN
    timezone: Annotated[str | None, Field(min_length=1)] = None

    @field_validator("timezone")
    @classmethod
    def _no_surrounding_timezone_whitespace(cls, value: str | None) -> str | None:
        if value is None:
            return None

        if value != value.strip():
            raise ValueError("timezone must not have surrounding whitespace")

        return value

    @model_validator(mode="after")
    def _validate(self) -> TemporalPeriod:
        self._check_precision_shape()
        self._check_timezone_shape()

        return self

    def _check_precision_shape(self) -> None:
        if self.precision is TemporalPeriodPrecision.DATETIME:
            if self.datetime_value is None:
                raise ValueError("datetime precision requires datetime_value")

            self._reject_fields(
                "datetime",
                date_value=self.date_value,
                year_value=self.year_value,
                month_value=self.month_value,
                hour_value=self.hour_value,
                start_datetime=self.start_datetime,
                end_datetime=self.end_datetime,
            )
            return

        if self.precision is TemporalPeriodPrecision.DATE:
            if self.date_value is None:
                raise ValueError("date precision requires date_value")

            self._reject_fields(
                "date",
                datetime_value=self.datetime_value,
                year_value=self.year_value,
                month_value=self.month_value,
                hour_value=self.hour_value,
                start_datetime=self.start_datetime,
                end_datetime=self.end_datetime,
            )
            return

        if self.precision is TemporalPeriodPrecision.DATE_HOUR:
            if self.date_value is None or self.hour_value is None:
                raise ValueError("date_hour precision requires date_value and hour_value")

            self._reject_fields(
                "date_hour",
                datetime_value=self.datetime_value,
                year_value=self.year_value,
                month_value=self.month_value,
                start_datetime=self.start_datetime,
                end_datetime=self.end_datetime,
            )
            return

        if self.precision is TemporalPeriodPrecision.MONTH:
            if self.year_value is None or self.month_value is None:
                raise ValueError("month precision requires year_value and month_value")

            self._reject_fields(
                "month",
                datetime_value=self.datetime_value,
                date_value=self.date_value,
                hour_value=self.hour_value,
                start_datetime=self.start_datetime,
                end_datetime=self.end_datetime,
            )
            return

        if self.precision is TemporalPeriodPrecision.YEAR:
            if self.year_value is None:
                raise ValueError("year precision requires year_value")

            self._reject_fields(
                "year",
                datetime_value=self.datetime_value,
                date_value=self.date_value,
                month_value=self.month_value,
                hour_value=self.hour_value,
                start_datetime=self.start_datetime,
                end_datetime=self.end_datetime,
            )
            return

        if self.start_datetime is None or self.end_datetime is None:
            raise ValueError("interval precision requires start_datetime and end_datetime")

        if self.end_datetime <= self.start_datetime:
            raise ValueError("interval precision requires end_datetime after start_datetime")

        self._reject_fields(
            "interval",
            datetime_value=self.datetime_value,
            date_value=self.date_value,
            year_value=self.year_value,
            month_value=self.month_value,
            hour_value=self.hour_value,
        )

    def _check_timezone_shape(self) -> None:
        if self.timezone_status in {
            TemporalTimezoneStatus.OFFSET,
            TemporalTimezoneStatus.NAMED_LOCAL,
        }:
            if self.timezone is None:
                raise ValueError("timezone status requires timezone metadata")

        if self.timezone_status in {
            TemporalTimezoneStatus.UTC,
            TemporalTimezoneStatus.LOCAL_UNSPECIFIED,
            TemporalTimezoneStatus.UNKNOWN,
        }:
            if self.timezone is not None:
                raise ValueError("timezone metadata requires a specific timezone status")

        for value in self._datetime_values():
            if self.timezone_status is TemporalTimezoneStatus.UTC:
                if value.tzinfo is not None and value.utcoffset() != timedelta(0):
                    raise ValueError("UTC datetime value must have a zero UTC offset")

            if (
                value.tzinfo is not None
                and self.timezone_status is TemporalTimezoneStatus.LOCAL_UNSPECIFIED
            ):
                raise ValueError("timezone-aware datetime cannot be local_unspecified")

    def _datetime_values(self) -> tuple[datetime, ...]:
        return tuple(
            value
            for value in (self.datetime_value, self.start_datetime, self.end_datetime)
            if value is not None
        )

    @staticmethod
    def _reject_fields(precision: str, **fields: object) -> None:
        present = tuple(name for name, value in fields.items() if value is not None)
        if present:
            names = ", ".join(present)
            raise ValueError(f"{precision} precision does not allow {names}")
