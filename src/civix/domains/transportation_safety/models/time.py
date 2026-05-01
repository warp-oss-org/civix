"""Occurrence time models for transportation safety records."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, Field, field_validator, model_validator

from civix.domains.transportation_safety.models.common import FROZEN_MODEL


class OccurrenceTimePrecision(StrEnum):
    """The amount of occurrence time detail published by a source."""

    DATETIME = "datetime"
    DATE = "date"
    DATE_HOUR = "date_hour"
    YEAR = "year"


class OccurrenceTimezoneStatus(StrEnum):
    """How timezone context was represented by the source."""

    UTC = "utc"
    OFFSET = "offset"
    NAMED_LOCAL = "named_local"
    LOCAL_UNSPECIFIED = "local_unspecified"
    UNKNOWN = "unknown"


class OccurrenceTime(BaseModel):
    """Source-published occurrence time with precision and timezone context."""

    model_config = FROZEN_MODEL

    precision: OccurrenceTimePrecision
    datetime_value: datetime | None = None
    date_value: date | None = None
    year_value: Annotated[int | None, Field(ge=1)] = None
    hour_value: Annotated[int | None, Field(ge=0, le=23)] = None
    timezone_status: OccurrenceTimezoneStatus = OccurrenceTimezoneStatus.UNKNOWN
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
    def _validate(self) -> OccurrenceTime:
        self._check_precision_shape()
        self._check_timezone_shape()

        return self

    def _check_precision_shape(self) -> None:
        if self.precision is OccurrenceTimePrecision.DATETIME:
            if self.datetime_value is None:
                raise ValueError("datetime precision requires datetime_value")

            self._reject_fields("datetime", date_value=self.date_value, year_value=self.year_value)
            self._reject_fields("datetime", hour_value=self.hour_value)
            return

        if self.precision is OccurrenceTimePrecision.DATE:
            if self.date_value is None:
                raise ValueError("date precision requires date_value")

            self._reject_fields(
                "date",
                datetime_value=self.datetime_value,
                year_value=self.year_value,
                hour_value=self.hour_value,
            )
            return

        if self.precision is OccurrenceTimePrecision.DATE_HOUR:
            if self.date_value is None or self.hour_value is None:
                raise ValueError("date_hour precision requires date_value and hour_value")

            self._reject_fields(
                "date_hour",
                datetime_value=self.datetime_value,
                year_value=self.year_value,
            )
            return

        if self.year_value is None:
            raise ValueError("year precision requires year_value")

        self._reject_fields(
            "year",
            datetime_value=self.datetime_value,
            date_value=self.date_value,
            hour_value=self.hour_value,
        )

    def _check_timezone_shape(self) -> None:
        if self.timezone_status in {
            OccurrenceTimezoneStatus.OFFSET,
            OccurrenceTimezoneStatus.NAMED_LOCAL,
        }:
            if self.timezone is None:
                raise ValueError("timezone status requires timezone metadata")

        if self.timezone_status in {
            OccurrenceTimezoneStatus.UTC,
            OccurrenceTimezoneStatus.LOCAL_UNSPECIFIED,
            OccurrenceTimezoneStatus.UNKNOWN,
        }:
            if self.timezone is not None:
                raise ValueError("timezone metadata requires a specific timezone status")

        if self.timezone_status is OccurrenceTimezoneStatus.UTC:
            if (
                self.datetime_value is not None
                and self.datetime_value.tzinfo is not None
                and self.datetime_value.utcoffset() != timedelta(0)
            ):
                raise ValueError("UTC datetime_value must have a zero UTC offset")

        if (
            self.datetime_value is not None
            and self.datetime_value.tzinfo is not None
            and self.timezone_status is OccurrenceTimezoneStatus.LOCAL_UNSPECIFIED
        ):
            raise ValueError("timezone-aware datetime cannot be local_unspecified")

    @staticmethod
    def _reject_fields(precision: str, **fields: object) -> None:
        present = tuple(name for name, value in fields.items() if value is not None)
        if present:
            names = ", ".join(present)
            raise ValueError(f"{precision} precision does not allow {names}")
