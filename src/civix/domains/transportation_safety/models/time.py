"""Occurrence time compatibility imports for transportation safety records."""

from civix.core.temporal import TemporalPeriod, TemporalPeriodPrecision, TemporalTimezoneStatus

OccurrenceTime = TemporalPeriod
OccurrenceTimePrecision = TemporalPeriodPrecision
OccurrenceTimezoneStatus = TemporalTimezoneStatus

__all__ = [
    "OccurrenceTime",
    "OccurrenceTimePrecision",
    "OccurrenceTimezoneStatus",
]
