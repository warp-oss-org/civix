"""Shared mobility observation model primitives."""

from decimal import Decimal
from enum import StrEnum
from typing import Annotated

from pydantic import ConfigDict, Field

FROZEN_MODEL = ConfigDict(frozen=True, extra="forbid", strict=True)
EMPTY_TUPLE_FIELD_DESCRIPTION = (
    "UNMAPPED means no source field or mapper support, NOT_PROVIDED means source fields "
    "exist but are blank, and an empty tuple with provided quality means the source explicitly "
    "reported no values."
)

NonEmptyString = Annotated[str, Field(min_length=1)]
NonNegativeDecimal = Annotated[Decimal, Field(ge=Decimal("0"))]


class MobilitySiteKind(StrEnum):
    """Normalized kind of mobility observation location."""

    COUNTER = "counter"
    SCREENLINE = "screenline"
    INTERSECTION = "intersection"
    BRIDGE_CROSSING = "bridge_crossing"
    ROAD_SEGMENT = "road_segment"
    REGION = "region"
    TRAFFIC_COUNT_POINT = "traffic_count_point"
    SOURCE_SPECIFIC = "source_specific"


class TravelMode(StrEnum):
    """Normalized travel mode for a mobility observation."""

    VEHICLE = "vehicle"
    PASSENGER_CAR = "passenger_car"
    TRUCK = "truck"
    BUS = "bus"
    BICYCLE = "bicycle"
    PEDESTRIAN = "pedestrian"
    MICROMOBILITY = "micromobility"
    MIXED_TRAFFIC = "mixed_traffic"
    OTHER = "other"


class ObservationDirection(StrEnum):
    """Normalized direction for a mobility observation."""

    INBOUND = "inbound"
    OUTBOUND = "outbound"
    NORTHBOUND = "northbound"
    SOUTHBOUND = "southbound"
    EASTBOUND = "eastbound"
    WESTBOUND = "westbound"
    BIDIRECTIONAL = "bidirectional"
    CLOCKWISE = "clockwise"
    COUNTERCLOCKWISE = "counterclockwise"
    SOURCE_SPECIFIC = "source_specific"


class MovementType(StrEnum):
    """Normalized movement represented by a mobility observation."""

    THROUGH = "through"
    LEFT_TURN = "left_turn"
    RIGHT_TURN = "right_turn"
    U_TURN = "u_turn"
    CROSSING = "crossing"
    ENTERING = "entering"
    EXITING = "exiting"
    ALL_MOVEMENTS = "all_movements"
    SOURCE_SPECIFIC = "source_specific"


class MeasurementMethod(StrEnum):
    """Normalized method used to produce a mobility observation."""

    AUTOMATED_COUNTER = "automated_counter"
    MANUAL_COUNT = "manual_count"
    SENSOR_FEED = "sensor_feed"
    MODELED_ESTIMATE = "modeled_estimate"
    BUS_GPS_ESTIMATE = "bus_gps_estimate"
    ANNUALIZED_ESTIMATE = "annualized_estimate"
    OTHER = "other"


class AggregationWindow(StrEnum):
    """Normalized aggregation window for a mobility observation."""

    RAW_INTERVAL = "raw_interval"
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKDAY_WEEKEND = "weekday_weekend"
    MONTHLY = "monthly"
    ANNUAL = "annual"
    ANNUAL_AVERAGE_DAILY = "annual_average_daily"
    SOURCE_SPECIFIC = "source_specific"


class CountMetricType(StrEnum):
    """Normalized count or volume metric type."""

    RAW_COUNT = "raw_count"
    ESTIMATED_COUNT = "estimated_count"
    ANNUALIZED_VOLUME = "annualized_volume"
    AADT = "aadt"
    AADF = "aadf"
    TMJA = "tmja"
    SOURCE_SPECIFIC = "source_specific"


class CountUnit(StrEnum):
    """Normalized unit for a count or volume value."""

    COUNT = "count"
    VEHICLES_PER_PERIOD = "vehicles_per_period"
    VEHICLES_PER_DAY = "vehicles_per_day"
    PERSONS_PER_PERIOD = "persons_per_period"
    SOURCE_SPECIFIC = "source_specific"


class SpeedMetricType(StrEnum):
    """Normalized speed-observation metric type."""

    OBSERVED_SPEED = "observed_speed"
    TRAVEL_TIME = "travel_time"
    SOURCE_SPECIFIC = "source_specific"


class SpeedUnit(StrEnum):
    """Normalized unit for speed-observation metric values."""

    MILES_PER_HOUR = "miles_per_hour"
    SECONDS = "seconds"
    SOURCE_SPECIFIC = "source_specific"
