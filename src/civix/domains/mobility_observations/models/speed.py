"""Mobility speed and travel-time observation models."""

from typing import Annotated

from pydantic import BaseModel, Field

from civix.core.provenance.models.provenance import ProvenanceRef
from civix.core.quality.models.fields import MappedField
from civix.core.taxonomy.models.category import CategoryRef
from civix.core.temporal import TemporalPeriod
from civix.domains.mobility_observations.models.common import (
    EMPTY_TUPLE_FIELD_DESCRIPTION,
    FROZEN_MODEL,
    AggregationWindow,
    MeasurementMethod,
    MovementType,
    NonEmptyString,
    NonNegativeDecimal,
    ObservationDirection,
    SpeedMetricType,
    SpeedUnit,
    TravelMode,
)


class MobilitySpeedMetric(BaseModel):
    """One metric value carried by a speed observation row."""

    model_config = FROZEN_MODEL

    metric_type: MappedField[SpeedMetricType]
    unit: MappedField[SpeedUnit]
    value: MappedField[NonNegativeDecimal]


class MobilitySpeedObservation(BaseModel):
    """One source-published speed, travel-time, or related movement metric row."""

    model_config = FROZEN_MODEL

    provenance: ProvenanceRef
    observation_id: NonEmptyString
    site_id: NonEmptyString
    period: MappedField[TemporalPeriod]
    travel_mode: MappedField[TravelMode]
    direction: MappedField[ObservationDirection]
    movement_type: MappedField[MovementType]
    measurement_method: MappedField[MeasurementMethod]
    aggregation_window: MappedField[AggregationWindow]
    metrics: Annotated[tuple[MobilitySpeedMetric, ...], Field(min_length=1)]
    source_caveats: MappedField[tuple[CategoryRef, ...]] = Field(
        description=EMPTY_TUPLE_FIELD_DESCRIPTION
    )
