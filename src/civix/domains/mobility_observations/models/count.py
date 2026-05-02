"""Mobility count and volume observation models."""

from pydantic import BaseModel, Field

from civix.core.provenance.models.provenance import ProvenanceRef
from civix.core.quality.models.fields import MappedField
from civix.core.taxonomy.models.category import CategoryRef
from civix.core.temporal import TemporalPeriod
from civix.domains.mobility_observations.models.common import (
    EMPTY_TUPLE_FIELD_DESCRIPTION,
    FROZEN_MODEL,
    AggregationWindow,
    CountMetricType,
    CountUnit,
    MeasurementMethod,
    MovementType,
    NonEmptyString,
    NonNegativeDecimal,
    ObservationDirection,
    TravelMode,
)


class MobilityCountObservation(BaseModel):
    """One source-published count or volume value.

    Counts use `Decimal` to avoid float drift for published volumes,
    AADT/AADF/TMJA values, and future mobility metrics.
    Row-level direction, movement, and method override site-level
    defaults when both are available.
    """

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
    metric_type: MappedField[CountMetricType]
    unit: MappedField[CountUnit]
    value: MappedField[NonNegativeDecimal]
    source_caveats: MappedField[tuple[CategoryRef, ...]] = Field(
        description=EMPTY_TUPLE_FIELD_DESCRIPTION
    )
