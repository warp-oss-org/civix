"""Mobility observation site models."""

from pydantic import BaseModel, Field

from civix.core.provenance.models.provenance import ProvenanceRef
from civix.core.quality.models.fields import MappedField
from civix.core.spatial.models.geometry import SpatialFootprint
from civix.core.spatial.models.location import Address
from civix.core.taxonomy.models.category import CategoryRef
from civix.core.temporal import TemporalPeriod
from civix.domains.mobility_observations.models.common import (
    EMPTY_TUPLE_FIELD_DESCRIPTION,
    FROZEN_MODEL,
    MeasurementMethod,
    MobilitySiteKind,
    MovementType,
    NonEmptyString,
    ObservationDirection,
)


class MobilityObservationSite(BaseModel):
    """A counter, road segment, region, or other mobility observation location.

    Direction, movement, and method are site-level defaults or context.
    Observation records may carry more specific row-level values.
    """

    model_config = FROZEN_MODEL

    provenance: ProvenanceRef
    site_id: NonEmptyString
    kind: MappedField[MobilitySiteKind]
    footprint: MappedField[SpatialFootprint]
    address: MappedField[Address]
    road_names: MappedField[tuple[NonEmptyString, ...]] = Field(
        description=EMPTY_TUPLE_FIELD_DESCRIPTION
    )
    direction: MappedField[ObservationDirection]
    movement_type: MappedField[MovementType]
    active_period: MappedField[TemporalPeriod]
    measurement_method: MappedField[MeasurementMethod]
    source_caveats: MappedField[tuple[CategoryRef, ...]] = Field(
        description=EMPTY_TUPLE_FIELD_DESCRIPTION
    )
