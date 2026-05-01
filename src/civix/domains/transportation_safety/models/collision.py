"""Traffic collision event models."""

from enum import StrEnum

from pydantic import BaseModel, Field

from civix.core.provenance.models.provenance import ProvenanceRef
from civix.core.quality.models.fields import MappedField
from civix.core.spatial.models.location import Address, Coordinate
from civix.core.taxonomy.models.category import CategoryRef
from civix.domains.transportation_safety.models.common import (
    CONTRIBUTING_FACTORS_DESCRIPTION,
    FROZEN_MODEL,
    NonEmptyString,
    NonNegativeCount,
)
from civix.domains.transportation_safety.models.parties import ContributingFactor
from civix.domains.transportation_safety.models.road import SpeedLimit
from civix.domains.transportation_safety.models.time import OccurrenceTime


class CollisionSeverity(StrEnum):
    """Normalized event-level harm severity."""

    FATAL = "fatal"
    SERIOUS_INJURY = "serious_injury"
    MINOR_INJURY = "minor_injury"
    POSSIBLE_INJURY = "possible_injury"
    PROPERTY_DAMAGE_ONLY = "property_damage_only"
    UNKNOWN = "unknown"


class TrafficCollision(BaseModel):
    """A normalized traffic collision event."""

    model_config = FROZEN_MODEL

    provenance: ProvenanceRef
    collision_id: NonEmptyString
    occurred_at: MappedField[OccurrenceTime]
    severity: MappedField[CollisionSeverity]
    address: MappedField[Address]
    coordinate: MappedField[Coordinate]
    locality: MappedField[NonEmptyString]
    road_names: MappedField[tuple[NonEmptyString, ...]] = Field(
        description=(
            "Ordered source-published road names, such as primary, secondary, then tertiary road."
        )
    )
    intersection_related: MappedField[bool]
    location_description: MappedField[NonEmptyString]
    weather: MappedField[CategoryRef]
    lighting: MappedField[CategoryRef]
    road_surface: MappedField[CategoryRef]
    road_condition: MappedField[CategoryRef]
    traffic_control: MappedField[CategoryRef]
    speed_limit: MappedField[SpeedLimit]
    fatal_count: MappedField[NonNegativeCount]
    serious_injury_count: MappedField[NonNegativeCount]
    minor_injury_count: MappedField[NonNegativeCount]
    possible_injury_count: MappedField[NonNegativeCount]
    uninjured_count: MappedField[NonNegativeCount]
    unknown_injury_count: MappedField[NonNegativeCount]
    total_injured_count: MappedField[NonNegativeCount]
    vehicle_count: MappedField[NonNegativeCount] = Field(
        description="Source-asserted vehicle total; may disagree with linked vehicle records."
    )
    person_count: MappedField[NonNegativeCount] = Field(
        description="Source-asserted person total; may disagree with linked person records."
    )
    contributing_factors: MappedField[tuple[ContributingFactor, ...]] = Field(
        description=CONTRIBUTING_FACTORS_DESCRIPTION
    )
