"""Collision vehicle models."""

from enum import StrEnum

from pydantic import BaseModel, Field

from civix.core.provenance.models.provenance import ProvenanceRef
from civix.core.quality.models.fields import MappedField
from civix.core.taxonomy.models.category import CategoryRef
from civix.domains.transportation_safety.models.common import (
    CONTRIBUTING_FACTORS_DESCRIPTION,
    FROZEN_MODEL,
    NonEmptyString,
    NonNegativeCount,
)
from civix.domains.transportation_safety.models.parties import (
    ContributingFactor,
    RoadUserRole,
)


class VehicleCategory(StrEnum):
    """Normalized vehicle, conveyance, or source-unit category."""

    PASSENGER_CAR = "passenger_car"
    TRUCK = "truck"
    BUS = "bus"
    MOTORCYCLE = "motorcycle"
    BICYCLE = "bicycle"
    MICROMOBILITY = "micromobility"
    EMERGENCY_VEHICLE = "emergency_vehicle"
    PEDESTRIAN_UNIT = "pedestrian_unit"
    OTHER = "other"
    UNKNOWN = "unknown"


class CollisionVehicle(BaseModel):
    """One involved vehicle, conveyance, or source unit."""

    model_config = FROZEN_MODEL

    provenance: ProvenanceRef
    collision_id: NonEmptyString
    vehicle_id: NonEmptyString
    category: MappedField[VehicleCategory]
    road_user_role: MappedField[RoadUserRole]
    occupant_count: MappedField[NonNegativeCount]
    travel_direction: MappedField[CategoryRef]
    maneuver: MappedField[CategoryRef]
    damage: MappedField[CategoryRef]
    contributing_factors: MappedField[tuple[ContributingFactor, ...]] = Field(
        description=CONTRIBUTING_FACTORS_DESCRIPTION
    )
