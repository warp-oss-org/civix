"""Collision person models."""

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


class InjuryOutcome(StrEnum):
    """Normalized person-level injury outcome."""

    FATAL = "fatal"
    SERIOUS = "serious"
    MINOR = "minor"
    POSSIBLE = "possible"
    UNINJURED = "uninjured"
    UNKNOWN = "unknown"


class CollisionPerson(BaseModel):
    """One involved person, casualty, or road user.

    Demographic fields are deferred pending a handling policy; do not
    add them ad hoc.
    """

    model_config = FROZEN_MODEL

    provenance: ProvenanceRef
    collision_id: NonEmptyString
    person_id: NonEmptyString
    vehicle_id: NonEmptyString | None = None
    role: MappedField[RoadUserRole]
    injury_outcome: MappedField[InjuryOutcome]
    age: MappedField[NonNegativeCount]
    safety_equipment: MappedField[CategoryRef]
    position_in_vehicle: MappedField[CategoryRef]
    ejection: MappedField[CategoryRef]
    contributing_factors: MappedField[tuple[ContributingFactor, ...]] = Field(
        description=CONTRIBUTING_FACTORS_DESCRIPTION
    )
