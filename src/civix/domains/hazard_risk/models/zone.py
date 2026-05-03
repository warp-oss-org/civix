"""Hazard-risk regulatory zone models."""

from pydantic import BaseModel, Field

from civix.core.provenance.models.provenance import ProvenanceRef
from civix.core.quality.models.fields import MappedField
from civix.core.spatial.models.geometry import GeometryRef, SpatialFootprint
from civix.core.taxonomy.models.category import CategoryRef
from civix.core.temporal import TemporalPeriod
from civix.domains.hazard_risk.models.common import (
    EMPTY_TUPLE_FIELD_DESCRIPTION,
    FROZEN_MODEL,
    HazardRiskHazardType,
    HazardRiskZoneStatus,
    NonEmptyString,
    SourceIdentifier,
)
from civix.domains.hazard_risk.models.keys import HazardRiskZoneKey


class HazardRiskZone(BaseModel):
    """One source-published regulatory or classified hazard zone."""

    model_config = FROZEN_MODEL

    provenance: ProvenanceRef
    zone_key: HazardRiskZoneKey
    source_zone_identifiers: MappedField[tuple[SourceIdentifier, ...]] = Field(
        description=EMPTY_TUPLE_FIELD_DESCRIPTION
    )
    hazard_type: MappedField[HazardRiskHazardType]
    source_hazard: MappedField[CategoryRef]
    source_zone: MappedField[CategoryRef]
    status: MappedField[HazardRiskZoneStatus]
    source_status: MappedField[CategoryRef]
    plan_identifier: MappedField[NonEmptyString]
    plan_name: MappedField[NonEmptyString]
    effective_period: MappedField[TemporalPeriod]
    footprint: MappedField[SpatialFootprint]
    geometry_ref: MappedField[GeometryRef]
    source_caveats: MappedField[tuple[CategoryRef, ...]] = Field(
        description=EMPTY_TUPLE_FIELD_DESCRIPTION
    )
