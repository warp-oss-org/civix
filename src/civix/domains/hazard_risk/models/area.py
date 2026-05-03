"""Hazard-risk area models."""

from pydantic import BaseModel, Field

from civix.core.identity.models.identifiers import Jurisdiction
from civix.core.provenance.models.provenance import ProvenanceRef
from civix.core.quality.models.fields import MappedField
from civix.core.spatial.models.geometry import GeometryRef, SpatialFootprint
from civix.core.taxonomy.models.category import CategoryRef
from civix.domains.hazard_risk.models.common import (
    EMPTY_TUPLE_FIELD_DESCRIPTION,
    FROZEN_MODEL,
    HazardRiskAreaKind,
    NonEmptyString,
    SourceIdentifier,
)
from civix.domains.hazard_risk.models.keys import HazardRiskAreaKey


class HazardRiskArea(BaseModel):
    """One source-published area or spatial unit used to carry risk context."""

    model_config = FROZEN_MODEL

    provenance: ProvenanceRef
    area_key: HazardRiskAreaKey
    source_area_identifiers: MappedField[tuple[SourceIdentifier, ...]] = Field(
        description=EMPTY_TUPLE_FIELD_DESCRIPTION
    )
    area_kind: MappedField[HazardRiskAreaKind]
    source_area_kind: MappedField[CategoryRef]
    name: MappedField[NonEmptyString]
    jurisdiction: MappedField[Jurisdiction]
    administrative_areas: MappedField[tuple[NonEmptyString, ...]] = Field(
        description=EMPTY_TUPLE_FIELD_DESCRIPTION
    )
    footprint: MappedField[SpatialFootprint]
    geometry_ref: MappedField[GeometryRef]
    source_hazards: MappedField[tuple[CategoryRef, ...]] = Field(
        description=EMPTY_TUPLE_FIELD_DESCRIPTION
    )
    source_caveats: MappedField[tuple[CategoryRef, ...]] = Field(
        description=EMPTY_TUPLE_FIELD_DESCRIPTION
    )
