"""Geography value models for hazard mitigation records."""

from pydantic import BaseModel, Field

from civix.core.spatial.models.geometry import SpatialFootprint
from civix.core.spatial.models.location import Address
from civix.core.taxonomy.models.category import CategoryRef
from civix.domains.hazard_mitigation.models.common import (
    FROZEN_MODEL,
    MitigationGeographySemantics,
    NonEmptyString,
)


class MitigationProjectGeography(BaseModel):
    """A source-published project geography with explicit semantics.

    Descriptor presence is intentionally left to the wrapping `MappedField`
    quality. A source may leave geography blank, redact it, or provide only
    source-specific labels.
    """

    model_config = FROZEN_MODEL

    semantics: MitigationGeographySemantics
    address: Address | None = None
    footprint: SpatialFootprint | None = None
    place_name: NonEmptyString | None = None
    administrative_areas: tuple[NonEmptyString, ...] = Field(default=())
    source_category: CategoryRef | None = None
