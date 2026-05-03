"""Building energy subject identity models."""

from decimal import Decimal
from typing import Annotated

from pydantic import BaseModel, Field, model_validator

from civix.core.identity.models.identifiers import Jurisdiction
from civix.core.provenance.models.provenance import ProvenanceRef
from civix.core.quality.models.fields import MappedField
from civix.core.spatial.models.location import Address, Coordinate
from civix.core.taxonomy.models.category import CategoryRef
from civix.domains.building_energy_emissions.models.common import (
    EMPTY_TUPLE_FIELD_DESCRIPTION,
    FROZEN_MODEL,
    BuildingSubjectKind,
    IdentityCertainty,
    NonEmptyString,
    SourceIdentifier,
)
from civix.domains.building_energy_emissions.models.keys import BuildingEnergySubjectKey

NonNegativeDecimal = Annotated[Decimal, Field(ge=Decimal("0"))]
PositiveYear = Annotated[int, Field(ge=1, le=9999)]


class BuildingEnergySubject(BaseModel):
    """One source-published reporting account, property, building, tax lot,
    campus, or portfolio entity used as the identity anchor for energy
    reports and compliance cases.

    A subject is intentionally separate from a report so that
    multi-building reporting accounts, parent/child campuses, and
    cross-year identity drift can be represented explicitly rather than
    inferred from report rows.
    """

    model_config = FROZEN_MODEL

    provenance: ProvenanceRef
    subject_key: BuildingEnergySubjectKey
    source_subject_identifiers: MappedField[tuple[SourceIdentifier, ...]] = Field(
        description=EMPTY_TUPLE_FIELD_DESCRIPTION
    )
    subject_kind: MappedField[BuildingSubjectKind]
    source_subject_kind: MappedField[CategoryRef]
    identity_certainty: MappedField[IdentityCertainty]
    parent_subject_key: MappedField[BuildingEnergySubjectKey]
    name: MappedField[NonEmptyString]
    jurisdiction: MappedField[Jurisdiction]
    address: MappedField[Address]
    coordinate: MappedField[Coordinate]
    property_types: MappedField[tuple[CategoryRef, ...]] = Field(
        description=EMPTY_TUPLE_FIELD_DESCRIPTION
    )
    floor_area: MappedField[NonNegativeDecimal]
    floor_area_unit: MappedField[CategoryRef]
    year_built: MappedField[PositiveYear]
    occupancy_label: MappedField[CategoryRef]
    ownership_label: MappedField[CategoryRef]
    source_caveats: MappedField[tuple[CategoryRef, ...]] = Field(
        description=EMPTY_TUPLE_FIELD_DESCRIPTION
    )

    @model_validator(mode="after")
    def _validate(self) -> "BuildingEnergySubject":
        parent = self.parent_subject_key.value

        if parent is not None and parent == self.subject_key:
            raise ValueError("parent_subject_key must not equal subject_key")

        return self
