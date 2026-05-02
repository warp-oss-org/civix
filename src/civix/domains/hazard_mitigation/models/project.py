"""Hazard mitigation project models."""

from decimal import Decimal

from pydantic import BaseModel, Field, field_validator

from civix.core.provenance.models.provenance import ProvenanceRef
from civix.core.quality.models.fields import MappedField
from civix.core.taxonomy.models.category import CategoryRef
from civix.core.temporal import TemporalPeriod
from civix.domains.hazard_mitigation.models.common import (
    EMPTY_TUPLE_FIELD_DESCRIPTION,
    FROZEN_MODEL,
    MitigationHazardType,
    MitigationInterventionType,
    MitigationProjectStatus,
    NonEmptyString,
)
from civix.domains.hazard_mitigation.models.funding import (
    MitigationFundingAmount,
    MitigationMoneyAmount,
)
from civix.domains.hazard_mitigation.models.geography import MitigationProjectGeography
from civix.domains.hazard_mitigation.models.organization import MitigationOrganization


class HazardMitigationProject(BaseModel):
    """One source-published funded hazard mitigation or resilience project.

    Project summary amounts must not be emitted as `MitigationFundingTransaction`
    rows. If a source publishes both initial and current approval dates, map the
    current/effective approval date here and preserve initial approval as a source
    category or caveat until a second source proves a portable field.
    """

    model_config = FROZEN_MODEL

    provenance: ProvenanceRef
    project_id: NonEmptyString
    title: MappedField[NonEmptyString]
    description: MappedField[NonEmptyString]
    programme: MappedField[CategoryRef]
    organizations: MappedField[tuple[MitigationOrganization, ...]] = Field(
        description=EMPTY_TUPLE_FIELD_DESCRIPTION
    )
    hazard_types: MappedField[tuple[MitigationHazardType, ...]] = Field(
        description=EMPTY_TUPLE_FIELD_DESCRIPTION
    )
    source_hazards: MappedField[tuple[CategoryRef, ...]] = Field(
        description=EMPTY_TUPLE_FIELD_DESCRIPTION
    )
    intervention_types: MappedField[tuple[MitigationInterventionType, ...]] = Field(
        description=EMPTY_TUPLE_FIELD_DESCRIPTION
    )
    source_interventions: MappedField[tuple[CategoryRef, ...]] = Field(
        description=EMPTY_TUPLE_FIELD_DESCRIPTION
    )
    status: MappedField[MitigationProjectStatus]
    source_status: MappedField[CategoryRef]
    approval_period: MappedField[TemporalPeriod]
    project_period: MappedField[TemporalPeriod]
    fiscal_period: MappedField[TemporalPeriod]
    publication_period: MappedField[TemporalPeriod]
    geography: MappedField[tuple[MitigationProjectGeography, ...]] = Field(
        description=EMPTY_TUPLE_FIELD_DESCRIPTION
    )
    funding_summaries: MappedField[tuple[MitigationFundingAmount, ...]] = Field(
        description=(
            f"{EMPTY_TUPLE_FIELD_DESCRIPTION} One field quality covers the whole tuple; "
            "if entries come from source fields with different qualities, the mapper must "
            "choose the least precise applicable tuple quality and preserve component "
            "semantics on each amount."
        )
    )
    benefit_cost_ratio: MappedField[Decimal]
    net_benefits: MappedField[MitigationMoneyAmount]
    source_caveats: MappedField[tuple[CategoryRef, ...]] = Field(
        description=EMPTY_TUPLE_FIELD_DESCRIPTION
    )

    @field_validator("project_id")
    @classmethod
    def _project_id_has_content(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("project_id must not have surrounding whitespace")

        return value
