"""Hazard mitigation funding transaction models."""

from pydantic import BaseModel, Field, field_validator

from civix.core.provenance.models.provenance import ProvenanceRef
from civix.core.quality.models.fields import MappedField
from civix.core.taxonomy.models.category import CategoryRef
from civix.core.temporal import TemporalPeriod
from civix.domains.hazard_mitigation.models.common import (
    EMPTY_TUPLE_FIELD_DESCRIPTION,
    FROZEN_MODEL,
    MitigationFundingEventType,
    NonEmptyString,
)
from civix.domains.hazard_mitigation.models.funding import MitigationFundingAmount


class MitigationFundingTransaction(BaseModel):
    """One true source-published mitigation funding transaction.

    `transaction_id` is the stable, source-published transaction identifier.
    Do not populate it from unstable refresh identifiers such as OpenFEMA `id`.
    """

    model_config = FROZEN_MODEL

    provenance: ProvenanceRef
    transaction_id: NonEmptyString
    project_id: MappedField[NonEmptyString]
    transaction_period: MappedField[TemporalPeriod]
    fiscal_period: MappedField[TemporalPeriod]
    funding_programme: MappedField[CategoryRef]
    event_type: MappedField[MitigationFundingEventType]
    source_event_category: MappedField[CategoryRef]
    amount_components: MappedField[tuple[MitigationFundingAmount, ...]] = Field(
        description=EMPTY_TUPLE_FIELD_DESCRIPTION
    )
    source_caveats: MappedField[tuple[CategoryRef, ...]] = Field(
        description=EMPTY_TUPLE_FIELD_DESCRIPTION
    )

    @field_validator("transaction_id")
    @classmethod
    def _transaction_id_has_content(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("transaction_id must not have surrounding whitespace")

        return value
