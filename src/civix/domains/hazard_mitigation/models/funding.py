"""Funding value models for hazard mitigation records."""

from decimal import Decimal

from pydantic import BaseModel, field_validator

from civix.core.taxonomy.models.category import CategoryRef
from civix.domains.hazard_mitigation.models.common import (
    FROZEN_MODEL,
    CurrencyCode,
    MitigationFundingAmountKind,
    MitigationFundingEventType,
    MitigationFundingShareKind,
)


class MitigationMoneyAmount(BaseModel):
    """A source-published signed money amount."""

    model_config = FROZEN_MODEL

    amount: Decimal
    currency: CurrencyCode

    @field_validator("amount")
    @classmethod
    def _finite_amount(cls, value: Decimal) -> Decimal:
        if not value.is_finite():
            raise ValueError("amount must be finite")

        return value


class MitigationFundingAmount(BaseModel):
    """One typed amount component in a project summary or transaction row.

    `amount_kind` is the cost bucket. `lifecycle` carries obligation,
    payment, refund, reversal, and similar timing/event semantics when the
    source publishes them at amount-component grain.
    """

    model_config = FROZEN_MODEL

    money: MitigationMoneyAmount
    amount_kind: MitigationFundingAmountKind
    share_kind: MitigationFundingShareKind = MitigationFundingShareKind.UNKNOWN
    lifecycle: MitigationFundingEventType | None = None
    source_category: CategoryRef | None = None
