"""Hazard-risk score fact models."""

from decimal import Decimal
from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from civix.core.provenance.models.provenance import ProvenanceRef
from civix.core.quality.models.fields import MappedField
from civix.core.taxonomy.models.category import CategoryRef
from civix.core.temporal import TemporalPeriod
from civix.domains.hazard_risk.models.common import (
    EMPTY_TUPLE_FIELD_DESCRIPTION,
    FROZEN_MODEL,
    HazardRiskHazardType,
    HazardRiskScoreDirection,
    HazardRiskScoreType,
    NonEmptyString,
)
from civix.domains.hazard_risk.models.keys import HazardRiskAreaKey


class NumericScoreMeasure(BaseModel):
    """A numeric score, value, rank, percentile, or other metric fact."""

    model_config = FROZEN_MODEL

    kind: Literal["numeric"] = "numeric"
    value: Decimal


class CategoryScoreMeasure(BaseModel):
    """A source-published score category or rating."""

    model_config = FROZEN_MODEL

    kind: Literal["category"] = "category"
    value: CategoryRef


class TextScoreMeasure(BaseModel):
    """A source-specific score value that is not taxonomy-backed yet.

    Prefer `CategoryScoreMeasure` when a stable source taxonomy reference
    can be constructed. This variant is for values that must be preserved
    before such a taxonomy exists.
    """

    model_config = FROZEN_MODEL

    kind: Literal["text"] = "text"
    value: NonEmptyString

    @field_validator("value")
    @classmethod
    def _no_surrounding_whitespace(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("text score measure must not have surrounding whitespace")

        return value


ScoreMeasure = Annotated[
    NumericScoreMeasure | CategoryScoreMeasure | TextScoreMeasure,
    Field(discriminator="kind"),
]


class ScoreScale(BaseModel):
    """A numeric score scale when the source publishes bounded semantics."""

    model_config = FROZEN_MODEL

    minimum: Decimal
    maximum: Decimal

    @model_validator(mode="after")
    def _validate(self) -> "ScoreScale":
        if self.maximum < self.minimum:
            raise ValueError("score scale maximum must be greater than or equal to minimum")

        return self


class HazardRiskScore(BaseModel):
    """One source-published score, rating, index, percentile, rank, or metric fact.

    `score_id` is a mapper-supplied deterministic identifier scoped to
    the emitted score fact. It is not a cross-source join key; records
    link to their parent risk area through `area_key`.
    """

    model_config = FROZEN_MODEL

    provenance: ProvenanceRef
    score_id: NonEmptyString
    area_key: HazardRiskAreaKey
    hazard_type: MappedField[HazardRiskHazardType]
    source_hazard: MappedField[CategoryRef]
    score_type: MappedField[HazardRiskScoreType]
    source_score_type: MappedField[CategoryRef]
    score_measure: MappedField[ScoreMeasure]
    score_unit: MappedField[CategoryRef]
    score_scale: MappedField[ScoreScale]
    score_direction: MappedField[HazardRiskScoreDirection]
    methodology_label: MappedField[NonEmptyString]
    methodology_version: MappedField[NonEmptyString]
    methodology_url: MappedField[NonEmptyString]
    publication_vintage: MappedField[TemporalPeriod]
    effective_period: MappedField[TemporalPeriod]
    source_caveats: MappedField[tuple[CategoryRef, ...]] = Field(
        description=EMPTY_TUPLE_FIELD_DESCRIPTION
    )

    @field_validator("score_id")
    @classmethod
    def _score_id_has_content(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("score_id must not have surrounding whitespace")

        return value

    @model_validator(mode="after")
    def _validate(self) -> "HazardRiskScore":
        measure = self.score_measure.value
        scale = self.score_scale.value

        if isinstance(measure, NumericScoreMeasure) and scale is not None:
            if measure.value < scale.minimum or measure.value > scale.maximum:
                raise ValueError("numeric score measure must be within score scale")

        return self
