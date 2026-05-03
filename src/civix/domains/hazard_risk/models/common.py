"""Shared hazard-risk model primitives."""

from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator

from civix.core.taxonomy.models.category import CategoryRef

FROZEN_MODEL = ConfigDict(frozen=True, extra="forbid", strict=True)
EMPTY_TUPLE_FIELD_DESCRIPTION = (
    "UNMAPPED means no source field or mapper support, NOT_PROVIDED means source fields "
    "exist but are blank, and an empty tuple with provided quality means the source explicitly "
    "reported no values."
)

NonEmptyString = Annotated[str, Field(min_length=1)]


class HazardRiskAreaKind(StrEnum):
    """Portable kinds of source-published hazard-risk areas."""

    ADMINISTRATIVE_AREA = "administrative_area"
    CENSUS_UNIT = "census_unit"
    RISK_INDEX_AREA = "risk_index_area"
    PLAN_AREA = "plan_area"
    SOURCE_SPECIFIC = "source_specific"
    UNKNOWN = "unknown"


class HazardRiskHazardType(StrEnum):
    """Portable hazard or risk source described by hazard-risk records.

    Some members overlap in plain language. Mappers should preserve the
    source's published hazard grain instead of collapsing distinct source
    hazards into a broader label.
    """

    FLOOD = "flood"
    COASTAL = "coastal"
    WILDFIRE = "wildfire"
    HEAT = "heat"
    DROUGHT = "drought"
    EARTHQUAKE = "earthquake"
    LANDSLIDE = "landslide"
    WIND = "wind"
    STORM = "storm"
    WINTER_WEATHER = "winter_weather"
    MULTI_HAZARD = "multi_hazard"
    SOURCE_SPECIFIC = "source_specific"
    UNKNOWN = "unknown"


class HazardRiskScoreType(StrEnum):
    """Portable kinds of score-like facts."""

    COMPOSITE_INDEX = "composite_index"
    EXPECTED_ANNUAL_LOSS = "expected_annual_loss"
    SOCIAL_VULNERABILITY = "social_vulnerability"
    COMMUNITY_RESILIENCE = "community_resilience"
    PER_HAZARD_SCORE = "per_hazard_score"
    PERCENTILE = "percentile"
    RANK = "rank"
    RATING = "rating"
    SOURCE_SPECIFIC = "source_specific"
    UNKNOWN = "unknown"


class HazardRiskScoreDirection(StrEnum):
    """How a numeric score should be interpreted."""

    HIGHER_IS_HIGHER_RISK = "higher_is_higher_risk"
    LOWER_IS_HIGHER_RISK = "lower_is_higher_risk"
    HIGHER_IS_BETTER = "higher_is_better"
    LOWER_IS_BETTER = "lower_is_better"
    NOT_APPLICABLE = "not_applicable"
    UNKNOWN = "unknown"


class HazardRiskZoneStatus(StrEnum):
    """Portable lifecycle or regulatory state for a hazard-risk zone."""

    EFFECTIVE = "effective"
    PRELIMINARY = "preliminary"
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    APPROVED = "approved"
    CANCELLED = "cancelled"
    ABROGATED = "abrogated"
    SOURCE_SPECIFIC = "source_specific"
    UNKNOWN = "unknown"


class SourceIdentifier(BaseModel):
    """A source-published identifier preserved separately from Civix keys."""

    model_config = FROZEN_MODEL

    value: NonEmptyString
    identifier_kind: CategoryRef | None = None

    @field_validator("value")
    @classmethod
    def _no_surrounding_whitespace(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("source identifier value must not have surrounding whitespace")

        return value
