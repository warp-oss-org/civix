"""Road user role and contributing factor models."""

from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, Field, field_validator

from civix.core.taxonomy.models.category import CategoryRef
from civix.domains.transportation_safety.models.common import FROZEN_MODEL, NonEmptyString


class RoadUserRole(StrEnum):
    """Normalized role of an involved road user."""

    DRIVER = "driver"
    PASSENGER = "passenger"
    PEDESTRIAN = "pedestrian"
    CYCLIST = "cyclist"
    MOTORCYCLIST = "motorcyclist"
    OTHER = "other"
    UNKNOWN = "unknown"


class ContributingFactor(BaseModel):
    """A raw contributing factor with optional normalized taxonomy."""

    model_config = FROZEN_MODEL

    raw_label: NonEmptyString
    rank: Annotated[
        int | None,
        Field(
            ge=1,
            description=(
                "Source ordering convention; rank=1 is primary when a source labels a "
                "factor primary."
            ),
        ),
    ] = None
    category: CategoryRef | None = None

    @field_validator("raw_label")
    @classmethod
    def _no_surrounding_raw_label_whitespace(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("raw_label must not have surrounding whitespace")

        return value
