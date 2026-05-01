"""Road context models for transportation safety records."""

from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, Field

from civix.domains.transportation_safety.models.common import FROZEN_MODEL


class SpeedLimitUnit(StrEnum):
    """Units for a source-reported speed limit."""

    MILES_PER_HOUR = "mph"
    KILOMETRES_PER_HOUR = "kmh"
    UNKNOWN = "unknown"


class SpeedLimit(BaseModel):
    """A source-reported speed limit."""

    model_config = FROZEN_MODEL

    value: Annotated[int, Field(ge=0)]
    unit: SpeedLimitUnit
