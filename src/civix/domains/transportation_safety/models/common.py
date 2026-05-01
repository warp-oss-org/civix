"""Shared transportation safety model primitives."""

from typing import Annotated

from pydantic import ConfigDict, Field

FROZEN_MODEL = ConfigDict(frozen=True, extra="forbid", strict=True)
CONTRIBUTING_FACTORS_DESCRIPTION = (
    "UNMAPPED means no source field or mapper support, NOT_PROVIDED means source fields "
    "exist but are blank, and an empty tuple with provided quality means the source explicitly "
    "reported no factors."
)

NonEmptyString = Annotated[str, Field(min_length=1)]
NonNegativeCount = Annotated[int, Field(ge=0)]
