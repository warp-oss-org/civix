"""Shared taxonomy reference models.

Many civic domains normalize source vocabularies into versioned
taxonomies. The reference type lives in core so records can point to a
taxonomy without depending on a source package or another domain.
"""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator

_FROZEN_MODEL = ConfigDict(frozen=True, extra="forbid", strict=True)


class CategoryRef(BaseModel):
    """A reference into a versioned category taxonomy."""

    model_config = _FROZEN_MODEL

    code: Annotated[str, Field(min_length=1)]
    label: Annotated[str, Field(min_length=1)]
    taxonomy_id: Annotated[str, Field(min_length=1)]
    taxonomy_version: Annotated[str, Field(min_length=1)]

    @field_validator("code", "label", "taxonomy_id", "taxonomy_version")
    @classmethod
    def _no_surrounding_whitespace(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("category parts must not have surrounding whitespace")

        return value
