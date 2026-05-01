"""Identity primitives shared across the core.

These are pure naming and value types with no dependencies. Every other
core module (snapshots, provenance, mapping, drift) and every domain
references them, so they live together in one place to keep dependency
direction clean.
"""

from __future__ import annotations

from typing import Annotated, NewType

from pydantic import BaseModel, ConfigDict, Field, field_validator

SourceId = NewType("SourceId", str)
DatasetId = NewType("DatasetId", str)
SnapshotId = NewType("SnapshotId", str)
MapperId = NewType("MapperId", str)


class Jurisdiction(BaseModel):
    """The civic scope a dataset describes.

    `region` and `locality` are optional so the same type can describe a
    federal dataset, a province- or state-wide dataset, or a city dataset
    without lying about a level that does not exist for that source.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    country: Annotated[str, Field(min_length=1)]
    region: Annotated[str | None, Field(min_length=1)] = None
    locality: Annotated[str | None, Field(min_length=1)] = None

    @field_validator("country", "region", "locality")
    @classmethod
    def _no_surrounding_whitespace(cls, value: str | None) -> str | None:
        if value is None:
            return None

        if value != value.strip():
            raise ValueError("jurisdiction parts must not have surrounding whitespace")

        return value
