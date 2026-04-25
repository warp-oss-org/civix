"""Normalized-record provenance primitives.

A `ProvenanceRef` is what every normalized domain record carries to
answer where it came from, when, and via which mapper. It complements
the per-field lineage already in `MappedField.source_fields`: this layer
describes the row, that layer describes each cell.

The reference is intentionally self-describing — `source_id`,
`dataset_id`, `jurisdiction`, and `fetched_at` are duplicated from the
referenced `SourceSnapshot` so an exported row is interpretable without
joining back to a snapshot manifest.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator

from civix.core.identity import (
    DatasetId,
    Jurisdiction,
    MapperId,
    SnapshotId,
    SourceId,
)
from civix.core.temporal import require_utc

_FROZEN_MODEL = ConfigDict(frozen=True, extra="forbid", strict=True)


class MapperVersion(BaseModel):
    """The mapper that produced a normalized record.

    `version` is a free-form string. Civic mappers vary in how they
    version themselves — semver, commit hash, build date — and pinning
    one shape now would lock out reasonable alternatives.
    """

    model_config = _FROZEN_MODEL

    mapper_id: Annotated[MapperId, Field(min_length=1)]
    version: Annotated[str, Field(min_length=1)]

    @field_validator("mapper_id", "version")
    @classmethod
    def _no_surrounding_whitespace(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("mapper identity parts must not have surrounding whitespace")
        return value


class ProvenanceRef(BaseModel):
    """Record-level lineage for a normalized domain record."""

    model_config = _FROZEN_MODEL

    snapshot_id: Annotated[SnapshotId, Field(min_length=1)]
    source_id: Annotated[SourceId, Field(min_length=1)]
    dataset_id: Annotated[DatasetId, Field(min_length=1)]
    jurisdiction: Jurisdiction
    fetched_at: datetime
    mapper: MapperVersion
    source_record_id: str | None = None

    @field_validator("fetched_at")
    @classmethod
    def _utc_only(cls, value: datetime) -> datetime:
        return require_utc(value)
