"""Manifest written alongside an exported snapshot.

The manifest is the single source of truth for "what is in this snapshot
directory and is it complete". It is written last; its presence on disk
implies records.jsonl, reports.jsonl, and schema.json have already been
finalized.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, SnapshotId, SourceId
from civix.core.provenance.models.provenance import MapperVersion
from civix.core.quality.models.fields import FieldQuality
from civix.core.temporal import require_utc

_FROZEN_MODEL = ConfigDict(frozen=True, extra="forbid", strict=True)


class ExportedFile(BaseModel):
    """One file written into the snapshot export directory."""

    model_config = _FROZEN_MODEL

    filename: Annotated[str, Field(min_length=1)]
    sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    byte_count: Annotated[int, Field(ge=0)]


class MappingSummary(BaseModel):
    """Aggregate mapping diagnostics across every record in the snapshot.

    Per-quality counts cover every `MappedField` on every normalized
    record. The two totals roll up the equivalent across `MappingReport`s.
    """

    model_config = _FROZEN_MODEL

    quality_counts: Mapping[FieldQuality, int] = Field(
        default_factory=lambda: dict[FieldQuality, int]()
    )
    unmapped_source_fields_total: Annotated[int, Field(ge=0)] = 0
    conflicts_total: Annotated[int, Field(ge=0)] = 0


class ExportManifest(BaseModel):
    """Top-level manifest for a JSON snapshot export."""

    model_config = _FROZEN_MODEL

    snapshot_id: Annotated[SnapshotId, Field(min_length=1)]
    source_id: Annotated[SourceId, Field(min_length=1)]
    dataset_id: Annotated[DatasetId, Field(min_length=1)]
    jurisdiction: Jurisdiction
    fetched_at: datetime
    record_count: Annotated[int, Field(ge=0)]
    mapper: MapperVersion | None = None
    files: tuple[ExportedFile, ...]
    mapping_summary: MappingSummary

    @field_validator("fetched_at")
    @classmethod
    def _utc_only(cls, value: datetime) -> datetime:
        return require_utc(value)
