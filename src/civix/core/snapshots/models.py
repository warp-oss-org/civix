"""Raw source-capture primitives: snapshots and the records inside them.

A `SourceSnapshot` carries the metadata for one fetch operation against
one dataset. A `RawRecord` is one preserved row from that fetch, paired
with a reference back to its snapshot.

These are pure data contracts. They describe what a fetched civic
dataset looks like before any normalization. No fetching, hashing, or
persistence happens here.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from civix.core.identity import DatasetId, Jurisdiction, SnapshotId, SourceId
from civix.core.temporal import require_utc

_FROZEN_MODEL = ConfigDict(frozen=True, extra="forbid", strict=True)


class SourceSnapshot(BaseModel):
    """Metadata describing one fetch operation against one dataset.

    A snapshot is the envelope of metadata shared by every `RawRecord`
    pulled in the same fetch.
    """

    model_config = _FROZEN_MODEL

    snapshot_id: Annotated[SnapshotId, Field(min_length=1)]
    source_id: Annotated[SourceId, Field(min_length=1)]
    dataset_id: Annotated[DatasetId, Field(min_length=1)]
    jurisdiction: Jurisdiction
    fetched_at: datetime
    record_count: Annotated[int, Field(ge=0)]
    source_url: str | None = None
    fetch_params: Mapping[str, str] | None = None
    content_hash: str | None = None

    @field_validator("fetched_at")
    @classmethod
    def _utc_only(cls, value: datetime) -> datetime:
        return require_utc(value)


class RawRecord(BaseModel):
    """One preserved source record, paired with a snapshot reference.

    `raw_data` is intentionally an opaque mapping: civic portals vary, and
    Civix does not interpret the payload at this layer. Mappers are the
    only place that should reach into `raw_data`.
    """

    model_config = _FROZEN_MODEL

    snapshot_id: Annotated[SnapshotId, Field(min_length=1)]
    raw_data: Mapping[str, Any]
    source_record_id: str | None = None
    source_updated_at: datetime | None = None
    record_hash: str | None = None

    @field_validator("source_updated_at")
    @classmethod
    def _utc_only(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return require_utc(value)
