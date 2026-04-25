"""Raw observation and source snapshot primitives.

These are pure data contracts. They describe what a fetched civic dataset
looks like before any normalization. No fetching, hashing, or persistence
happens here.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Annotated, Any, NewType

from pydantic import BaseModel, ConfigDict, Field, field_validator

SourceId = NewType("SourceId", str)
DatasetId = NewType("DatasetId", str)
SnapshotId = NewType("SnapshotId", str)


_FROZEN_MODEL = ConfigDict(frozen=True, extra="forbid", strict=True)


def _require_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError("datetime must be timezone-aware and in UTC")
    return value


class Jurisdiction(BaseModel):
    """The civic scope a dataset describes.

    `region` and `locality` are optional so the same type can describe a
    federal dataset, a province- or state-wide dataset, or a city dataset
    without lying about a level that does not exist for that source.
    """

    model_config = _FROZEN_MODEL

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
        return _require_utc(value)


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
        return _require_utc(value)
