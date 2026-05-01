"""Drift report contracts.

Reports are pydantic models so they serialize to JSON cleanly and pass
through validation without reinterpretation. Each report carries both
counts (how big) and sample source-record IDs (where to look).
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, SnapshotId, SourceId
from civix.core.temporal import require_utc

_FROZEN_MODEL = ConfigDict(frozen=True, extra="forbid", strict=True)


class DriftSeverity(StrEnum):
    """Validation-oriented severity for drift findings."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class SchemaDriftKind(StrEnum):
    """Kinds of schema drift reported by V1."""

    UNEXPECTED_FIELD = "unexpected_field"
    MISSING_FIELD = "missing_field"
    TYPE_MISMATCH = "type_mismatch"
    NULLABILITY_MISMATCH = "nullability_mismatch"


class SchemaDriftFinding(BaseModel):
    """One schema drift finding."""

    model_config = _FROZEN_MODEL

    kind: SchemaDriftKind
    severity: DriftSeverity
    field_name: Annotated[str, Field(min_length=1)]
    expected: str
    observed: str
    count: Annotated[int, Field(ge=1)]
    sample_source_record_ids: tuple[str, ...] = ()


class SchemaDriftReport(BaseModel):
    """Schema drift report for one snapshot against one explicit spec."""

    model_config = _FROZEN_MODEL

    snapshot_id: Annotated[SnapshotId, Field(min_length=1)]
    source_id: Annotated[SourceId, Field(min_length=1)]
    dataset_id: Annotated[DatasetId, Field(min_length=1)]
    jurisdiction: Jurisdiction
    fetched_at: datetime
    spec_id: Annotated[str, Field(min_length=1)]
    spec_version: Annotated[str, Field(min_length=1)]
    checked_record_count: Annotated[int, Field(ge=0)]
    findings: tuple[SchemaDriftFinding, ...] = ()

    @computed_field
    @property
    def has_errors(self) -> bool:
        return any(finding.severity is DriftSeverity.ERROR for finding in self.findings)

    @field_validator("fetched_at")
    @classmethod
    def _utc_only(cls, value: datetime) -> datetime:
        return require_utc(value)


class TaxonomyDriftKind(StrEnum):
    """Kinds of taxonomy drift reported by V1."""

    UNRECOGNIZED_VALUE = "unrecognized_value"
    RETIRED_VALUE_OBSERVED = "retired_value_observed"


class TaxonomyDriftFinding(BaseModel):
    """One taxonomy drift finding.

    `observed_value` is the normalized form (the same form the spec is
    written in). `raw_samples` shows the unnormalized strings the
    observer actually saw — useful for distinguishing a real new value
    from a casing/whitespace artifact in the spec.
    """

    model_config = _FROZEN_MODEL

    kind: TaxonomyDriftKind
    severity: DriftSeverity
    taxonomy_id: Annotated[str, Field(min_length=1)]
    source_field: Annotated[str, Field(min_length=1)]
    observed_value: Annotated[str, Field(min_length=1)]
    count: Annotated[int, Field(ge=1)]
    raw_samples: tuple[str, ...] = ()
    sample_source_record_ids: tuple[str, ...] = ()


class TaxonomyDriftReport(BaseModel):
    """Taxonomy drift report for one snapshot against one or more specs."""

    model_config = _FROZEN_MODEL

    snapshot_id: Annotated[SnapshotId, Field(min_length=1)]
    source_id: Annotated[SourceId, Field(min_length=1)]
    dataset_id: Annotated[DatasetId, Field(min_length=1)]
    jurisdiction: Jurisdiction
    fetched_at: datetime
    spec_versions: Mapping[str, str] = Field(default_factory=lambda: dict[str, str]())
    checked_record_count: Annotated[int, Field(ge=0)]
    findings: tuple[TaxonomyDriftFinding, ...] = ()

    @computed_field
    @property
    def has_errors(self) -> bool:
        return any(finding.severity is DriftSeverity.ERROR for finding in self.findings)

    @field_validator("fetched_at")
    @classmethod
    def _utc_only(cls, value: datetime) -> datetime:
        return require_utc(value)
