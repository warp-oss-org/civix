"""Validation report contracts.

A `ValidationReport` is the pass/fail decision over a snapshot's
already-produced artifacts (manifest + drift reports). The validator is
pure; these types describe its output.

Reports carry the same identity quintet as `ExportManifest` and the
drift reports — `snapshot_id`, `source_id`, `dataset_id`, `jurisdiction`,
`fetched_at` — so a downstream consumer can correlate them without
joining on opaque keys.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator

from civix.core.drift.report import DriftSeverity
from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, SnapshotId, SourceId
from civix.core.temporal import require_utc

_FROZEN_MODEL = ConfigDict(frozen=True, extra="forbid", strict=True)


ValidationFindingSource = Literal[
    "input_identity",
    "schema_drift",
    "taxonomy_drift",
    "mapping_coverage",
    "record_count",
]


class ValidationOutcome(StrEnum):
    """Whether a snapshot is safe to consume under V1 default rules."""

    PASS = "pass"
    FAIL = "fail"


class ValidationFinding(BaseModel):
    """One reason a snapshot did or did not pass validation.

    `detail_ref` carries the most specific identifier the underlying
    rule had: a field name (schema), a taxonomy id (taxonomy), a
    `FieldQuality` value (mapping coverage), or `None` when the finding
    is about the snapshot as a whole.
    """

    model_config = _FROZEN_MODEL

    source: ValidationFindingSource
    severity: DriftSeverity
    message: Annotated[str, Field(min_length=1)]
    detail_ref: str | None = None


class ValidationReport(BaseModel):
    """Pass/fail decision over one snapshot, with the reasons attached."""

    model_config = _FROZEN_MODEL

    snapshot_id: Annotated[SnapshotId, Field(min_length=1)]
    source_id: Annotated[SourceId, Field(min_length=1)]
    dataset_id: Annotated[DatasetId, Field(min_length=1)]
    jurisdiction: Jurisdiction
    fetched_at: datetime
    outcome: ValidationOutcome
    findings: tuple[ValidationFinding, ...] = ()

    @computed_field
    @property
    def has_errors(self) -> bool:
        return self.outcome is ValidationOutcome.FAIL

    @field_validator("fetched_at")
    @classmethod
    def _utc_only(cls, value: datetime) -> datetime:
        return require_utc(value)
