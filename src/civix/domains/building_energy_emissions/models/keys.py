"""Stable building energy and emissions record key helpers."""

from __future__ import annotations

import hashlib
import json
from typing import Annotated

from pydantic import Field

from civix.core.identity.models.identifiers import DatasetId, SourceId

_SUBJECT_KEY_PATTERN = r"^bee-subject:v1:[0-9a-f]{64}$"
_REPORT_KEY_PATTERN = r"^bee-report:v1:[0-9a-f]{64}$"
_CASE_KEY_PATTERN = r"^bee-case:v1:[0-9a-f]{64}$"
_METRIC_KEY_PATTERN = r"^bee-metric:v1:[0-9a-f]{64}$"

BuildingEnergySubjectKey = Annotated[str, Field(pattern=_SUBJECT_KEY_PATTERN)]
BuildingEnergyReportKey = Annotated[str, Field(pattern=_REPORT_KEY_PATTERN)]
BuildingComplianceCaseKey = Annotated[str, Field(pattern=_CASE_KEY_PATTERN)]
BuildingMetricValueKey = Annotated[str, Field(pattern=_METRIC_KEY_PATTERN)]


def build_building_energy_subject_key(
    source_id: SourceId, dataset_id: DatasetId, source_subject_id: str
) -> str:
    """Build a deterministic Civix key for one source-published energy subject."""

    return _build_key(
        prefix="bee-subject",
        parts={
            "source_id": str(source_id),
            "dataset_id": str(dataset_id),
            "source_record_id": source_subject_id,
        },
    )


def build_building_energy_report_key(
    source_id: SourceId,
    dataset_id: DatasetId,
    source_report_id: str,
    reporting_period_token: str,
) -> str:
    """Build a deterministic Civix key for one source-published energy report.

    Reports are scoped by reporting period because the same source record
    identifier can be reused across reporting years.
    """

    return _build_key(
        prefix="bee-report",
        parts={
            "source_id": str(source_id),
            "dataset_id": str(dataset_id),
            "source_record_id": source_report_id,
            "reporting_period_token": reporting_period_token,
        },
    )


def build_building_compliance_case_key(
    source_id: SourceId,
    dataset_id: DatasetId,
    source_case_id: str,
    covered_period_token: str,
) -> str:
    """Build a deterministic Civix key for one source-published compliance case."""

    return _build_key(
        prefix="bee-case",
        parts={
            "source_id": str(source_id),
            "dataset_id": str(dataset_id),
            "source_record_id": source_case_id,
            "covered_period_token": covered_period_token,
        },
    )


def build_building_metric_value_key(
    source_id: SourceId,
    dataset_id: DatasetId,
    parent_record_key: str,
    metric_discriminator: str,
) -> str:
    """Build a deterministic Civix key for one source-published metric value.

    `parent_record_key` is the key of the metric's parent record: either
    a `BuildingEnergyReport` key or a `BuildingComplianceCase` key.
    `metric_discriminator` is a mapper-supplied stable label (e.g. metric
    type plus fuel/scope qualifier) that distinguishes child metrics
    emitted under the same parent record.
    """

    return _build_key(
        prefix="bee-metric",
        parts={
            "source_id": str(source_id),
            "dataset_id": str(dataset_id),
            "parent_record_key": parent_record_key,
            "metric_discriminator": metric_discriminator,
        },
    )


def _build_key(*, prefix: str, parts: dict[str, str]) -> str:
    for label, value in parts.items():
        if not value:
            raise ValueError(f"{label} must be non-empty")

        if value != value.strip():
            raise ValueError(f"{label} must not have surrounding whitespace")

    canonical = json.dumps(parts, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    return f"{prefix}:v1:{digest}"
