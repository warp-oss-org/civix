"""Stable hazard-risk record key helpers."""

from __future__ import annotations

import hashlib
import json
from typing import Annotated

from pydantic import Field

from civix.core.identity.models.identifiers import DatasetId, SourceId

_AREA_KEY_PATTERN = r"^hr-area:v1:[0-9a-f]{64}$"
_ZONE_KEY_PATTERN = r"^hr-zone:v1:[0-9a-f]{64}$"

HazardRiskAreaKey = Annotated[str, Field(pattern=_AREA_KEY_PATTERN)]
HazardRiskZoneKey = Annotated[str, Field(pattern=_ZONE_KEY_PATTERN)]


def build_hazard_risk_area_key(
    source_id: SourceId, dataset_id: DatasetId, source_area_id: str
) -> str:
    """Build a deterministic Civix key for one source-published risk area."""

    return _build_key(
        prefix="hr-area",
        source_id=str(source_id),
        dataset_id=str(dataset_id),
        source_record_id=source_area_id,
    )


def build_hazard_risk_zone_key(
    source_id: SourceId, dataset_id: DatasetId, source_zone_id: str
) -> str:
    """Build a deterministic Civix key for one source-published risk zone."""

    return _build_key(
        prefix="hr-zone",
        source_id=str(source_id),
        dataset_id=str(dataset_id),
        source_record_id=source_zone_id,
    )


def _build_key(*, prefix: str, source_id: str, dataset_id: str, source_record_id: str) -> str:
    for label, value in (
        ("source_id", source_id),
        ("dataset_id", dataset_id),
        ("source_record_id", source_record_id),
    ):
        if not value:
            raise ValueError(f"{label} must be non-empty")

        if value != value.strip():
            raise ValueError(f"{label} must not have surrounding whitespace")

    payload = {
        "dataset_id": dataset_id,
        "source_id": source_id,
        "source_record_id": source_record_id,
    }
    canonical = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    return f"{prefix}:v1:{digest}"
