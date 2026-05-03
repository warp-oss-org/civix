"""Canada DMAF source adapter."""

from __future__ import annotations

from collections.abc import AsyncIterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final, cast

from pydantic import ValidationError

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, SnapshotId, SourceId
from civix.core.ports.errors import FetchError
from civix.core.ports.models.adapter import FetchResult
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.infra.sources.ckan import (
    CkanDatasetConfig,
    CkanFetchConfig,
    fetch_ckan_static_json_resource,
)

SOURCE_ID: Final[SourceId] = SourceId("infrastructure-canada")
CA_JURISDICTION: Final[Jurisdiction] = Jurisdiction(country="CA")
CANADA_DMAF_PROJECTS_DATASET_ID: Final[DatasetId] = DatasetId(
    "beee0771-dab9-4be8-9b80-f8e8b3fdfd9d"
)
CANADA_DMAF_PROJECTS_RESOURCE_NAME: Final[str] = "Project List"
CANADA_DMAF_PROJECTS_RESOURCE_FORMAT: Final[str] = "JSON"
CANADA_DMAF_PROJECTS_RESOURCE_LANGUAGES: Final[tuple[str, ...]] = ("en", "fr")
CANADA_DMAF_PROGRAM_CODE: Final[str] = "DMAF"
CANADA_DMAF_ROW_FILTER: Final[str] = "programCode_en == DMAF"
OPEN_CANADA_CKAN_BASE_URL: Final[str] = "https://open.canada.ca/data/api/action/"
CANADA_DMAF_SOURCE_SCOPE: Final[str] = (
    "Infrastructure Canada Projects rows for the Disaster Mitigation and Adaptation Fund."
)

CANADA_DMAF_PROJECTS_DATASET_CONFIG: Final[CkanDatasetConfig] = CkanDatasetConfig(
    source_id=SOURCE_ID,
    dataset_id=CANADA_DMAF_PROJECTS_DATASET_ID,
    jurisdiction=CA_JURISDICTION,
    source_record_id_fields=("projectNumber",),
    resource_name=CANADA_DMAF_PROJECTS_RESOURCE_NAME,
    base_url=OPEN_CANADA_CKAN_BASE_URL,
)


@dataclass(frozen=True, slots=True)
class CanadaDmafProjectsAdapter:
    """Fetches DMAF project rows from the Infrastructure Canada project list."""

    fetch_config: CkanFetchConfig

    @property
    def source_id(self) -> SourceId:
        return CANADA_DMAF_PROJECTS_DATASET_CONFIG.source_id

    @property
    def dataset_id(self) -> DatasetId:
        return CANADA_DMAF_PROJECTS_DATASET_CONFIG.dataset_id

    @property
    def jurisdiction(self) -> Jurisdiction:
        return CANADA_DMAF_PROJECTS_DATASET_CONFIG.jurisdiction

    async def fetch(self) -> FetchResult:
        fetched_at = self.fetch_config.clock()
        resource = await fetch_ckan_static_json_resource(
            dataset=CANADA_DMAF_PROJECTS_DATASET_CONFIG,
            fetch=self.fetch_config,
            resource_name=CANADA_DMAF_PROJECTS_RESOURCE_NAME,
            resource_format=CANADA_DMAF_PROJECTS_RESOURCE_FORMAT,
            languages=CANADA_DMAF_PROJECTS_RESOURCE_LANGUAGES,
        )
        rows = _decode_project_list(resource.payload)
        dmaf_rows = tuple(
            row
            for row in rows
            if _text_or_none(row.get("programCode_en")) == CANADA_DMAF_PROGRAM_CODE
        )
        snapshot = _build_snapshot(
            fetched_at=fetched_at,
            record_count=len(dmaf_rows),
            resource_id=resource.resource_id,
            resource_name=resource.resource_name,
            resource_format=resource.resource_format,
            resource_url=resource.resource_url,
            source_total_records=len(rows),
        )

        return FetchResult(snapshot=snapshot, records=_stream_records(snapshot, dmaf_rows))


def _decode_project_list(payload: Any) -> tuple[dict[str, Any], ...]:
    if not isinstance(payload, dict):
        raise _fetch_error("static JSON resource must be an object", operation="decode-resource")

    table = cast(dict[str, Any], payload)
    headers = table.get("indexTitles")
    data = table.get("data")

    if not isinstance(headers, list):
        raise _fetch_error("missing or invalid indexTitles", operation="decode-resource")

    header_values = cast(list[Any], headers)

    if not all(isinstance(header, str) for header in header_values):
        raise _fetch_error("missing or invalid indexTitles", operation="decode-resource")

    header_names = cast(list[str], header_values)

    if len(set(header_names)) != len(header_names):
        raise _fetch_error(
            "duplicate indexTitles in static JSON resource",
            operation="decode-resource",
        )

    if not isinstance(data, list):
        raise _fetch_error("missing or invalid data rows", operation="decode-resource")

    data_rows = cast(list[Any], data)
    decoded: list[dict[str, Any]] = []

    for index, row in enumerate(data_rows):
        if not isinstance(row, list):
            raise _fetch_error(
                f"data row {index} is not an array",
                operation="decode-resource",
            )

        row_values = cast(list[Any], row)

        if len(row_values) != len(header_names):
            raise _fetch_error(
                f"data row {index} width does not match indexTitles",
                operation="decode-resource",
            )

        decoded.append(dict(zip(header_names, row_values, strict=True)))

    return tuple(decoded)


async def _stream_records(
    snapshot: SourceSnapshot,
    rows: tuple[Mapping[str, Any], ...],
) -> AsyncIterable[RawRecord]:
    for row in rows:
        row_dict = dict(row)
        source_record_id = _text_or_none(row_dict.get("projectNumber"))

        try:
            yield RawRecord(
                snapshot_id=snapshot.snapshot_id,
                raw_data=row_dict,
                source_record_id=source_record_id,
                source_updated_at=None,
            )
        except ValidationError as e:
            raise _fetch_error("raw record failed validation", operation="stream-records") from e


def _build_snapshot(
    *,
    fetched_at: datetime,
    record_count: int,
    resource_id: str,
    resource_name: str,
    resource_format: str,
    resource_url: str,
    source_total_records: int,
) -> SourceSnapshot:
    snapshot_id = SnapshotId(
        f"{SOURCE_ID}:{CANADA_DMAF_PROJECTS_DATASET_ID}:dmaf:{fetched_at.isoformat()}"
    )

    return SourceSnapshot(
        snapshot_id=snapshot_id,
        source_id=SOURCE_ID,
        dataset_id=CANADA_DMAF_PROJECTS_DATASET_ID,
        jurisdiction=CA_JURISDICTION,
        fetched_at=fetched_at,
        record_count=record_count,
        source_url=resource_url,
        fetch_params={
            "package_id": str(CANADA_DMAF_PROJECTS_DATASET_ID),
            "resource_id": resource_id,
            "resource_name": resource_name,
            "resource_format": resource_format,
            "source_total_records": str(source_total_records),
            "row_filter": CANADA_DMAF_ROW_FILTER,
        },
    )


def _text_or_none(value: object) -> str | None:
    if value is None:
        return None

    text = str(value).strip()

    return text if text else None


def _fetch_error(message: str, *, operation: str) -> FetchError:
    return FetchError(
        message,
        source_id=SOURCE_ID,
        dataset_id=CANADA_DMAF_PROJECTS_DATASET_ID,
        operation=operation,
    )
