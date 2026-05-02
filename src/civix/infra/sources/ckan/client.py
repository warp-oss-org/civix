"""Shared CKAN datastore fetch loop."""

from __future__ import annotations

from collections.abc import AsyncIterable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Final, cast

import httpx
from pydantic import ValidationError

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, SnapshotId, SourceId
from civix.core.ports.errors import FetchError
from civix.core.ports.models.adapter import FetchResult
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.temporal import Clock, utc_now

DEFAULT_BASE_URL: Final[str] = "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/"
DEFAULT_PAGE_SIZE: Final[int] = 1000


@dataclass(frozen=True, slots=True)
class CkanDatasetConfig:
    """Stable identity and endpoint details for one CKAN package."""

    source_id: SourceId
    dataset_id: DatasetId
    jurisdiction: Jurisdiction
    source_record_id_fields: tuple[str, ...] = ()
    resource_name: str | None = None
    base_url: str = DEFAULT_BASE_URL


@dataclass(frozen=True, slots=True)
class CkanFetchConfig:
    """Runtime fetch options for one CKAN datastore snapshot."""

    client: httpx.AsyncClient
    clock: Clock = field(default=utc_now)
    page_size: int = DEFAULT_PAGE_SIZE

    def __post_init__(self) -> None:
        if self.page_size <= 0:
            raise ValueError("page_size must be greater than zero")


@dataclass(frozen=True, slots=True)
class CkanSourceAdapter:
    """Configured source adapter for one CKAN datastore package."""

    dataset: CkanDatasetConfig
    fetch_config: CkanFetchConfig

    @property
    def source_id(self) -> SourceId:
        return self.dataset.source_id

    @property
    def dataset_id(self) -> DatasetId:
        return self.dataset.dataset_id

    @property
    def jurisdiction(self) -> Jurisdiction:
        return self.dataset.jurisdiction

    async def fetch(self) -> FetchResult:
        return await fetch_ckan_dataset(dataset=self.dataset, fetch=self.fetch_config)


async def fetch_ckan_dataset(*, dataset: CkanDatasetConfig, fetch: CkanFetchConfig) -> FetchResult:
    """Fetch a CKAN datastore snapshot and return lazy raw records."""
    fetched_at = fetch.clock()
    resource_id = await _resolve_resource_id(dataset=dataset, fetch=fetch)
    first_page = await _fetch_page(dataset=dataset, fetch=fetch, resource_id=resource_id, offset=0)
    total = _read_total(page=first_page, dataset=dataset)
    snapshot = _build_snapshot(
        dataset=dataset,
        fetched_at=fetched_at,
        record_count=total,
        resource_id=resource_id,
    )

    return FetchResult(
        snapshot=snapshot,
        records=_stream_records(
            dataset=dataset,
            fetch=fetch,
            snapshot=snapshot,
            resource_id=resource_id,
            first_page=first_page,
            total=total,
        ),
    )


async def _resolve_resource_id(*, dataset: CkanDatasetConfig, fetch: CkanFetchConfig) -> str:
    url = _action_url(dataset, "package_show")

    try:
        response = await fetch.client.get(url, params={"id": dataset.dataset_id})

        response.raise_for_status()

        payload = response.json()
    except httpx.HTTPError as e:
        raise FetchError(
            f"failed to read package metadata from {url}",
            source_id=dataset.source_id,
            dataset_id=dataset.dataset_id,
            operation="resolve-resource",
        ) from e
    except ValueError as e:
        raise FetchError(
            f"non-JSON response from {url}",
            source_id=dataset.source_id,
            dataset_id=dataset.dataset_id,
            operation="resolve-resource",
        ) from e

    result = _unwrap_ckan_result(
        payload,
        dataset=dataset,
        url=url,
        operation="resolve-resource",
    )
    resources = result.get("resources")

    if not isinstance(resources, list):
        raise FetchError(
            f"missing or invalid resources list in package metadata from {url}",
            source_id=dataset.source_id,
            dataset_id=dataset.dataset_id,
            operation="resolve-resource",
        )

    resource_entries = cast(list[Any], resources)

    if dataset.resource_name is not None:
        return _resolve_named_resource_id(
            resources=resource_entries,
            dataset=dataset,
        )

    for entry in resource_entries:
        if not isinstance(entry, dict):
            continue

        resource = cast(dict[str, Any], entry)

        if resource.get("datastore_active") is True:
            resource_id = resource.get("id")

            if isinstance(resource_id, str) and resource_id:
                return resource_id

    raise FetchError(
        "no datastore-active resource found for dataset",
        source_id=dataset.source_id,
        dataset_id=dataset.dataset_id,
        operation="resolve-resource",
    )


def _resolve_named_resource_id(
    *,
    resources: list[Any],
    dataset: CkanDatasetConfig,
) -> str:
    requested_name = dataset.resource_name

    for entry in resources:
        if not isinstance(entry, dict):
            continue

        resource = cast(dict[str, Any], entry)

        if resource.get("name") != requested_name:
            continue

        if resource.get("datastore_active") is not True:
            raise FetchError(
                f"datastore resource named {requested_name!r} is not active for dataset",
                source_id=dataset.source_id,
                dataset_id=dataset.dataset_id,
                operation="resolve-resource",
            )

        resource_id = resource.get("id")

        if isinstance(resource_id, str) and resource_id:
            return resource_id

        raise FetchError(
            f"datastore resource named {requested_name!r} is missing an id for dataset",
            source_id=dataset.source_id,
            dataset_id=dataset.dataset_id,
            operation="resolve-resource",
        )

    raise FetchError(
        f"datastore resource named {requested_name!r} not found for dataset",
        source_id=dataset.source_id,
        dataset_id=dataset.dataset_id,
        operation="resolve-resource",
    )


async def _fetch_page(
    *,
    dataset: CkanDatasetConfig,
    fetch: CkanFetchConfig,
    resource_id: str,
    offset: int,
) -> dict[str, Any]:
    url = _action_url(dataset, "datastore_search")
    params: dict[str, str | int] = {
        "resource_id": resource_id,
        "limit": fetch.page_size,
        "offset": offset,
    }

    try:
        response = await fetch.client.get(url, params=params)

        response.raise_for_status()

        payload = response.json()
    except httpx.HTTPError as e:
        raise FetchError(
            f"failed to read records from {url} at offset={offset}",
            source_id=dataset.source_id,
            dataset_id=dataset.dataset_id,
            operation="fetch-page",
        ) from e
    except ValueError as e:
        raise FetchError(
            f"non-JSON response from {url}",
            source_id=dataset.source_id,
            dataset_id=dataset.dataset_id,
            operation="fetch-page",
        ) from e

    return _unwrap_ckan_result(payload, dataset=dataset, url=url, operation="fetch-page")


async def _stream_records(
    *,
    dataset: CkanDatasetConfig,
    fetch: CkanFetchConfig,
    snapshot: SourceSnapshot,
    resource_id: str,
    first_page: dict[str, Any],
    total: int,
) -> AsyncIterable[RawRecord]:
    page = first_page
    offset = 0

    while True:
        records = page.get("records")

        if not isinstance(records, list):
            raise FetchError(
                "missing or invalid records list in datastore_search response",
                source_id=dataset.source_id,
                dataset_id=dataset.dataset_id,
                operation="stream-records",
            )

        rows = cast(list[Any], records)

        if not rows:
            return

        for row in rows:
            yield _build_record(dataset=dataset, snapshot_id=snapshot.snapshot_id, row=row)

        offset += len(rows)

        if offset >= total:
            return

        if len(rows) < fetch.page_size:
            return

        page = await _fetch_page(
            dataset=dataset,
            fetch=fetch,
            resource_id=resource_id,
            offset=offset,
        )


def _build_record(
    *,
    dataset: CkanDatasetConfig,
    snapshot_id: SnapshotId,
    row: Any,
) -> RawRecord:
    if not isinstance(row, dict):
        raise FetchError(
            "datastore_search returned a non-object record",
            source_id=dataset.source_id,
            dataset_id=dataset.dataset_id,
            operation="stream-records",
        )

    row_dict = cast(dict[str, Any], row)
    source_record_id = _source_record_id(row_dict, dataset.source_record_id_fields)

    try:
        return RawRecord(
            snapshot_id=snapshot_id,
            raw_data=row_dict,
            source_record_id=source_record_id,
            source_updated_at=None,
        )
    except ValidationError as e:
        raise FetchError(
            "raw record failed validation",
            source_id=dataset.source_id,
            dataset_id=dataset.dataset_id,
            operation="stream-records",
        ) from e


def _unwrap_ckan_result(
    payload: object,
    *,
    dataset: CkanDatasetConfig,
    url: str,
    operation: str,
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise FetchError(
            f"non-object JSON body from {url}",
            source_id=dataset.source_id,
            dataset_id=dataset.dataset_id,
            operation=operation,
        )

    envelope = cast(dict[str, Any], payload)

    if envelope.get("success") is not True:
        raise FetchError(
            f"CKAN reported success=False for {url}",
            source_id=dataset.source_id,
            dataset_id=dataset.dataset_id,
            operation=operation,
        )

    result = envelope.get("result")

    if not isinstance(result, dict):
        raise FetchError(
            f"missing or invalid result object in CKAN response from {url}",
            source_id=dataset.source_id,
            dataset_id=dataset.dataset_id,
            operation=operation,
        )

    return cast(dict[str, Any], result)


def _read_total(*, page: dict[str, Any], dataset: CkanDatasetConfig) -> int:
    total = page.get("total")

    if not isinstance(total, int) or total < 0:
        raise FetchError(
            "missing or invalid total in datastore_search response",
            source_id=dataset.source_id,
            dataset_id=dataset.dataset_id,
            operation="fetch-page",
        )

    return total


def _source_record_id(row: dict[str, Any], fields: tuple[str, ...]) -> str | None:
    if not fields:
        return None

    values: list[str] = []

    for field_name in fields:
        value = row.get(field_name)

        if value is None:
            return None

        text = str(value).strip()

        if not text:
            return None

        values.append(text)

    return ":".join(values)


def _build_snapshot(
    *,
    dataset: CkanDatasetConfig,
    fetched_at: datetime,
    record_count: int,
    resource_id: str,
) -> SourceSnapshot:
    snapshot_id = SnapshotId(f"{dataset.source_id}:{dataset.dataset_id}:{fetched_at.isoformat()}")

    return SourceSnapshot(
        snapshot_id=snapshot_id,
        source_id=dataset.source_id,
        dataset_id=dataset.dataset_id,
        jurisdiction=dataset.jurisdiction,
        fetched_at=fetched_at,
        record_count=record_count,
        source_url=f"{_action_url(dataset, 'datastore_search')}?resource_id={resource_id}",
        fetch_params={"resource_id": resource_id},
    )


def _action_url(dataset: CkanDatasetConfig, action: str) -> str:
    return f"{dataset.base_url}{action}"
