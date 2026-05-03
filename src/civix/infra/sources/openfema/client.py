"""Shared OpenFEMA fetch loop."""

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

DEFAULT_BASE_URL: Final[str] = "https://www.fema.gov/api/open/"
DEFAULT_PAGE_SIZE: Final[int] = 1000
MAX_PAGE_SIZE: Final[int] = 10000


@dataclass(frozen=True, slots=True)
class OpenFemaDatasetConfig:
    """Stable identity and endpoint details for one OpenFEMA entity."""

    source_id: SourceId
    dataset_id: DatasetId
    jurisdiction: Jurisdiction
    version: str
    entity: str
    source_record_id_fields: tuple[str, ...] = ()
    base_url: str = DEFAULT_BASE_URL


@dataclass(frozen=True, slots=True)
class OpenFemaFetchConfig:
    """Runtime fetch options for one OpenFEMA dataset snapshot."""

    client: httpx.AsyncClient
    clock: Clock = field(default=utc_now)
    page_size: int = DEFAULT_PAGE_SIZE
    order_by: str | None = None
    filter_expr: str | None = None
    select: str | None = None

    def __post_init__(self) -> None:
        if self.page_size <= 0:
            raise ValueError("page_size must be greater than zero")

        if self.page_size > MAX_PAGE_SIZE:
            raise ValueError(f"page_size must be less than or equal to {MAX_PAGE_SIZE}")


@dataclass(frozen=True, slots=True)
class OpenFemaSourceAdapter:
    """Configured source adapter for one OpenFEMA entity."""

    dataset: OpenFemaDatasetConfig
    fetch_config: OpenFemaFetchConfig

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
        return await fetch_openfema_dataset(dataset=self.dataset, fetch=self.fetch_config)


async def fetch_openfema_dataset(
    *,
    dataset: OpenFemaDatasetConfig,
    fetch: OpenFemaFetchConfig,
) -> FetchResult:
    """Fetch an OpenFEMA dataset snapshot and return lazy raw records."""
    fetched_at = fetch.clock()
    first_page = await _fetch_page(dataset=dataset, fetch=fetch, skip=0, include_count=True)
    total = _read_count(payload=first_page, dataset=dataset)
    snapshot = _build_snapshot(
        dataset=dataset,
        fetch=fetch,
        fetched_at=fetched_at,
        record_count=total,
    )

    return FetchResult(
        snapshot=snapshot,
        records=_stream_records(
            dataset=dataset,
            fetch=fetch,
            snapshot=snapshot,
            first_page=first_page,
            total=total,
        ),
    )


async def _fetch_page(
    *,
    dataset: OpenFemaDatasetConfig,
    fetch: OpenFemaFetchConfig,
    skip: int,
    include_count: bool,
) -> dict[str, Any]:
    url = _resource_url(dataset)

    try:
        response = await fetch.client.get(
            url,
            params=_page_params(fetch=fetch, skip=skip, include_count=include_count),
        )

        response.raise_for_status()

        payload = response.json()
    except httpx.HTTPError as e:
        raise FetchError(
            f"failed to read records from {url} at skip={skip}",
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

    if not isinstance(payload, dict):
        raise FetchError(
            f"non-object JSON body from {url}",
            source_id=dataset.source_id,
            dataset_id=dataset.dataset_id,
            operation="fetch-page",
        )

    return cast(dict[str, Any], payload)


async def _stream_records(
    *,
    dataset: OpenFemaDatasetConfig,
    fetch: OpenFemaFetchConfig,
    snapshot: SourceSnapshot,
    first_page: dict[str, Any],
    total: int,
) -> AsyncIterable[RawRecord]:
    page = first_page
    skip = 0

    while True:
        rows = _read_rows(payload=page, dataset=dataset)

        if not rows:
            return

        for row in rows:
            yield _build_record(dataset=dataset, snapshot_id=snapshot.snapshot_id, row=row)

        skip += len(rows)

        if skip >= total:
            return

        if len(rows) < fetch.page_size:
            return

        page = await _fetch_page(dataset=dataset, fetch=fetch, skip=skip, include_count=False)


def _build_record(
    *,
    dataset: OpenFemaDatasetConfig,
    snapshot_id: SnapshotId,
    row: Any,
) -> RawRecord:
    if not isinstance(row, dict):
        raise FetchError(
            "OpenFEMA returned a non-object record",
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


def _read_count(*, payload: dict[str, Any], dataset: OpenFemaDatasetConfig) -> int:
    metadata = payload.get("metadata")

    if not isinstance(metadata, dict):
        raise FetchError(
            "missing or invalid metadata in OpenFEMA response",
            source_id=dataset.source_id,
            dataset_id=dataset.dataset_id,
            operation="fetch-page",
        )

    metadata_dict = cast(dict[str, Any], metadata)
    count = metadata_dict.get("count")

    if isinstance(count, int) and count >= 0:
        return count

    raise FetchError(
        "missing or invalid count in OpenFEMA response",
        source_id=dataset.source_id,
        dataset_id=dataset.dataset_id,
        operation="fetch-page",
    )


def _read_rows(*, payload: dict[str, Any], dataset: OpenFemaDatasetConfig) -> list[Any]:
    rows = payload.get(dataset.entity)

    if not isinstance(rows, list):
        raise FetchError(
            f"missing or invalid {dataset.entity!r} records list in OpenFEMA response",
            source_id=dataset.source_id,
            dataset_id=dataset.dataset_id,
            operation="stream-records",
        )

    return cast(list[Any], rows)


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
    dataset: OpenFemaDatasetConfig,
    fetch: OpenFemaFetchConfig,
    fetched_at: datetime,
    record_count: int,
) -> SourceSnapshot:
    snapshot_id = SnapshotId(f"{dataset.source_id}:{dataset.dataset_id}:{fetched_at.isoformat()}")

    return SourceSnapshot(
        snapshot_id=snapshot_id,
        source_id=dataset.source_id,
        dataset_id=dataset.dataset_id,
        jurisdiction=dataset.jurisdiction,
        fetched_at=fetched_at,
        record_count=record_count,
        source_url=_resource_url(dataset),
        fetch_params=_snapshot_fetch_params(fetch),
    )


def _resource_url(dataset: OpenFemaDatasetConfig) -> str:
    return f"{dataset.base_url}{dataset.version}/{dataset.entity}"


def _page_params(
    *,
    fetch: OpenFemaFetchConfig,
    skip: int,
    include_count: bool,
) -> dict[str, str | int]:
    params: dict[str, str | int] = {
        "$top": fetch.page_size,
        "$skip": skip,
    }

    if include_count:
        params["$count"] = "true"

    if fetch.order_by is not None:
        params["$orderby"] = fetch.order_by

    if fetch.filter_expr is not None:
        params["$filter"] = fetch.filter_expr

    if fetch.select is not None:
        params["$select"] = fetch.select

    return params


def _snapshot_fetch_params(fetch: OpenFemaFetchConfig) -> dict[str, str]:
    params: dict[str, str] = {"$top": str(fetch.page_size)}

    if fetch.order_by is not None:
        params["$orderby"] = fetch.order_by

    if fetch.filter_expr is not None:
        params["$filter"] = fetch.filter_expr

    if fetch.select is not None:
        params["$select"] = fetch.select

    return params
