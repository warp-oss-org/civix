"""Shared Socrata SODA fetch loop."""

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

DEFAULT_PAGE_SIZE: Final[int] = 50000
SOCRATA_DEFAULT_ORDER: Final[str] = ":id"
SOCRATA_COUNT_FIELD: Final[str] = "count"
SOCRATA_COMPUTED_REGION_PREFIX: Final[str] = ":@computed_region_"


@dataclass(frozen=True, slots=True)
class SocrataDatasetConfig:
    """Stable identity and endpoint details for one Socrata dataset."""

    source_id: SourceId
    dataset_id: DatasetId
    jurisdiction: Jurisdiction
    base_url: str
    source_record_id_fields: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class SocrataFetchConfig:
    """Runtime fetch options for one Socrata dataset snapshot."""

    client: httpx.AsyncClient
    clock: Clock = field(default=utc_now)
    page_size: int = DEFAULT_PAGE_SIZE
    app_token: str | None = None
    where: str | None = None
    order: str = SOCRATA_DEFAULT_ORDER
    select: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.page_size <= 0:
            raise ValueError("page_size must be greater than zero")


@dataclass(frozen=True, slots=True)
class SocrataSourceAdapter:
    """Configured source adapter for one Socrata dataset."""

    dataset: SocrataDatasetConfig
    fetch_config: SocrataFetchConfig

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
        return await fetch_socrata_dataset(dataset=self.dataset, fetch=self.fetch_config)


async def fetch_socrata_dataset(
    *,
    dataset: SocrataDatasetConfig,
    fetch: SocrataFetchConfig,
) -> FetchResult:
    """Fetch a Socrata dataset snapshot and return lazy raw records."""
    fetched_at = fetch.clock()
    total = await _fetch_total_count(dataset=dataset, fetch=fetch)
    first_page = await _fetch_page(dataset=dataset, fetch=fetch, offset=0)
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
        ),
    )


async def _fetch_total_count(
    *,
    dataset: SocrataDatasetConfig,
    fetch: SocrataFetchConfig,
) -> int:
    url = _resource_url(dataset)
    params = _count_params(fetch)

    try:
        response = await fetch.client.get(url, params=params, headers=_headers(fetch))

        response.raise_for_status()

        payload = response.json()
    except httpx.HTTPError as e:
        raise FetchError(
            f"failed to read count from {url}",
            source_id=dataset.source_id,
            dataset_id=dataset.dataset_id,
            operation="count",
        ) from e
    except ValueError as e:
        raise FetchError(
            f"non-JSON response from {url}",
            source_id=dataset.source_id,
            dataset_id=dataset.dataset_id,
            operation="count",
        ) from e

    return _read_count(payload=payload, dataset=dataset)


async def _fetch_page(
    *,
    dataset: SocrataDatasetConfig,
    fetch: SocrataFetchConfig,
    offset: int,
) -> list[Any]:
    url = _resource_url(dataset)

    try:
        response = await fetch.client.get(
            url,
            params=_page_params(fetch=fetch, offset=offset),
            headers=_headers(fetch),
        )

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

    if not isinstance(payload, list):
        raise FetchError(
            f"non-list JSON body from {url}",
            source_id=dataset.source_id,
            dataset_id=dataset.dataset_id,
            operation="fetch-page",
        )

    return cast(list[Any], payload)


async def _stream_records(
    *,
    dataset: SocrataDatasetConfig,
    fetch: SocrataFetchConfig,
    snapshot: SourceSnapshot,
    first_page: list[Any],
) -> AsyncIterable[RawRecord]:
    page = first_page
    offset = 0

    while True:
        if not page:
            return

        for row in page:
            yield _build_record(dataset=dataset, snapshot_id=snapshot.snapshot_id, row=row)

        offset += len(page)

        if len(page) < fetch.page_size:
            return

        page = await _fetch_page(dataset=dataset, fetch=fetch, offset=offset)


def _build_record(
    *,
    dataset: SocrataDatasetConfig,
    snapshot_id: SnapshotId,
    row: Any,
) -> RawRecord:
    if not isinstance(row, dict):
        raise FetchError(
            "Socrata returned a non-object record",
            source_id=dataset.source_id,
            dataset_id=dataset.dataset_id,
            operation="stream-records",
        )

    row_dict = cast(dict[str, Any], row)
    raw_data = {
        name: value
        for name, value in row_dict.items()
        if not name.startswith(SOCRATA_COMPUTED_REGION_PREFIX)
    }
    source_record_id = _source_record_id(raw_data, dataset.source_record_id_fields)

    try:
        return RawRecord(
            snapshot_id=snapshot_id,
            raw_data=raw_data,
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


def _read_count(*, payload: object, dataset: SocrataDatasetConfig) -> int:
    if not isinstance(payload, list):
        raise FetchError(
            "missing or invalid count response",
            source_id=dataset.source_id,
            dataset_id=dataset.dataset_id,
            operation="count",
        )

    rows = cast(list[Any], payload)

    if len(rows) != 1:
        raise FetchError(
            "missing or invalid count response",
            source_id=dataset.source_id,
            dataset_id=dataset.dataset_id,
            operation="count",
        )

    count_row = rows[0]

    if not isinstance(count_row, dict):
        raise FetchError(
            "missing or invalid count row",
            source_id=dataset.source_id,
            dataset_id=dataset.dataset_id,
            operation="count",
        )

    raw_count = cast(dict[str, Any], count_row).get(SOCRATA_COUNT_FIELD)

    if isinstance(raw_count, int) and raw_count >= 0:
        return raw_count

    if isinstance(raw_count, str):
        try:
            parsed = int(raw_count)
        except ValueError:
            parsed = -1

        if parsed >= 0:
            return parsed

    raise FetchError(
        "missing or invalid count in Socrata response",
        source_id=dataset.source_id,
        dataset_id=dataset.dataset_id,
        operation="count",
    )


def _build_snapshot(
    *,
    dataset: SocrataDatasetConfig,
    fetch: SocrataFetchConfig,
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


def _resource_url(dataset: SocrataDatasetConfig) -> str:
    return f"{dataset.base_url}{dataset.dataset_id}.json"


def _count_params(fetch: SocrataFetchConfig) -> dict[str, str]:
    params = {"$select": "count(*)"}

    if fetch.where is not None:
        params["$where"] = fetch.where

    return params


def _page_params(*, fetch: SocrataFetchConfig, offset: int) -> dict[str, str | int]:
    params: dict[str, str | int] = {
        "$limit": fetch.page_size,
        "$offset": offset,
        "$order": fetch.order,
    }

    if fetch.where is not None:
        params["$where"] = fetch.where

    if fetch.select:
        params["$select"] = ",".join(fetch.select)

    return params


def _snapshot_fetch_params(fetch: SocrataFetchConfig) -> dict[str, str]:
    params = {"$order": fetch.order}

    if fetch.where is not None:
        params["$where"] = fetch.where

    if fetch.select:
        params["$select"] = ",".join(fetch.select)
        params["$limit"] = str(fetch.page_size)

    return params


def _headers(fetch: SocrataFetchConfig) -> dict[str, str] | None:
    if fetch.app_token is None:
        return None

    return {"X-App-Token": fetch.app_token}
