"""Slice-local paginated-JSON client for the DfT road-traffic API.

The DfT API publishes Laravel-style paginated JSON envelopes:

    {"current_page": 1, "last_page": 4252, "next_page_url": "...",
     "per_page": 250, "total": 1062881, "data": [...], ...}

Pagination terminates when `next_page_url` becomes null. This client
intentionally lives inside the source slice rather than under
`civix.infra.sources` until a second DfT consumer arrives — at which
point lift it to `civix.infra.sources.dft`.
"""

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

DEFAULT_BASE_URL: Final[str] = "https://roadtraffic.dft.gov.uk/api/"
DEFAULT_PAGE_SIZE: Final[int] = 250


@dataclass(frozen=True, slots=True)
class DftDatasetConfig:
    """Stable identity and endpoint details for one DfT dataset."""

    source_id: SourceId
    dataset_id: DatasetId
    jurisdiction: Jurisdiction
    endpoint: str
    source_record_id_field: str = "id"
    base_url: str = DEFAULT_BASE_URL


@dataclass(frozen=True, slots=True)
class DftFetchConfig:
    """Runtime fetch options for one DfT dataset snapshot."""

    client: httpx.AsyncClient
    clock: Clock = field(default=utc_now)
    page_size: int = DEFAULT_PAGE_SIZE

    def __post_init__(self) -> None:
        if self.page_size <= 0:
            raise ValueError("page_size must be greater than zero")


@dataclass(frozen=True, slots=True)
class DftSourceAdapter:
    """Configured source adapter for one DfT dataset."""

    dataset: DftDatasetConfig
    fetch_config: DftFetchConfig

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
        return await fetch_dft_dataset(dataset=self.dataset, fetch=self.fetch_config)


async def fetch_dft_dataset(*, dataset: DftDatasetConfig, fetch: DftFetchConfig) -> FetchResult:
    """Fetch a DfT dataset snapshot and return lazy raw records."""
    fetched_at = fetch.clock()
    first_page = await _fetch_page(dataset=dataset, fetch=fetch, page=1)
    total = _read_total(page=first_page, dataset=dataset)
    snapshot = _build_snapshot(
        dataset=dataset,
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


async def _fetch_page(
    *,
    dataset: DftDatasetConfig,
    fetch: DftFetchConfig,
    page: int,
) -> dict[str, Any]:
    url = _endpoint_url(dataset)
    params: dict[str, str | int] = {
        "page[number]": page,
        "page[size]": fetch.page_size,
    }

    try:
        response = await fetch.client.get(url, params=params)

        response.raise_for_status()

        payload = response.json()
    except httpx.HTTPError as e:
        raise FetchError(
            f"failed to read records from {url} at page={page}",
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
    dataset: DftDatasetConfig,
    fetch: DftFetchConfig,
    snapshot: SourceSnapshot,
    first_page: dict[str, Any],
) -> AsyncIterable[RawRecord]:
    page = first_page

    while True:
        rows = _read_data(page=page, dataset=dataset)

        if not rows:
            return

        for row in rows:
            yield _build_record(dataset=dataset, snapshot_id=snapshot.snapshot_id, row=row)

        next_url = page.get("next_page_url")

        if not isinstance(next_url, str) or not next_url:
            return

        next_page = page.get("current_page")

        if not isinstance(next_page, int) or next_page < 1:
            raise FetchError(
                "missing or invalid current_page in DfT response",
                source_id=dataset.source_id,
                dataset_id=dataset.dataset_id,
                operation="fetch-page",
            )

        page = await _fetch_page(dataset=dataset, fetch=fetch, page=next_page + 1)


def _build_record(
    *,
    dataset: DftDatasetConfig,
    snapshot_id: SnapshotId,
    row: Any,
) -> RawRecord:
    if not isinstance(row, dict):
        raise FetchError(
            "DfT returned a non-object record",
            source_id=dataset.source_id,
            dataset_id=dataset.dataset_id,
            operation="stream-records",
        )

    row_dict = cast(dict[str, Any], row)
    raw_id = row_dict.get(dataset.source_record_id_field)
    source_record_id = str(raw_id) if raw_id is not None and str(raw_id).strip() else None

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


def _read_total(*, page: dict[str, Any], dataset: DftDatasetConfig) -> int:
    total = page.get("total")

    if not isinstance(total, int) or total < 0:
        raise FetchError(
            "missing or invalid total in DfT response",
            source_id=dataset.source_id,
            dataset_id=dataset.dataset_id,
            operation="fetch-page",
        )

    return total


def _read_data(*, page: dict[str, Any], dataset: DftDatasetConfig) -> list[Any]:
    data = page.get("data")

    if not isinstance(data, list):
        raise FetchError(
            "missing or invalid data array in DfT response",
            source_id=dataset.source_id,
            dataset_id=dataset.dataset_id,
            operation="stream-records",
        )

    return cast(list[Any], data)


def _build_snapshot(
    *,
    dataset: DftDatasetConfig,
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
        source_url=_endpoint_url(dataset),
        fetch_params={"endpoint": dataset.endpoint},
    )


def _endpoint_url(dataset: DftDatasetConfig) -> str:
    return f"{dataset.base_url}{dataset.endpoint}"
