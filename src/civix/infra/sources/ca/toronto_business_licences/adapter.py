"""Toronto business-licences source adapter.

Fetches the Toronto Open Data Portal "Municipal Licensing and Standards —
Business Licences and Permits" dataset over CKAN's `datastore_search`
JSON endpoint.

Each fetch resolves the dataset's currently-active datastore resource
through `package_show`, then walks `datastore_search` pages by
`limit`/`offset` until a page returns no more rows. The first page also
carries the dataset's `total`, which is captured into the snapshot
eagerly so consumers don't need to drain the iterator before reading
metadata.

The adapter does not normalize. `Licence No.` is surfaced as
`RawRecord.source_record_id` because it is the source's own stable
identifier. Toronto's `Last Record Update` is a date-only string with no
time component, so `RawRecord.source_updated_at` is left unset rather
than fabricating a UTC datetime. The full CKAN record, including CKAN's
`_id` row index, stays in `raw_data` for reproducibility and drift
inspection.
"""

from __future__ import annotations

from collections.abc import AsyncIterable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Final, cast

import httpx
from pydantic import ValidationError

from civix.core.adapters import FetchError, FetchResult
from civix.core.identity import DatasetId, Jurisdiction, SnapshotId, SourceId
from civix.core.snapshots import RawRecord, SourceSnapshot
from civix.core.temporal import Clock, utc_now

DEFAULT_BASE_URL: Final[str] = "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/"
SOURCE_ID: Final[SourceId] = SourceId("toronto-open-data")
DEFAULT_PAGE_SIZE: Final[int] = 1000


@dataclass(frozen=True, slots=True)
class TorontoBusinessLicencesAdapter:
    """Fetches Toronto's business-licences dataset over CKAN.

    Construct with the `httpx.AsyncClient` you want it to use; reuse
    across fetches if appropriate. The clock is injected so tests can
    pin `fetched_at` deterministically.

    `dataset_id` is the CKAN package slug (e.g.
    "municipal-licensing-and-standards-business-licences-and-permits").
    The active datastore resource is discovered at fetch time so the
    adapter survives Toronto re-loading the resource under a new UUID.
    """

    dataset_id: DatasetId
    jurisdiction: Jurisdiction
    client: httpx.AsyncClient
    clock: Clock = field(default=utc_now)
    base_url: str = DEFAULT_BASE_URL
    page_size: int = DEFAULT_PAGE_SIZE

    def __post_init__(self) -> None:
        if self.page_size <= 0:
            raise ValueError("page_size must be greater than zero")

    @property
    def source_id(self) -> SourceId:
        return SOURCE_ID

    async def fetch(self) -> FetchResult:
        fetched_at = self.clock()
        resource_id = await self._resolve_resource_id()
        first_page = await self._fetch_page(resource_id=resource_id, offset=0)
        total = self._read_total(first_page)
        snapshot = self._build_snapshot(
            fetched_at=fetched_at, record_count=total, resource_id=resource_id
        )

        return FetchResult(
            snapshot=snapshot,
            records=self._stream_records(
                snapshot_id=snapshot.snapshot_id,
                resource_id=resource_id,
                first_page=first_page,
                total=total,
            ),
        )

    async def _resolve_resource_id(self) -> str:
        url = f"{self.base_url}package_show"

        try:
            response = await self.client.get(url, params={"id": self.dataset_id})
            response.raise_for_status()
            payload = response.json()
        except httpx.HTTPError as e:
            raise FetchError(
                f"failed to read package metadata from {url}",
                source_id=self.source_id,
                dataset_id=self.dataset_id,
                operation="resolve-resource",
            ) from e
        except ValueError as e:
            raise FetchError(
                f"non-JSON response from {url}",
                source_id=self.source_id,
                dataset_id=self.dataset_id,
                operation="resolve-resource",
            ) from e

        result = self._unwrap_ckan_result(payload, url=url, operation="resolve-resource")
        resources = result.get("resources")

        if not isinstance(resources, list):
            raise FetchError(
                f"missing or invalid resources list in package metadata from {url}",
                source_id=self.source_id,
                dataset_id=self.dataset_id,
                operation="resolve-resource",
            )

        for entry in cast(list[Any], resources):
            if not isinstance(entry, dict):
                continue

            resource = cast(dict[str, Any], entry)

            if resource.get("datastore_active") is True:
                resource_id = resource.get("id")

                if isinstance(resource_id, str) and resource_id:
                    return resource_id

        raise FetchError(
            "no datastore-active resource found for dataset",
            source_id=self.source_id,
            dataset_id=self.dataset_id,
            operation="resolve-resource",
        )

    async def _fetch_page(self, *, resource_id: str, offset: int) -> dict[str, Any]:
        url = f"{self.base_url}datastore_search"
        params: dict[str, str | int] = {
            "resource_id": resource_id,
            "limit": self.page_size,
            "offset": offset,
        }

        try:
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            payload = response.json()
        except httpx.HTTPError as e:
            raise FetchError(
                f"failed to read records from {url} at offset={offset}",
                source_id=self.source_id,
                dataset_id=self.dataset_id,
                operation="fetch-page",
            ) from e
        except ValueError as e:
            raise FetchError(
                f"non-JSON response from {url}",
                source_id=self.source_id,
                dataset_id=self.dataset_id,
                operation="fetch-page",
            ) from e

        return self._unwrap_ckan_result(payload, url=url, operation="fetch-page")

    def _read_total(self, page: dict[str, Any]) -> int:
        total = page.get("total")

        if not isinstance(total, int) or total < 0:
            raise FetchError(
                "missing or invalid total in datastore_search response",
                source_id=self.source_id,
                dataset_id=self.dataset_id,
                operation="fetch-page",
            )

        return total

    async def _stream_records(
        self,
        *,
        snapshot_id: SnapshotId,
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
                    source_id=self.source_id,
                    dataset_id=self.dataset_id,
                    operation="stream-records",
                )

            rows = cast(list[Any], records)

            if not rows:
                return

            for row in rows:
                yield self._build_record(snapshot_id=snapshot_id, row=row)

            offset += len(rows)

            if offset >= total:
                return

            if len(rows) < self.page_size:
                return

            page = await self._fetch_page(resource_id=resource_id, offset=offset)

    def _build_record(self, *, snapshot_id: SnapshotId, row: Any) -> RawRecord:
        if not isinstance(row, dict):
            raise FetchError(
                "datastore_search returned a non-object record",
                source_id=self.source_id,
                dataset_id=self.dataset_id,
                operation="stream-records",
            )

        row_dict = cast(dict[str, Any], row)
        source_record_id = row_dict.get("Licence No.")

        try:
            return RawRecord(
                snapshot_id=snapshot_id,
                raw_data=row_dict,
                source_record_id=str(source_record_id) if source_record_id else None,
                source_updated_at=None,
            )
        except ValidationError as e:
            raise FetchError(
                "raw record failed validation",
                source_id=self.source_id,
                dataset_id=self.dataset_id,
                operation="stream-records",
            ) from e

    def _unwrap_ckan_result(self, payload: object, *, url: str, operation: str) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise FetchError(
                f"non-object JSON body from {url}",
                source_id=self.source_id,
                dataset_id=self.dataset_id,
                operation=operation,
            )

        envelope = cast(dict[str, Any], payload)

        if envelope.get("success") is not True:
            raise FetchError(
                f"CKAN reported success=False for {url}",
                source_id=self.source_id,
                dataset_id=self.dataset_id,
                operation=operation,
            )

        result = envelope.get("result")

        if not isinstance(result, dict):
            raise FetchError(
                f"missing or invalid result object in CKAN response from {url}",
                source_id=self.source_id,
                dataset_id=self.dataset_id,
                operation=operation,
            )

        return cast(dict[str, Any], result)

    def _build_snapshot(
        self, *, fetched_at: datetime, record_count: int, resource_id: str
    ) -> SourceSnapshot:
        snapshot_id = SnapshotId(f"{self.source_id}:{self.dataset_id}:{fetched_at.isoformat()}")

        return SourceSnapshot(
            snapshot_id=snapshot_id,
            source_id=self.source_id,
            dataset_id=self.dataset_id,
            jurisdiction=self.jurisdiction,
            fetched_at=fetched_at,
            record_count=record_count,
            source_url=f"{self.base_url}datastore_search?resource_id={resource_id}",
        )
