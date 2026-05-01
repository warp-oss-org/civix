"""Calgary business-licences source adapter.

Fetches Calgary's Socrata SODA "Calgary Business Licenses" dataset.
The adapter makes a count probe with SoQL, then walks record pages with
`$limit`/`$offset`. Socrata-computed region fields are transport
artifacts and are stripped before records enter Civix snapshots.
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

DEFAULT_BASE_URL: Final[str] = "https://data.calgary.ca/resource/"
SOURCE_ID: Final[SourceId] = SourceId("calgary-open-data")
DEFAULT_PAGE_SIZE: Final[int] = 50000

_COUNT_FIELD: Final[str] = "count"
_SOCRATA_COMPUTED_REGION_PREFIX: Final[str] = ":@computed_region_"


@dataclass(frozen=True, slots=True)
class CalgaryBusinessLicencesAdapter:
    """Fetches Calgary's business-licences dataset over Socrata SODA."""

    dataset_id: DatasetId
    jurisdiction: Jurisdiction
    client: httpx.AsyncClient
    clock: Clock = field(default=utc_now)
    base_url: str = DEFAULT_BASE_URL
    page_size: int = DEFAULT_PAGE_SIZE
    app_token: str | None = None

    def __post_init__(self) -> None:
        if self.page_size <= 0:
            raise ValueError("page_size must be greater than zero")

    @property
    def source_id(self) -> SourceId:
        return SOURCE_ID

    async def fetch(self) -> FetchResult:
        fetched_at = self.clock()
        total = await self._fetch_total_count()
        first_page = await self._fetch_page(offset=0)
        snapshot = self._build_snapshot(fetched_at=fetched_at, record_count=total)

        return FetchResult(
            snapshot=snapshot,
            records=self._stream_records(snapshot_id=snapshot.snapshot_id, first_page=first_page),
        )

    async def _fetch_total_count(self) -> int:
        url = self._resource_url()

        try:
            response = await self.client.get(
                url,
                params={"$select": "count(*)"},
                headers=self._headers(),
            )

            response.raise_for_status()

            payload = response.json()
        except httpx.HTTPError as e:
            raise FetchError(
                f"failed to read count from {url}",
                source_id=self.source_id,
                dataset_id=self.dataset_id,
                operation="count",
            ) from e
        except ValueError as e:
            raise FetchError(
                f"non-JSON response from {url}",
                source_id=self.source_id,
                dataset_id=self.dataset_id,
                operation="count",
            ) from e

        return self._read_count(payload)

    async def _fetch_page(self, *, offset: int) -> list[Any]:
        url = self._resource_url()

        try:
            response = await self.client.get(
                url,
                params={"$limit": self.page_size, "$offset": offset},
                headers=self._headers(),
            )

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

        if not isinstance(payload, list):
            raise FetchError(
                f"non-list JSON body from {url}",
                source_id=self.source_id,
                dataset_id=self.dataset_id,
                operation="fetch-page",
            )

        return cast(list[Any], payload)

    async def _stream_records(
        self,
        *,
        snapshot_id: SnapshotId,
        first_page: list[Any],
    ) -> AsyncIterable[RawRecord]:
        page = first_page
        offset = 0

        while True:
            if not page:
                return

            for row in page:
                yield self._build_record(snapshot_id=snapshot_id, row=row)

            offset += len(page)

            if len(page) < self.page_size:
                return

            page = await self._fetch_page(offset=offset)

    def _build_record(self, *, snapshot_id: SnapshotId, row: Any) -> RawRecord:
        if not isinstance(row, dict):
            raise FetchError(
                "Socrata returned a non-object record",
                source_id=self.source_id,
                dataset_id=self.dataset_id,
                operation="stream-records",
            )

        row_dict = cast(dict[str, Any], row)
        raw_data = {
            name: value
            for name, value in row_dict.items()
            if not name.startswith(_SOCRATA_COMPUTED_REGION_PREFIX)
        }
        source_record_id = raw_data.get("getbusid")

        try:
            return RawRecord(
                snapshot_id=snapshot_id,
                raw_data=raw_data,
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

    def _read_count(self, payload: object) -> int:
        if not isinstance(payload, list):
            raise FetchError(
                "missing or invalid count response",
                source_id=self.source_id,
                dataset_id=self.dataset_id,
                operation="count",
            )

        rows = cast(list[Any], payload)

        if len(rows) != 1:
            raise FetchError(
                "missing or invalid count response",
                source_id=self.source_id,
                dataset_id=self.dataset_id,
                operation="count",
            )

        count_row = rows[0]

        if not isinstance(count_row, dict):
            raise FetchError(
                "missing or invalid count row",
                source_id=self.source_id,
                dataset_id=self.dataset_id,
                operation="count",
            )

        raw_count = cast(dict[str, Any], count_row).get(_COUNT_FIELD)

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
            source_id=self.source_id,
            dataset_id=self.dataset_id,
            operation="count",
        )

    def _build_snapshot(self, *, fetched_at: datetime, record_count: int) -> SourceSnapshot:
        snapshot_id = SnapshotId(f"{self.source_id}:{self.dataset_id}:{fetched_at.isoformat()}")

        return SourceSnapshot(
            snapshot_id=snapshot_id,
            source_id=self.source_id,
            dataset_id=self.dataset_id,
            jurisdiction=self.jurisdiction,
            fetched_at=fetched_at,
            record_count=record_count,
            source_url=self._resource_url(),
        )

    def _resource_url(self) -> str:
        return f"{self.base_url}{self.dataset_id}.json"

    def _headers(self) -> dict[str, str] | None:
        if self.app_token is None:
            return None

        return {"X-App-Token": self.app_token}
