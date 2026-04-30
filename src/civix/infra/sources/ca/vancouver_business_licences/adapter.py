"""Vancouver business-licences source adapter.

Fetches Vancouver Open Data Portal datasets backed by OpenDataSoft v2.1.
Three datasets are reachable through this adapter, distinguished only
by `dataset_id`:

- `business-licences` — current licences.
- `business-licences-2013-to-2024` — historical, includes the
  May 6 2024 taxonomy consolidation boundary.
- `business-licences-1997-to-2012` — older historical.

Each fetch makes two requests: a count probe so the snapshot can be
built eagerly, then a streaming JSONL export so large datasets can be
consumed without offset-based pagination.

The adapter does not normalize. It surfaces the source's own record
identifier and extract timestamp as `RawRecord.source_record_id` and
`source_updated_at`; everything else is left in `raw_data` for the
mapper to interpret.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Final, cast

import httpx
from pydantic import ValidationError

from civix.core.adapters import FetchError, FetchResult
from civix.core.identity import DatasetId, Jurisdiction, SnapshotId, SourceId
from civix.core.snapshots import RawRecord, SourceSnapshot
from civix.core.temporal import Clock, utc_now

DEFAULT_BASE_URL: Final[str] = "https://opendata.vancouver.ca/api/explore/v2.1/"
SOURCE_ID: Final[SourceId] = SourceId("vancouver-open-data")


def _parse_extract_date(value: object) -> datetime | None:
    """Parse the portal's `extractdate` into a UTC datetime, or None.

    The portal serves ISO 8601 strings, sometimes with offset, sometimes
    without. We accept tz-aware values and convert to UTC; tz-naive or
    unparseable values become None rather than failing the whole record
    over a metadata field.
    """
    if not isinstance(value, str) or not value:
        return None

    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return None

    return parsed.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class VancouverBusinessLicencesAdapter:
    """Fetches one Vancouver business-licences dataset.

    Construct with the `httpx.AsyncClient` you want it to use; reuse
    across fetches if appropriate. The clock is injected so tests can
    pin `fetched_at` deterministically.
    """

    dataset_id: DatasetId
    jurisdiction: Jurisdiction
    client: httpx.AsyncClient
    clock: Clock = field(default=utc_now)
    base_url: str = DEFAULT_BASE_URL

    @property
    def source_id(self) -> SourceId:
        return SOURCE_ID

    async def fetch(self) -> FetchResult:
        fetched_at = self.clock()
        total_count = await self._fetch_total_count()
        snapshot = self._build_snapshot(fetched_at=fetched_at, record_count=total_count)

        return FetchResult(
            snapshot=snapshot,
            records=self._stream_records(snapshot_id=snapshot.snapshot_id),
        )

    async def _fetch_total_count(self) -> int:
        url = self._records_url()

        try:
            response = await self.client.get(url, params={"limit": 0})

            response.raise_for_status()

            payload = response.json()
        except httpx.HTTPError as e:
            raise FetchError(
                f"failed to read total_count from {url}",
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

        if not isinstance(payload, dict):
            raise FetchError(
                f"non-object JSON body from {url}",
                source_id=self.source_id,
                dataset_id=self.dataset_id,
                operation="count",
            )

        total_count = cast(dict[str, Any], payload).get("total_count")

        if not isinstance(total_count, int) or total_count < 0:
            raise FetchError(
                f"missing or invalid total_count in response from {url}",
                source_id=self.source_id,
                dataset_id=self.dataset_id,
                operation="count",
            )

        return total_count

    async def _stream_records(self, *, snapshot_id: SnapshotId) -> AsyncIterable[RawRecord]:
        url = self._exports_url()

        try:
            async with self.client.stream("GET", url) as response:
                response.raise_for_status()

                async for line in response.aiter_lines():
                    if not line:
                        continue

                    yield self._build_record(snapshot_id=snapshot_id, line=line)
        except httpx.HTTPError as e:
            raise FetchError(
                f"failed to stream records from {url}",
                source_id=self.source_id,
                dataset_id=self.dataset_id,
                operation="stream-records",
            ) from e

    def _build_record(self, *, snapshot_id: SnapshotId, line: str) -> RawRecord:
        try:
            row = json.loads(line)
        except json.JSONDecodeError as e:
            raise FetchError(
                "failed to parse JSONL line from exports stream",
                source_id=self.source_id,
                dataset_id=self.dataset_id,
                operation="stream-records",
            ) from e

        if not isinstance(row, dict):
            raise FetchError(
                "JSONL line was not a JSON object",
                source_id=self.source_id,
                dataset_id=self.dataset_id,
                operation="stream-records",
            )

        row_dict = cast(dict[str, Any], row)
        source_record_id = row_dict.get("licencersn")

        try:
            return RawRecord(
                snapshot_id=snapshot_id,
                raw_data=row_dict,
                source_record_id=str(source_record_id) if source_record_id else None,
                source_updated_at=_parse_extract_date(row_dict.get("extractdate")),
            )
        except ValidationError as e:
            raise FetchError(
                "raw record failed validation",
                source_id=self.source_id,
                dataset_id=self.dataset_id,
                operation="stream-records",
            ) from e

    def _build_snapshot(self, *, fetched_at: datetime, record_count: int) -> SourceSnapshot:
        snapshot_id = SnapshotId(f"{self.source_id}:{self.dataset_id}:{fetched_at.isoformat()}")

        return SourceSnapshot(
            snapshot_id=snapshot_id,
            source_id=self.source_id,
            dataset_id=self.dataset_id,
            jurisdiction=self.jurisdiction,
            fetched_at=fetched_at,
            record_count=record_count,
            source_url=self._exports_url(),
        )

    def _records_url(self) -> str:
        return f"{self.base_url}catalog/datasets/{self.dataset_id}/records"

    def _exports_url(self) -> str:
        return f"{self.base_url}catalog/datasets/{self.dataset_id}/exports/jsonl"
