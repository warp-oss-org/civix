"""FEMA National Flood Hazard Layer source adapter."""

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
from civix.domains.hazard_risk.adapters.sources.us.fema_nfhl.schema import (
    FEMA_NFHL_FLOOD_HAZARD_ZONES_OUT_FIELDS,
)

SOURCE_ID: Final[SourceId] = SourceId("fema-arcgis")
US_JURISDICTION: Final[Jurisdiction] = Jurisdiction(country="US")
FEMA_NFHL_FLOOD_HAZARD_ZONES_DATASET_ID: Final[DatasetId] = DatasetId("NFHL_Flood_Hazard_Zones")
FEMA_NFHL_FLOOD_HAZARD_ZONES_SERVICE_URL: Final[str] = (
    "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer/28"
)
FEMA_NFHL_FLOOD_HAZARD_ZONES_QUERY_URL: Final[str] = (
    f"{FEMA_NFHL_FLOOD_HAZARD_ZONES_SERVICE_URL}/query"
)
FEMA_NFHL_FLOOD_HAZARD_ZONES_LAYER_NAME: Final[str] = "Flood Hazard Zones"
FEMA_NFHL_FLOOD_HAZARD_ZONES_ORDER: Final[str] = "OBJECTID"
FEMA_NFHL_FLOOD_HAZARD_ZONES_SOURCE_CRS: Final[str] = "EPSG:4269"
FEMA_NFHL_SOURCE_SCOPE: Final[str] = (
    "FEMA National Flood Hazard Layer effective Flood Hazard Zones published "
    "through the public NFHL ArcGIS MapServer layer 28."
)
DEFAULT_PAGE_SIZE: Final[int] = 2000


@dataclass(frozen=True, slots=True)
class FemaNfhlFloodHazardZonesFetchConfig:
    """Runtime fetch options for a FEMA NFHL Flood Hazard Zones query snapshot.

    FEMA publishes this layer through a MapServer feature layer. The adapter
    relies on the current layer metadata advertising pagination and advanced
    query support; ordering by `OBJECTID` keeps pagination deterministic.
    """

    client: httpx.AsyncClient
    clock: Clock = field(default=utc_now)
    page_size: int = DEFAULT_PAGE_SIZE
    where: str = "1=1"
    out_fields: tuple[str, ...] = FEMA_NFHL_FLOOD_HAZARD_ZONES_OUT_FIELDS
    order_by: str = FEMA_NFHL_FLOOD_HAZARD_ZONES_ORDER

    def __post_init__(self) -> None:
        if self.page_size <= 0:
            raise ValueError("page_size must be greater than zero")


@dataclass(frozen=True, slots=True)
class FemaNfhlFloodHazardZonesAdapter:
    """Fetches FEMA NFHL Flood Hazard Zone rows from the ArcGIS MapServer."""

    fetch_config: FemaNfhlFloodHazardZonesFetchConfig

    @property
    def source_id(self) -> SourceId:
        return SOURCE_ID

    @property
    def dataset_id(self) -> DatasetId:
        return FEMA_NFHL_FLOOD_HAZARD_ZONES_DATASET_ID

    @property
    def jurisdiction(self) -> Jurisdiction:
        return US_JURISDICTION

    async def fetch(self) -> FetchResult:
        return await fetch_fema_nfhl_flood_hazard_zones(self.fetch_config)


async def fetch_fema_nfhl_flood_hazard_zones(
    fetch: FemaNfhlFloodHazardZonesFetchConfig,
) -> FetchResult:
    """Fetch FEMA NFHL Flood Hazard Zone rows and return traceable raw records."""
    fetched_at = fetch.clock()
    total = await _fetch_count(fetch)
    first_page = await _fetch_page(fetch=fetch, offset=0)
    snapshot = _build_snapshot(fetch=fetch, fetched_at=fetched_at, record_count=total)

    return FetchResult(
        snapshot=snapshot,
        records=_stream_records(fetch=fetch, snapshot=snapshot, first_page=first_page, total=total),
    )


async def _fetch_count(fetch: FemaNfhlFloodHazardZonesFetchConfig) -> int:
    try:
        response = await fetch.client.get(
            FEMA_NFHL_FLOOD_HAZARD_ZONES_QUERY_URL,
            params=_count_params(fetch),
        )

        response.raise_for_status()

        payload = response.json()
    except httpx.HTTPError as e:
        raise FetchError(
            f"failed to read count from {FEMA_NFHL_FLOOD_HAZARD_ZONES_QUERY_URL}",
            source_id=SOURCE_ID,
            dataset_id=FEMA_NFHL_FLOOD_HAZARD_ZONES_DATASET_ID,
            operation="count",
        ) from e
    except ValueError as e:
        raise FetchError(
            f"non-JSON count response from {FEMA_NFHL_FLOOD_HAZARD_ZONES_QUERY_URL}",
            source_id=SOURCE_ID,
            dataset_id=FEMA_NFHL_FLOOD_HAZARD_ZONES_DATASET_ID,
            operation="count",
        ) from e

    if not isinstance(payload, dict):
        raise FetchError(
            "non-object count response from FEMA NFHL ArcGIS query",
            source_id=SOURCE_ID,
            dataset_id=FEMA_NFHL_FLOOD_HAZARD_ZONES_DATASET_ID,
            operation="count",
        )

    payload_dict = cast(dict[str, Any], payload)
    count = payload_dict.get("count")
    if isinstance(count, int) and count >= 0:
        return count

    raise FetchError(
        "missing or invalid count in FEMA NFHL ArcGIS response",
        source_id=SOURCE_ID,
        dataset_id=FEMA_NFHL_FLOOD_HAZARD_ZONES_DATASET_ID,
        operation="count",
    )


async def _fetch_page(
    *,
    fetch: FemaNfhlFloodHazardZonesFetchConfig,
    offset: int,
) -> dict[str, Any]:
    try:
        response = await fetch.client.get(
            FEMA_NFHL_FLOOD_HAZARD_ZONES_QUERY_URL,
            params=_page_params(fetch=fetch, offset=offset),
        )

        response.raise_for_status()

        payload = response.json()
    except httpx.HTTPError as e:
        raise FetchError(
            f"failed to read FEMA NFHL records at offset={offset}",
            source_id=SOURCE_ID,
            dataset_id=FEMA_NFHL_FLOOD_HAZARD_ZONES_DATASET_ID,
            operation="fetch-page",
        ) from e
    except ValueError as e:
        raise FetchError(
            f"non-JSON records response from {FEMA_NFHL_FLOOD_HAZARD_ZONES_QUERY_URL}",
            source_id=SOURCE_ID,
            dataset_id=FEMA_NFHL_FLOOD_HAZARD_ZONES_DATASET_ID,
            operation="fetch-page",
        ) from e

    if not isinstance(payload, dict):
        raise FetchError(
            "non-object records response from FEMA NFHL ArcGIS query",
            source_id=SOURCE_ID,
            dataset_id=FEMA_NFHL_FLOOD_HAZARD_ZONES_DATASET_ID,
            operation="fetch-page",
        )

    return cast(dict[str, Any], payload)


async def _stream_records(
    *,
    fetch: FemaNfhlFloodHazardZonesFetchConfig,
    snapshot: SourceSnapshot,
    first_page: dict[str, Any],
    total: int,
) -> AsyncIterable[RawRecord]:
    page = first_page
    offset = 0

    while True:
        features = _read_features(page)

        if not features:
            return

        for feature in features:
            yield _build_record(snapshot.snapshot_id, feature)

        offset += len(features)

        if offset >= total:
            return

        if len(features) < fetch.page_size:
            return

        page = await _fetch_page(fetch=fetch, offset=offset)


def _read_features(payload: dict[str, Any]) -> list[Any]:
    features = payload.get("features")

    if not isinstance(features, list):
        raise FetchError(
            "missing or invalid features list in FEMA NFHL ArcGIS response",
            source_id=SOURCE_ID,
            dataset_id=FEMA_NFHL_FLOOD_HAZARD_ZONES_DATASET_ID,
            operation="stream-records",
        )

    return cast(list[Any], features)


def _build_record(snapshot_id: SnapshotId, feature: Any) -> RawRecord:
    if not isinstance(feature, dict):
        raise FetchError(
            "FEMA NFHL ArcGIS returned a non-object feature",
            source_id=SOURCE_ID,
            dataset_id=FEMA_NFHL_FLOOD_HAZARD_ZONES_DATASET_ID,
            operation="stream-records",
        )

    attributes = cast(dict[str, Any], feature).get("attributes")
    if not isinstance(attributes, dict):
        raise FetchError(
            "FEMA NFHL ArcGIS feature is missing attributes",
            source_id=SOURCE_ID,
            dataset_id=FEMA_NFHL_FLOOD_HAZARD_ZONES_DATASET_ID,
            operation="stream-records",
        )

    raw_data = cast(dict[str, Any], attributes)
    source_record_id = raw_data.get("FLD_AR_ID")
    # NFHL zone identity is load-bearing for both raw traceability and
    # normalized zone keys, so this slice is stricter than older adapters.
    if not isinstance(source_record_id, str) or not source_record_id.strip():
        raise FetchError(
            "FEMA NFHL raw record is missing FLD_AR_ID",
            source_id=SOURCE_ID,
            dataset_id=FEMA_NFHL_FLOOD_HAZARD_ZONES_DATASET_ID,
            operation="stream-records",
        )

    try:
        return RawRecord(
            snapshot_id=snapshot_id,
            raw_data=raw_data,
            source_record_id=source_record_id.strip(),
            source_updated_at=None,
        )
    except ValidationError as e:
        raise FetchError(
            "FEMA NFHL raw record failed validation",
            source_id=SOURCE_ID,
            dataset_id=FEMA_NFHL_FLOOD_HAZARD_ZONES_DATASET_ID,
            operation="stream-records",
        ) from e


def _build_snapshot(
    *,
    fetch: FemaNfhlFloodHazardZonesFetchConfig,
    fetched_at: datetime,
    record_count: int,
) -> SourceSnapshot:
    snapshot_id = SnapshotId(
        f"{SOURCE_ID}:{FEMA_NFHL_FLOOD_HAZARD_ZONES_DATASET_ID}:{fetched_at.isoformat()}"
    )

    return SourceSnapshot(
        snapshot_id=snapshot_id,
        source_id=SOURCE_ID,
        dataset_id=FEMA_NFHL_FLOOD_HAZARD_ZONES_DATASET_ID,
        jurisdiction=US_JURISDICTION,
        fetched_at=fetched_at,
        record_count=record_count,
        source_url=FEMA_NFHL_FLOOD_HAZARD_ZONES_QUERY_URL,
        fetch_params=_snapshot_fetch_params(fetch),
    )


def _count_params(fetch: FemaNfhlFloodHazardZonesFetchConfig) -> dict[str, str]:
    return {
        "f": "json",
        "where": fetch.where,
        "returnCountOnly": "true",
    }


def _page_params(
    *,
    fetch: FemaNfhlFloodHazardZonesFetchConfig,
    offset: int,
) -> dict[str, str | int]:
    return {
        "f": "json",
        "where": fetch.where,
        "outFields": ",".join(fetch.out_fields),
        "returnGeometry": "false",
        "orderByFields": fetch.order_by,
        "resultOffset": offset,
        "resultRecordCount": fetch.page_size,
    }


def _snapshot_fetch_params(fetch: FemaNfhlFloodHazardZonesFetchConfig) -> dict[str, str]:
    return {
        "where": fetch.where,
        "outFields": ",".join(fetch.out_fields),
        "returnGeometry": "false",
        "orderByFields": fetch.order_by,
        "resultRecordCount": str(fetch.page_size),
    }
