"""FEMA National Risk Index source adapter."""

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
from civix.domains.hazard_risk.adapters.sources.us.fema_nri.schema import (
    FEMA_NRI_TRACTS_OUT_FIELDS,
)

SOURCE_ID: Final[SourceId] = SourceId("fema-arcgis")
US_JURISDICTION: Final[Jurisdiction] = Jurisdiction(country="US")
FEMA_NRI_TRACTS_DATASET_ID: Final[DatasetId] = DatasetId("National_Risk_Index_Census_Tracts")
FEMA_NRI_TRACTS_SERVICE_URL: Final[str] = (
    "https://services.arcgis.com/XG15cJAlne2vxtgt/arcgis/rest/services/"
    "National_Risk_Index_Census_Tracts/FeatureServer/0"
)
FEMA_NRI_TRACTS_QUERY_URL: Final[str] = f"{FEMA_NRI_TRACTS_SERVICE_URL}/query"
FEMA_NRI_TRACTS_LAYER_NAME: Final[str] = "NRI_CensusTracts_Prod"
FEMA_NRI_TRACTS_ORDER: Final[str] = "NRI_ID"
FEMA_NRI_TRACTS_SOURCE_CRS: Final[str] = "EPSG:3857"
FEMA_NRI_SOURCE_SCOPE: Final[str] = (
    "FEMA National Risk Index Census tract-level hazard-risk scores published "
    "through ArcGIS feature services."
)
DEFAULT_PAGE_SIZE: Final[int] = 2000


@dataclass(frozen=True, slots=True)
class FemaNriTractsFetchConfig:
    """Runtime fetch options for a FEMA NRI Census Tracts query snapshot."""

    client: httpx.AsyncClient
    clock: Clock = field(default=utc_now)
    page_size: int = DEFAULT_PAGE_SIZE
    where: str = "1=1"
    out_fields: tuple[str, ...] = FEMA_NRI_TRACTS_OUT_FIELDS
    order_by: str = FEMA_NRI_TRACTS_ORDER

    def __post_init__(self) -> None:
        if self.page_size <= 0:
            raise ValueError("page_size must be greater than zero")


@dataclass(frozen=True, slots=True)
class FemaNriTractsAdapter:
    """Fetches FEMA NRI Census tract rows from the ArcGIS feature service."""

    fetch_config: FemaNriTractsFetchConfig

    @property
    def source_id(self) -> SourceId:
        return SOURCE_ID

    @property
    def dataset_id(self) -> DatasetId:
        return FEMA_NRI_TRACTS_DATASET_ID

    @property
    def jurisdiction(self) -> Jurisdiction:
        return US_JURISDICTION

    async def fetch(self) -> FetchResult:
        return await fetch_fema_nri_tracts(self.fetch_config)


async def fetch_fema_nri_tracts(fetch: FemaNriTractsFetchConfig) -> FetchResult:
    """Fetch FEMA NRI tract rows and return traceable raw records."""
    fetched_at = fetch.clock()
    total = await _fetch_count(fetch)
    first_page = await _fetch_page(fetch=fetch, offset=0)
    snapshot = _build_snapshot(fetch=fetch, fetched_at=fetched_at, record_count=total)

    return FetchResult(
        snapshot=snapshot,
        records=_stream_records(fetch=fetch, snapshot=snapshot, first_page=first_page, total=total),
    )


async def _fetch_count(fetch: FemaNriTractsFetchConfig) -> int:
    try:
        response = await fetch.client.get(FEMA_NRI_TRACTS_QUERY_URL, params=_count_params(fetch))

        response.raise_for_status()

        payload = response.json()
    except httpx.HTTPError as e:
        raise FetchError(
            f"failed to read count from {FEMA_NRI_TRACTS_QUERY_URL}",
            source_id=SOURCE_ID,
            dataset_id=FEMA_NRI_TRACTS_DATASET_ID,
            operation="count",
        ) from e
    except ValueError as e:
        raise FetchError(
            f"non-JSON count response from {FEMA_NRI_TRACTS_QUERY_URL}",
            source_id=SOURCE_ID,
            dataset_id=FEMA_NRI_TRACTS_DATASET_ID,
            operation="count",
        ) from e

    if not isinstance(payload, dict):
        raise FetchError(
            "non-object count response from FEMA NRI ArcGIS query",
            source_id=SOURCE_ID,
            dataset_id=FEMA_NRI_TRACTS_DATASET_ID,
            operation="count",
        )

    payload_dict = cast(dict[str, Any], payload)
    count = payload_dict.get("count")
    if isinstance(count, int) and count >= 0:
        return count

    raise FetchError(
        "missing or invalid count in FEMA NRI ArcGIS response",
        source_id=SOURCE_ID,
        dataset_id=FEMA_NRI_TRACTS_DATASET_ID,
        operation="count",
    )


async def _fetch_page(*, fetch: FemaNriTractsFetchConfig, offset: int) -> dict[str, Any]:
    try:
        response = await fetch.client.get(
            FEMA_NRI_TRACTS_QUERY_URL,
            params=_page_params(fetch=fetch, offset=offset),
        )

        response.raise_for_status()

        payload = response.json()
    except httpx.HTTPError as e:
        raise FetchError(
            f"failed to read FEMA NRI records at offset={offset}",
            source_id=SOURCE_ID,
            dataset_id=FEMA_NRI_TRACTS_DATASET_ID,
            operation="fetch-page",
        ) from e
    except ValueError as e:
        raise FetchError(
            f"non-JSON records response from {FEMA_NRI_TRACTS_QUERY_URL}",
            source_id=SOURCE_ID,
            dataset_id=FEMA_NRI_TRACTS_DATASET_ID,
            operation="fetch-page",
        ) from e

    if not isinstance(payload, dict):
        raise FetchError(
            "non-object records response from FEMA NRI ArcGIS query",
            source_id=SOURCE_ID,
            dataset_id=FEMA_NRI_TRACTS_DATASET_ID,
            operation="fetch-page",
        )

    return cast(dict[str, Any], payload)


async def _stream_records(
    *,
    fetch: FemaNriTractsFetchConfig,
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
            "missing or invalid features list in FEMA NRI ArcGIS response",
            source_id=SOURCE_ID,
            dataset_id=FEMA_NRI_TRACTS_DATASET_ID,
            operation="stream-records",
        )

    return cast(list[Any], features)


def _build_record(snapshot_id: SnapshotId, feature: Any) -> RawRecord:
    if not isinstance(feature, dict):
        raise FetchError(
            "FEMA NRI ArcGIS returned a non-object feature",
            source_id=SOURCE_ID,
            dataset_id=FEMA_NRI_TRACTS_DATASET_ID,
            operation="stream-records",
        )

    attributes = cast(dict[str, Any], feature).get("attributes")
    if not isinstance(attributes, dict):
        raise FetchError(
            "FEMA NRI ArcGIS feature is missing attributes",
            source_id=SOURCE_ID,
            dataset_id=FEMA_NRI_TRACTS_DATASET_ID,
            operation="stream-records",
        )

    raw_data = cast(dict[str, Any], attributes)
    source_record_id = raw_data.get("NRI_ID")

    try:
        return RawRecord(
            snapshot_id=snapshot_id,
            raw_data=raw_data,
            source_record_id=str(source_record_id) if source_record_id is not None else None,
            source_updated_at=None,
        )
    except ValidationError as e:
        raise FetchError(
            "FEMA NRI raw record failed validation",
            source_id=SOURCE_ID,
            dataset_id=FEMA_NRI_TRACTS_DATASET_ID,
            operation="stream-records",
        ) from e


def _build_snapshot(
    *,
    fetch: FemaNriTractsFetchConfig,
    fetched_at: datetime,
    record_count: int,
) -> SourceSnapshot:
    snapshot_id = SnapshotId(f"{SOURCE_ID}:{FEMA_NRI_TRACTS_DATASET_ID}:{fetched_at.isoformat()}")

    return SourceSnapshot(
        snapshot_id=snapshot_id,
        source_id=SOURCE_ID,
        dataset_id=FEMA_NRI_TRACTS_DATASET_ID,
        jurisdiction=US_JURISDICTION,
        fetched_at=fetched_at,
        record_count=record_count,
        source_url=FEMA_NRI_TRACTS_QUERY_URL,
        fetch_params=_snapshot_fetch_params(fetch),
    )


def _count_params(fetch: FemaNriTractsFetchConfig) -> dict[str, str]:
    return {
        "f": "json",
        "where": fetch.where,
        "returnCountOnly": "true",
    }


def _page_params(*, fetch: FemaNriTractsFetchConfig, offset: int) -> dict[str, str | int]:
    return {
        "f": "json",
        "where": fetch.where,
        "outFields": ",".join(fetch.out_fields),
        "returnGeometry": "false",
        "orderByFields": fetch.order_by,
        "resultOffset": offset,
        "resultRecordCount": fetch.page_size,
    }


def _snapshot_fetch_params(fetch: FemaNriTractsFetchConfig) -> dict[str, str]:
    return {
        "where": fetch.where,
        "outFields": ",".join(fetch.out_fields),
        "returnGeometry": "false",
        "orderByFields": fetch.order_by,
        "resultRecordCount": str(fetch.page_size),
    }
