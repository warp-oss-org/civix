"""Tests for the NYC traffic volume counts source slice."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from civix.core.drift import SchemaObserver, TaxonomyDriftKind, TaxonomyObserver
from civix.core.identity.models.identifiers import SnapshotId
from civix.core.mapping.errors import MappingError
from civix.core.pipeline import attach_observers, run
from civix.core.ports.models.adapter import SourceAdapter
from civix.core.quality.models.fields import FieldQuality
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.domains.mobility_observations.adapters.sources.us.nyc_traffic_volume_counts import (
    DEFAULT_BASE_URL,
    NYC_JURISDICTION,
    NYC_TRAFFIC_VOLUME_COUNTS_DATASET_ID,
    NYC_TRAFFIC_VOLUME_COUNTS_RELEASE_CAVEATS,
    NYC_TRAFFIC_VOLUME_COUNTS_SCHEMA,
    NYC_TRAFFIC_VOLUME_COUNTS_SOURCE_SCOPE,
    NYC_TRAFFIC_VOLUME_COUNTS_TAXONOMIES,
    SOCRATA_ORDER,
    SOURCE_ID,
    NycTrafficVolumeCountMapper,
    NycTrafficVolumeCountsAdapter,
    NycTrafficVolumeSiteMapper,
)
from civix.domains.mobility_observations.models.common import (
    CountMetricType,
    CountUnit,
    ObservationDirection,
    TravelMode,
)
from civix.infra.sources.socrata import SocrataFetchConfig

PINNED_NOW = datetime(2026, 5, 2, 12, 0, tzinfo=UTC)
RESOURCE_URL = f"{DEFAULT_BASE_URL}{NYC_TRAFFIC_VOLUME_COUNTS_DATASET_ID}.json"
FIXTURES = Path(__file__).parent / "fixtures"


def _adapter(client: httpx.AsyncClient, *, page_size: int = 1000) -> NycTrafficVolumeCountsAdapter:
    return NycTrafficVolumeCountsAdapter(
        fetch_config=SocrataFetchConfig(
            client=client,
            clock=lambda: PINNED_NOW,
            page_size=page_size,
            order=SOCRATA_ORDER,
        ),
    )


def _snapshot() -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId("snap-volume"),
        source_id=SOURCE_ID,
        dataset_id=NYC_TRAFFIC_VOLUME_COUNTS_DATASET_ID,
        jurisdiction=NYC_JURISDICTION,
        fetched_at=PINNED_NOW,
        record_count=1,
    )


def _raw(**overrides: Any) -> dict[str, Any]:
    raw = json.loads((FIXTURES / "records_page.json").read_text())[0]
    raw.update(overrides)

    return raw


def _record(**overrides: Any) -> RawRecord:
    snap = _snapshot()

    return RawRecord(
        snapshot_id=snap.snapshot_id,
        raw_data=_raw(**overrides),
        source_record_id="REQ-100",
    )


def _capture_sequence(
    requests: list[httpx.Request],
    responses: list[httpx.Response],
) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)

        return responses.pop(0)

    return handler


class TestAdapter:
    async def test_fetches_records_and_preserves_request_ids(self) -> None:
        count = json.loads((FIXTURES / "count_response.json").read_text())
        rows = json.loads((FIXTURES / "records_page.json").read_text())
        rows[0][":@computed_region_test"] = "drop"
        requests: list[httpx.Request] = []

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=_capture_sequence(
                    requests,
                    [httpx.Response(200, json=count), httpx.Response(200, json=rows)],
                )
            )

            async with httpx.AsyncClient() as client:
                adapter = _adapter(client)
                result = await adapter.fetch()
                records = [record async for record in result.records]

        assert isinstance(adapter, SourceAdapter)
        assert result.snapshot.dataset_id == NYC_TRAFFIC_VOLUME_COUNTS_DATASET_ID
        assert result.snapshot.fetch_params == {"$order": SOCRATA_ORDER}
        assert [record.source_record_id for record in records] == ["REQ-100", "REQ-101"]
        assert requests[1].url.params["$order"] == SOCRATA_ORDER
        assert all(not key.startswith(":@computed_region_") for key in records[0].raw_data)


class TestMapper:
    def test_maps_count_observation(self) -> None:
        result = NycTrafficVolumeCountMapper()(_record(), _snapshot())
        observation = result.record

        assert observation.observation_id == "REQ-100:12345:2026:4:2:8:15:N/B"
        assert observation.site_id == "12345"
        assert observation.value.value == Decimal("42")
        assert observation.metric_type.value is CountMetricType.RAW_COUNT
        assert observation.unit.value is CountUnit.COUNT
        assert observation.travel_mode.value is TravelMode.VEHICLE
        assert observation.direction.value is ObservationDirection.NORTHBOUND
        assert observation.movement_type.quality is FieldQuality.UNMAPPED
        assert "WktGeom" in result.report.unmapped_source_fields

    def test_missing_direction_uses_stable_observation_id_fallback(self) -> None:
        result = NycTrafficVolumeCountMapper()(_record(Direction=None), _snapshot())

        assert result.record.observation_id == "REQ-100:12345:2026:4:2:8:15:unknown-direction"
        assert result.record.direction.quality is FieldQuality.NOT_PROVIDED

    def test_maps_site_record_without_deduplication_policy(self) -> None:
        result = NycTrafficVolumeSiteMapper()(_record(), _snapshot())
        site = result.record

        assert site.site_id == "12345"
        assert site.footprint.value is not None
        assert site.footprint.value.line is not None
        assert site.address.value is not None
        assert site.address.value.locality == "Manhattan"
        assert site.road_names.value == ("Broadway", "West 42 Street", "West 43 Street")
        assert site.movement_type.quality is FieldQuality.UNMAPPED

    def test_negative_count_fails_loudly(self) -> None:
        with pytest.raises(MappingError, match="negative numeric"):
            NycTrafficVolumeCountMapper()(_record(Vol="-1"), _snapshot())


class TestDrift:
    async def test_fixture_drift_clean(self) -> None:
        count = json.loads((FIXTURES / "count_response.json").read_text())
        page = json.loads((FIXTURES / "records_page.json").read_text())

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[httpx.Response(200, json=count), httpx.Response(200, json=page)]
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(_adapter(client), NycTrafficVolumeCountMapper())
                schema_obs = SchemaObserver(spec=NYC_TRAFFIC_VOLUME_COUNTS_SCHEMA)
                taxonomy_obs = TaxonomyObserver(specs=NYC_TRAFFIC_VOLUME_COUNTS_TAXONOMIES)
                observed = attach_observers(pipeline_result, [schema_obs, taxonomy_obs])
                records = [record async for record in observed.records]

        assert len(records) == 2
        assert schema_obs.finalize(pipeline_result.snapshot).findings == ()
        assert taxonomy_obs.finalize(pipeline_result.snapshot).findings == ()

    async def test_unknown_direction_surfaces_as_taxonomy_drift(self) -> None:
        count = json.loads((FIXTURES / "count_response.json").read_text())
        page = json.loads((FIXTURES / "records_page.json").read_text())
        page[0]["Direction"] = "UPTOWNISH"

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[httpx.Response(200, json=count), httpx.Response(200, json=page)]
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(_adapter(client), NycTrafficVolumeCountMapper())
                taxonomy_obs = TaxonomyObserver(specs=NYC_TRAFFIC_VOLUME_COUNTS_TAXONOMIES)
                observed = attach_observers(pipeline_result, [taxonomy_obs])
                async for _ in observed.records:
                    pass

        report = taxonomy_obs.finalize(pipeline_result.snapshot)

        assert any(
            finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE for finding in report.findings
        )


def test_source_metadata_preserves_scope_and_caveats() -> None:
    assert SOURCE_ID == "nyc-open-data"
    assert NYC_TRAFFIC_VOLUME_COUNTS_DATASET_ID == "7ym2-wayt"
    assert "automated traffic volume" in NYC_TRAFFIC_VOLUME_COUNTS_SOURCE_SCOPE.casefold()
    assert any("annualized" in caveat for caveat in NYC_TRAFFIC_VOLUME_COUNTS_RELEASE_CAVEATS)
