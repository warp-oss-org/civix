"""Tests for the Chicago Traffic Tracker regions source slice."""

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

from civix.core.drift import SchemaObserver
from civix.core.identity.models.identifiers import SnapshotId
from civix.core.mapping.errors import MappingError
from civix.core.pipeline import attach_observers, run
from civix.core.ports.models.adapter import SourceAdapter
from civix.core.quality.models.fields import FieldQuality
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.spatial.models.geometry import BoundingBox
from civix.domains.mobility_observations.adapters.sources.us.chicago_traffic_tracker_regions import (
    CHICAGO_JURISDICTION,
    CHICAGO_TRAFFIC_TRACKER_REGIONS_DATASET_ID,
    CHICAGO_TRAFFIC_TRACKER_REGIONS_RELEASE_CAVEATS,
    CHICAGO_TRAFFIC_TRACKER_REGIONS_SCHEMA,
    CHICAGO_TRAFFIC_TRACKER_REGIONS_SOURCE_SCOPE,
    DEFAULT_BASE_URL,
    REFRESH_NOT_INTERVAL_CAVEAT_CODE,
    REGIONAL_ROLLUP_CAVEAT_CODE,
    SOCRATA_ORDER,
    SOURCE_ID,
    ChicagoTrafficTrackerRegionsAdapter,
    ChicagoTrafficTrackerRegionSiteMapper,
    ChicagoTrafficTrackerRegionSpeedMapper,
)
from civix.domains.mobility_observations.models.common import (
    AggregationWindow,
    MeasurementMethod,
    MobilitySiteKind,
    SpeedMetricType,
    SpeedUnit,
    TravelMode,
)
from civix.infra.sources.socrata import SocrataFetchConfig

PINNED_NOW = datetime(2026, 5, 2, 12, 0, tzinfo=UTC)
RESOURCE_URL = f"{DEFAULT_BASE_URL}{CHICAGO_TRAFFIC_TRACKER_REGIONS_DATASET_ID}.json"
FIXTURES = Path(__file__).parent / "fixtures"


def _adapter(
    client: httpx.AsyncClient, *, page_size: int = 1000
) -> ChicagoTrafficTrackerRegionsAdapter:
    return ChicagoTrafficTrackerRegionsAdapter(
        fetch_config=SocrataFetchConfig(
            client=client,
            clock=lambda: PINNED_NOW,
            page_size=page_size,
            order=SOCRATA_ORDER,
        ),
    )


def _snapshot() -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId("snap-chicago-regions"),
        source_id=SOURCE_ID,
        dataset_id=CHICAGO_TRAFFIC_TRACKER_REGIONS_DATASET_ID,
        jurisdiction=CHICAGO_JURISDICTION,
        fetched_at=PINNED_NOW,
        record_count=2,
    )


def _raw(index: int = 0, **overrides: Any) -> dict[str, Any]:
    raw = json.loads((FIXTURES / "records_page.json").read_text())[index]
    raw.update(overrides)

    return raw


def _record(index: int = 0, **overrides: Any) -> RawRecord:
    snap = _snapshot()
    raw = _raw(index, **overrides)

    return RawRecord(
        snapshot_id=snap.snapshot_id,
        raw_data=raw,
        source_record_id=str(raw["_region_id"]),
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
    async def test_fetches_records_and_preserves_region_ids(self) -> None:
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
        assert result.snapshot.dataset_id == CHICAGO_TRAFFIC_TRACKER_REGIONS_DATASET_ID
        assert [record.source_record_id for record in records] == ["1", "2"]
        assert requests[1].url.params["$order"] == SOCRATA_ORDER
        assert all(not key.startswith(":@computed_region_") for key in records[0].raw_data)


class TestSiteMapper:
    def test_maps_region_site(self) -> None:
        result = ChicagoTrafficTrackerRegionSiteMapper()(_record(), _snapshot())
        site = result.record

        assert site.site_id == "1"
        assert site.kind.value is MobilitySiteKind.REGION
        assert site.measurement_method.value is MeasurementMethod.BUS_GPS_ESTIMATE

        footprint = site.footprint.value

        assert footprint is not None
        assert isinstance(footprint.bounding_box, BoundingBox)
        assert footprint.bounding_box.west == -87.709
        assert footprint.bounding_box.east == -87.654
        assert footprint.bounding_box.south == 41.997
        assert footprint.bounding_box.north == 42.024

        assert site.road_names.value == ("Rogers Park - West Ridge", "North side neighborhoods")
        assert site.address.value is not None
        assert site.address.value.locality == "Chicago"

    def test_invalid_bounding_box_raises(self) -> None:
        with pytest.raises(MappingError, match="invalid region bounding box"):
            ChicagoTrafficTrackerRegionSiteMapper()(
                _record(west="-87.0", east="-88.0"), _snapshot()
            )


class TestSpeedMapper:
    def test_maps_region_speed(self) -> None:
        result = ChicagoTrafficTrackerRegionSpeedMapper()(_record(), _snapshot())
        observation = result.record

        assert observation.observation_id == "1:2026-04-20T09:55:21"
        assert observation.site_id == "1"
        assert observation.travel_mode.value is TravelMode.MIXED_TRAFFIC
        assert observation.measurement_method.value is MeasurementMethod.BUS_GPS_ESTIMATE
        assert observation.aggregation_window.value is AggregationWindow.SOURCE_SPECIFIC

        period = observation.period.value

        assert period is not None
        assert period.timezone == "America/Chicago"
        assert period.datetime_value == datetime(2026, 4, 20, 9, 55, 21)

        assert len(observation.metrics) == 1
        metric = observation.metrics[0]

        assert metric.metric_type.value is SpeedMetricType.OBSERVED_SPEED
        assert metric.unit.value is SpeedUnit.MILES_PER_HOUR
        assert metric.value.value == Decimal("26.5")

        caveats = observation.source_caveats.value

        assert caveats is not None
        codes = {caveat.code for caveat in caveats}

        assert REGIONAL_ROLLUP_CAVEAT_CODE in codes
        assert REFRESH_NOT_INTERVAL_CAVEAT_CODE in codes

    def test_current_speed_sentinel_maps_to_not_provided(self) -> None:
        result = ChicagoTrafficTrackerRegionSpeedMapper()(_record(current_speed="-1"), _snapshot())
        metric = result.record.metrics[0]

        assert metric.value.value is None
        assert metric.value.quality is FieldQuality.NOT_PROVIDED

    def test_other_negative_current_speed_raises(self) -> None:
        with pytest.raises(MappingError, match="negative numeric"):
            ChicagoTrafficTrackerRegionSpeedMapper()(_record(current_speed="-3.5"), _snapshot())

    def test_invalid_last_updt_raises(self) -> None:
        with pytest.raises(MappingError, match="invalid datetime"):
            ChicagoTrafficTrackerRegionSpeedMapper()(_record(_last_updt="garbage"), _snapshot())


class TestDrift:
    async def test_fixture_schema_drift_clean(self) -> None:
        count = json.loads((FIXTURES / "count_response.json").read_text())
        page = json.loads((FIXTURES / "records_page.json").read_text())

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[httpx.Response(200, json=count), httpx.Response(200, json=page)]
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(
                    _adapter(client), ChicagoTrafficTrackerRegionSpeedMapper()
                )
                schema_obs = SchemaObserver(spec=CHICAGO_TRAFFIC_TRACKER_REGIONS_SCHEMA)
                observed = attach_observers(pipeline_result, [schema_obs])
                async for _ in observed.records:
                    pass

        assert schema_obs.finalize(pipeline_result.snapshot).findings == ()


def test_source_metadata_preserves_scope_and_caveats() -> None:
    assert SOURCE_ID == "chicago-data-portal"
    assert CHICAGO_TRAFFIC_TRACKER_REGIONS_DATASET_ID == "t2qc-9pjd"
    assert "region" in CHICAGO_TRAFFIC_TRACKER_REGIONS_SOURCE_SCOPE.casefold()
    assert any(
        "rollup" in caveat.casefold() for caveat in CHICAGO_TRAFFIC_TRACKER_REGIONS_RELEASE_CAVEATS
    )
