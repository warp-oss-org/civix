"""Tests for the NYC bicycle/pedestrian counts source slice."""

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
from civix.core.identity.models.identifiers import DatasetId, SnapshotId
from civix.core.mapping.errors import MappingError
from civix.core.pipeline import attach_observers, run
from civix.core.ports.models.adapter import SourceAdapter
from civix.core.quality.models.fields import FieldQuality
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.domains.mobility_observations.adapters.sources.us.nyc_bicycle_pedestrian_counts import (
    DEFAULT_BASE_URL,
    NYC_BICYCLE_PEDESTRIAN_COUNTS_DATASET_ID,
    NYC_BICYCLE_PEDESTRIAN_COUNTS_SCHEMA,
    NYC_BICYCLE_PEDESTRIAN_COUNTS_TAXONOMIES,
    NYC_BICYCLE_PEDESTRIAN_RELEASE_CAVEATS,
    NYC_BICYCLE_PEDESTRIAN_SENSORS_DATASET_ID,
    NYC_BICYCLE_PEDESTRIAN_SENSORS_SCHEMA,
    NYC_BICYCLE_PEDESTRIAN_SENSORS_TAXONOMIES,
    NYC_BICYCLE_PEDESTRIAN_SOURCE_SCOPE,
    NYC_JURISDICTION,
    SOCRATA_ORDER,
    SOURCE_ID,
    NycBicyclePedestrianCountMapper,
    NycBicyclePedestrianCountsAdapter,
    NycBicyclePedestrianSensorMapper,
    NycBicyclePedestrianSensorsAdapter,
)
from civix.domains.mobility_observations.models.common import (
    AggregationWindow,
    CountMetricType,
    ObservationDirection,
    TravelMode,
)
from civix.infra.sources.socrata import SocrataFetchConfig

PINNED_NOW = datetime(2026, 5, 2, 12, 0, tzinfo=UTC)
COUNT_RESOURCE_URL = f"{DEFAULT_BASE_URL}{NYC_BICYCLE_PEDESTRIAN_COUNTS_DATASET_ID}.json"
SENSOR_RESOURCE_URL = f"{DEFAULT_BASE_URL}{NYC_BICYCLE_PEDESTRIAN_SENSORS_DATASET_ID}.json"
FIXTURES = Path(__file__).parent / "fixtures"


def _count_adapter(
    client: httpx.AsyncClient,
    *,
    page_size: int = 1000,
) -> NycBicyclePedestrianCountsAdapter:
    return NycBicyclePedestrianCountsAdapter(
        fetch_config=SocrataFetchConfig(
            client=client,
            clock=lambda: PINNED_NOW,
            page_size=page_size,
            order=SOCRATA_ORDER,
        ),
    )


def _sensor_adapter(
    client: httpx.AsyncClient,
    *,
    page_size: int = 1000,
) -> NycBicyclePedestrianSensorsAdapter:
    return NycBicyclePedestrianSensorsAdapter(
        fetch_config=SocrataFetchConfig(
            client=client,
            clock=lambda: PINNED_NOW,
            page_size=page_size,
            order=SOCRATA_ORDER,
        ),
    )


def _snapshot(dataset_id: DatasetId) -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId(f"snap-{dataset_id}"),
        source_id=SOURCE_ID,
        dataset_id=dataset_id,
        jurisdiction=NYC_JURISDICTION,
        fetched_at=PINNED_NOW,
        record_count=1,
    )


def _count_raw(**overrides: Any) -> dict[str, Any]:
    raw = json.loads((FIXTURES / "count_records_page.json").read_text())[0]
    raw.update(overrides)

    return raw


def _sensor_raw(**overrides: Any) -> dict[str, Any]:
    raw = json.loads((FIXTURES / "sensor_records_page.json").read_text())[0]
    raw.update(overrides)

    return raw


def _count_record(**overrides: Any) -> RawRecord:
    snap = _snapshot(NYC_BICYCLE_PEDESTRIAN_COUNTS_DATASET_ID)

    return RawRecord(
        snapshot_id=snap.snapshot_id,
        raw_data=_count_raw(**overrides),
        source_record_id="eco-100",
    )


def _sensor_record(**overrides: Any) -> RawRecord:
    snap = _snapshot(NYC_BICYCLE_PEDESTRIAN_SENSORS_DATASET_ID)

    return RawRecord(
        snapshot_id=snap.snapshot_id,
        raw_data=_sensor_raw(**overrides),
        source_record_id="eco-100",
    )


def _capture_sequence(
    requests: list[httpx.Request],
    responses: list[httpx.Response],
) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)

        return responses.pop(0)

    return handler


class TestAdapters:
    async def test_count_adapter_fetches_records(self) -> None:
        count = json.loads((FIXTURES / "count_response.json").read_text())
        rows = json.loads((FIXTURES / "count_records_page.json").read_text())
        requests: list[httpx.Request] = []

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(COUNT_RESOURCE_URL).mock(
                side_effect=_capture_sequence(
                    requests,
                    [httpx.Response(200, json=count), httpx.Response(200, json=rows)],
                )
            )

            async with httpx.AsyncClient() as client:
                adapter = _count_adapter(client)
                result = await adapter.fetch()
                records = [record async for record in result.records]

        assert isinstance(adapter, SourceAdapter)
        assert result.snapshot.dataset_id == NYC_BICYCLE_PEDESTRIAN_COUNTS_DATASET_ID
        assert [record.source_record_id for record in records] == [None, None]
        assert requests[1].url.params["$order"] == SOCRATA_ORDER

    async def test_sensor_adapter_fetches_records(self) -> None:
        count = json.loads((FIXTURES / "count_response.json").read_text())
        rows = json.loads((FIXTURES / "sensor_records_page.json").read_text())

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(SENSOR_RESOURCE_URL).mock(
                side_effect=[httpx.Response(200, json=count), httpx.Response(200, json=rows)]
            )

            async with httpx.AsyncClient() as client:
                adapter = _sensor_adapter(client)
                result = await adapter.fetch()
                records = [record async for record in result.records]

        assert isinstance(adapter, SourceAdapter)
        assert result.snapshot.dataset_id == NYC_BICYCLE_PEDESTRIAN_SENSORS_DATASET_ID
        assert [record.source_record_id for record in records] == ["eco-100", "eco-101"]


class TestMappers:
    def test_maps_count_observation(self) -> None:
        result = NycBicyclePedestrianCountMapper()(
            _count_record(),
            _snapshot(NYC_BICYCLE_PEDESTRIAN_COUNTS_DATASET_ID),
        )
        observation = result.record

        assert observation.observation_id == (
            "eco-100:flow-nb:Bicycle:2026-04-03T08:15:00:15 minutes:Northbound"
        )
        assert observation.site_id == "eco-100"
        assert observation.travel_mode.value is TravelMode.BICYCLE
        assert observation.direction.value is ObservationDirection.NORTHBOUND
        assert observation.aggregation_window.value is AggregationWindow.RAW_INTERVAL
        assert observation.metric_type.value is CountMetricType.RAW_COUNT
        assert observation.value.value == Decimal("16")
        assert observation.source_caveats.value is not None
        assert observation.source_caveats.value[0].code == "valid"
        assert "flowName" in result.report.unmapped_source_fields

    def test_missing_direction_uses_stable_observation_id_fallback(self) -> None:
        result = NycBicyclePedestrianCountMapper()(
            _count_record(direction=None),
            _snapshot(NYC_BICYCLE_PEDESTRIAN_COUNTS_DATASET_ID),
        )

        assert result.record.observation_id == (
            "eco-100:flow-nb:Bicycle:2026-04-03T08:15:00:15 minutes:unknown-direction"
        )
        assert result.record.direction.quality is FieldQuality.NOT_PROVIDED

    def test_unknown_travel_mode_is_unmapped_not_other(self) -> None:
        result = NycBicyclePedestrianCountMapper()(
            _count_record(travelMode="Hoverboard"),
            _snapshot(NYC_BICYCLE_PEDESTRIAN_COUNTS_DATASET_ID),
        )

        assert result.record.travel_mode.value is None
        assert result.record.travel_mode.quality is FieldQuality.UNMAPPED
        assert "travelMode" in result.report.unmapped_source_fields

    def test_maps_sensor_site(self) -> None:
        result = NycBicyclePedestrianSensorMapper()(
            _sensor_record(),
            _snapshot(NYC_BICYCLE_PEDESTRIAN_SENSORS_DATASET_ID),
        )
        site = result.record

        assert site.site_id == "eco-100"
        assert site.footprint.value is not None
        assert site.footprint.value.point is not None
        assert site.direction.value is ObservationDirection.NORTHBOUND
        assert site.source_caveats.value is not None

    def test_negative_count_fails_loudly(self) -> None:
        with pytest.raises(MappingError, match="negative numeric"):
            NycBicyclePedestrianCountMapper()(
                _count_record(counts="-1"),
                _snapshot(NYC_BICYCLE_PEDESTRIAN_COUNTS_DATASET_ID),
            )


class TestDrift:
    async def test_count_fixture_drift_clean(self) -> None:
        count = json.loads((FIXTURES / "count_response.json").read_text())
        page = json.loads((FIXTURES / "count_records_page.json").read_text())

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(COUNT_RESOURCE_URL).mock(
                side_effect=[httpx.Response(200, json=count), httpx.Response(200, json=page)]
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(
                    _count_adapter(client),
                    NycBicyclePedestrianCountMapper(),
                )
                schema_obs = SchemaObserver(spec=NYC_BICYCLE_PEDESTRIAN_COUNTS_SCHEMA)
                taxonomy_obs = TaxonomyObserver(specs=NYC_BICYCLE_PEDESTRIAN_COUNTS_TAXONOMIES)
                observed = attach_observers(pipeline_result, [schema_obs, taxonomy_obs])
                records = [record async for record in observed.records]

        assert len(records) == 2
        assert schema_obs.finalize(pipeline_result.snapshot).findings == ()
        assert taxonomy_obs.finalize(pipeline_result.snapshot).findings == ()

    async def test_sensor_fixture_drift_clean(self) -> None:
        count = json.loads((FIXTURES / "count_response.json").read_text())
        page = json.loads((FIXTURES / "sensor_records_page.json").read_text())

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(SENSOR_RESOURCE_URL).mock(
                side_effect=[httpx.Response(200, json=count), httpx.Response(200, json=page)]
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(
                    _sensor_adapter(client),
                    NycBicyclePedestrianSensorMapper(),
                )
                schema_obs = SchemaObserver(spec=NYC_BICYCLE_PEDESTRIAN_SENSORS_SCHEMA)
                taxonomy_obs = TaxonomyObserver(specs=NYC_BICYCLE_PEDESTRIAN_SENSORS_TAXONOMIES)
                observed = attach_observers(pipeline_result, [schema_obs, taxonomy_obs])
                records = [record async for record in observed.records]

        assert len(records) == 2
        assert schema_obs.finalize(pipeline_result.snapshot).findings == ()
        assert taxonomy_obs.finalize(pipeline_result.snapshot).findings == ()

    async def test_unknown_travel_mode_surfaces_as_taxonomy_drift(self) -> None:
        count = json.loads((FIXTURES / "count_response.json").read_text())
        page = json.loads((FIXTURES / "count_records_page.json").read_text())
        page[0]["travelMode"] = "Hoverboard"

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(COUNT_RESOURCE_URL).mock(
                side_effect=[httpx.Response(200, json=count), httpx.Response(200, json=page)]
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(
                    _count_adapter(client),
                    NycBicyclePedestrianCountMapper(),
                )
                taxonomy_obs = TaxonomyObserver(specs=NYC_BICYCLE_PEDESTRIAN_COUNTS_TAXONOMIES)
                observed = attach_observers(pipeline_result, [taxonomy_obs])
                async for _ in observed.records:
                    pass

        report = taxonomy_obs.finalize(pipeline_result.snapshot)

        assert any(
            finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE for finding in report.findings
        )


def test_source_metadata_preserves_scope_and_caveats() -> None:
    assert SOURCE_ID == "nyc-open-data"
    assert NYC_BICYCLE_PEDESTRIAN_COUNTS_DATASET_ID == "ct66-47at"
    assert NYC_BICYCLE_PEDESTRIAN_SENSORS_DATASET_ID == "6up2-gnw8"
    assert "sensor" in NYC_BICYCLE_PEDESTRIAN_SOURCE_SCOPE.casefold()
    assert any(
        "sensor metadata" in caveat.casefold() for caveat in NYC_BICYCLE_PEDESTRIAN_RELEASE_CAVEATS
    )
