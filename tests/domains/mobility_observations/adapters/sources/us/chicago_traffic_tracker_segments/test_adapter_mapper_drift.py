"""Tests for the Chicago Traffic Tracker segments source slice."""

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
from civix.core.spatial.models.geometry import LineString
from civix.domains.mobility_observations.adapters.sources.us.chicago_traffic_tracker_segments import (
    CHICAGO_JURISDICTION,
    CHICAGO_TRAFFIC_TRACKER_SEGMENTS_DATASET_ID,
    CHICAGO_TRAFFIC_TRACKER_SEGMENTS_RELEASE_CAVEATS,
    CHICAGO_TRAFFIC_TRACKER_SEGMENTS_SCHEMA,
    CHICAGO_TRAFFIC_TRACKER_SEGMENTS_SOURCE_SCOPE,
    CHICAGO_TRAFFIC_TRACKER_SEGMENTS_TAXONOMIES,
    DEFAULT_BASE_URL,
    REFRESH_NOT_INTERVAL_CAVEAT_CODE,
    SOCRATA_ORDER,
    SOURCE_ID,
    ChicagoTrafficTrackerSegmentsAdapter,
    ChicagoTrafficTrackerSegmentSiteMapper,
    ChicagoTrafficTrackerSegmentSpeedMapper,
)
from civix.domains.mobility_observations.models.common import (
    AggregationWindow,
    MeasurementMethod,
    MobilitySiteKind,
    ObservationDirection,
    SpeedMetricType,
    SpeedUnit,
    TravelMode,
)
from civix.infra.sources.socrata import SocrataFetchConfig

PINNED_NOW = datetime(2026, 5, 2, 12, 0, tzinfo=UTC)
RESOURCE_URL = f"{DEFAULT_BASE_URL}{CHICAGO_TRAFFIC_TRACKER_SEGMENTS_DATASET_ID}.json"
FIXTURES = Path(__file__).parent / "fixtures"


def _adapter(
    client: httpx.AsyncClient, *, page_size: int = 1000
) -> ChicagoTrafficTrackerSegmentsAdapter:
    return ChicagoTrafficTrackerSegmentsAdapter(
        fetch_config=SocrataFetchConfig(
            client=client,
            clock=lambda: PINNED_NOW,
            page_size=page_size,
            order=SOCRATA_ORDER,
        ),
    )


def _snapshot() -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId("snap-chicago-segments"),
        source_id=SOURCE_ID,
        dataset_id=CHICAGO_TRAFFIC_TRACKER_SEGMENTS_DATASET_ID,
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
        source_record_id=str(raw["segmentid"]),
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
    async def test_fetches_records_and_preserves_segment_ids(self) -> None:
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
        assert result.snapshot.dataset_id == CHICAGO_TRAFFIC_TRACKER_SEGMENTS_DATASET_ID
        assert [record.source_record_id for record in records] == ["1", "2"]
        assert requests[1].url.params["$order"] == SOCRATA_ORDER
        assert all(not key.startswith(":@computed_region_") for key in records[0].raw_data)
        assert "_traffic" in records[0].raw_data


class TestSiteMapper:
    def test_maps_segment_site(self) -> None:
        result = ChicagoTrafficTrackerSegmentSiteMapper()(_record(), _snapshot())
        site = result.record

        assert site.site_id == "1"
        assert site.kind.value is MobilitySiteKind.ROAD_SEGMENT
        assert site.measurement_method.value is MeasurementMethod.BUS_GPS_ESTIMATE

        footprint = site.footprint.value

        assert footprint is not None
        assert isinstance(footprint.line, LineString)
        coords = footprint.line.coordinates

        assert len(coords) == 2
        assert coords[0].latitude == 41.997
        assert coords[0].longitude == -87.7264
        assert coords[1].latitude == 41.984
        assert coords[1].longitude == -87.7264

        assert site.direction.value is ObservationDirection.NORTHBOUND
        assert site.direction.quality is FieldQuality.STANDARDIZED
        assert site.road_names.value == ("Pulaski", "Devon", "Bryn Mawr")
        assert site.address.value is not None
        assert site.address.value.locality == "Chicago"
        assert site.address.value.street == "Pulaski"

        assert "_strheading" in result.report.unmapped_source_fields
        assert "_length" in result.report.unmapped_source_fields
        assert "_comments" in result.report.unmapped_source_fields

    def test_diagonal_and_unknown_direction_both_map_to_source_specific(self) -> None:
        # Field-level shape is identical for diagonals and unknown tokens; the
        # asymmetry is only visible through taxonomy drift (see TestDrift).
        for raw_value in ("NE", "???"):
            result = ChicagoTrafficTrackerSegmentSiteMapper()(
                _record(_direction=raw_value), _snapshot()
            )

            assert result.record.direction.value is ObservationDirection.SOURCE_SPECIFIC
            assert result.record.direction.quality is FieldQuality.DERIVED

    def test_invalid_coordinates_raise_mapping_error(self) -> None:
        with pytest.raises(MappingError, match="invalid segment coordinates"):
            ChicagoTrafficTrackerSegmentSiteMapper()(_record(_lif_lat="not-a-number"), _snapshot())


class TestSpeedMapper:
    def test_maps_segment_speed(self) -> None:
        result = ChicagoTrafficTrackerSegmentSpeedMapper()(_record(), _snapshot())
        observation = result.record

        assert observation.observation_id == "1:2026-04-13T10:10:54"
        assert observation.site_id == "1"
        assert observation.travel_mode.value is TravelMode.MIXED_TRAFFIC
        assert observation.measurement_method.value is MeasurementMethod.BUS_GPS_ESTIMATE
        assert observation.aggregation_window.value is AggregationWindow.SOURCE_SPECIFIC

        period = observation.period.value

        assert period is not None
        assert period.timezone == "America/Chicago"
        assert period.datetime_value == datetime(2026, 4, 13, 10, 10, 54)

        assert len(observation.metrics) == 1
        metric = observation.metrics[0]

        assert metric.metric_type.value is SpeedMetricType.OBSERVED_SPEED
        assert metric.unit.value is SpeedUnit.MILES_PER_HOUR
        assert metric.value.value == Decimal("25")
        assert metric.value.quality is FieldQuality.DIRECT

        caveats = observation.source_caveats.value

        assert caveats is not None
        codes = {caveat.code for caveat in caveats}

        assert REFRESH_NOT_INTERVAL_CAVEAT_CODE in codes
        assert "regional-bus-gps-rollup" not in codes

    def test_traffic_sentinel_maps_to_not_provided(self) -> None:
        result = ChicagoTrafficTrackerSegmentSpeedMapper()(_record(index=1), _snapshot())
        metric = result.record.metrics[0]

        assert metric.value.value is None
        assert metric.value.quality is FieldQuality.NOT_PROVIDED
        assert metric.value.source_fields == ("_traffic",)

    def test_other_negative_traffic_value_raises(self) -> None:
        with pytest.raises(MappingError, match="negative numeric"):
            ChicagoTrafficTrackerSegmentSpeedMapper()(_record(_traffic="-2"), _snapshot())

    def test_invalid_last_updt_raises(self) -> None:
        with pytest.raises(MappingError, match="invalid datetime"):
            ChicagoTrafficTrackerSegmentSpeedMapper()(_record(_last_updt="not-a-date"), _snapshot())


class TestDrift:
    async def test_fixture_drift_clean(self) -> None:
        count = json.loads((FIXTURES / "count_response.json").read_text())
        page = json.loads((FIXTURES / "records_page.json").read_text())

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[httpx.Response(200, json=count), httpx.Response(200, json=page)]
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(
                    _adapter(client), ChicagoTrafficTrackerSegmentSpeedMapper()
                )
                schema_obs = SchemaObserver(spec=CHICAGO_TRAFFIC_TRACKER_SEGMENTS_SCHEMA)
                taxonomy_obs = TaxonomyObserver(specs=CHICAGO_TRAFFIC_TRACKER_SEGMENTS_TAXONOMIES)
                observed = attach_observers(pipeline_result, [schema_obs, taxonomy_obs])
                async for _ in observed.records:
                    pass

        assert schema_obs.finalize(pipeline_result.snapshot).findings == ()
        assert taxonomy_obs.finalize(pipeline_result.snapshot).findings == ()

    async def test_diagonal_direction_does_not_surface_as_taxonomy_drift(self) -> None:
        count = json.loads((FIXTURES / "count_response.json").read_text())
        page = json.loads((FIXTURES / "records_page.json").read_text())
        page[0]["_direction"] = "NE"

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[httpx.Response(200, json=count), httpx.Response(200, json=page)]
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(
                    _adapter(client), ChicagoTrafficTrackerSegmentSpeedMapper()
                )
                taxonomy_obs = TaxonomyObserver(specs=CHICAGO_TRAFFIC_TRACKER_SEGMENTS_TAXONOMIES)
                observed = attach_observers(pipeline_result, [taxonomy_obs])
                async for _ in observed.records:
                    pass

        report = taxonomy_obs.finalize(pipeline_result.snapshot)

        assert not any(
            finding.taxonomy_id == "chicago-traffic-tracker-segment-direction"
            for finding in report.findings
        )

    async def test_unknown_direction_surfaces_as_taxonomy_drift(self) -> None:
        count = json.loads((FIXTURES / "count_response.json").read_text())
        page = json.loads((FIXTURES / "records_page.json").read_text())
        page[0]["_direction"] = "ZZZ"

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[httpx.Response(200, json=count), httpx.Response(200, json=page)]
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(
                    _adapter(client), ChicagoTrafficTrackerSegmentSpeedMapper()
                )
                taxonomy_obs = TaxonomyObserver(specs=CHICAGO_TRAFFIC_TRACKER_SEGMENTS_TAXONOMIES)
                observed = attach_observers(pipeline_result, [taxonomy_obs])
                async for _ in observed.records:
                    pass

        report = taxonomy_obs.finalize(pipeline_result.snapshot)

        assert any(
            finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE
            and finding.taxonomy_id == "chicago-traffic-tracker-segment-direction"
            for finding in report.findings
        )

    async def test_unknown_strheading_surfaces_as_taxonomy_drift(self) -> None:
        count = json.loads((FIXTURES / "count_response.json").read_text())
        page = json.loads((FIXTURES / "records_page.json").read_text())
        page[0]["_strheading"] = "Q"

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[httpx.Response(200, json=count), httpx.Response(200, json=page)]
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(
                    _adapter(client), ChicagoTrafficTrackerSegmentSpeedMapper()
                )
                taxonomy_obs = TaxonomyObserver(specs=CHICAGO_TRAFFIC_TRACKER_SEGMENTS_TAXONOMIES)
                observed = attach_observers(pipeline_result, [taxonomy_obs])
                async for _ in observed.records:
                    pass

        report = taxonomy_obs.finalize(pipeline_result.snapshot)

        assert any(
            finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE
            and finding.taxonomy_id == "chicago-traffic-tracker-segment-strheading"
            for finding in report.findings
        )


def test_source_metadata_preserves_scope_and_caveats() -> None:
    assert SOURCE_ID == "chicago-data-portal"
    assert CHICAGO_TRAFFIC_TRACKER_SEGMENTS_DATASET_ID == "n4j6-wkkf"
    assert "segment" in CHICAGO_TRAFFIC_TRACKER_SEGMENTS_SOURCE_SCOPE.casefold()
    assert any("_traffic" in caveat for caveat in CHICAGO_TRAFFIC_TRACKER_SEGMENTS_RELEASE_CAVEATS)
