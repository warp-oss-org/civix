"""Tests for the GB DfT road-traffic-counts source slice."""

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

from civix.core.drift import (
    SchemaDriftKind,
    SchemaObserver,
    TaxonomyDriftKind,
    TaxonomyObserver,
)
from civix.core.identity.models.identifiers import DatasetId, SnapshotId
from civix.core.mapping.errors import MappingError
from civix.core.pipeline import attach_observers, run
from civix.core.ports.models.adapter import SourceAdapter
from civix.core.quality.models.fields import FieldQuality
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.temporal import TemporalPeriodPrecision
from civix.domains.mobility_observations.adapters.sources.gb import (
    road_traffic_counts as gb,
)
from civix.domains.mobility_observations.models.common import (
    AggregationWindow,
    CountMetricType,
    CountUnit,
    MeasurementMethod,
    MobilitySiteKind,
    ObservationDirection,
    TravelMode,
)

PINNED_NOW = datetime(2026, 5, 2, 12, 0, tzinfo=UTC)
COUNT_POINTS_URL = f"{gb.DEFAULT_BASE_URL}{gb.GB_DFT_COUNT_POINTS_ENDPOINT}"
AADF_URL = f"{gb.DEFAULT_BASE_URL}{gb.GB_DFT_AADF_BY_DIRECTION_ENDPOINT}"
FIXTURES = Path(__file__).parent / "fixtures"


def _envelope(records: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "current_page": 1,
        "data": records,
        "first_page_url": "https://example.test/api?page=1",
        "from": 1 if records else None,
        "last_page": 1,
        "last_page_url": "https://example.test/api?page=1",
        "links": [],
        "next_page_url": None,
        "path": "https://example.test/api",
        "per_page": len(records) or 250,
        "prev_page_url": None,
        "to": len(records) or None,
        "total": len(records),
    }


def _capture(
    requests: list[httpx.Request],
    responses: list[httpx.Response],
) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)

        return responses.pop(0)

    return handler


def _fetch(client: httpx.AsyncClient) -> gb.DftFetchConfig:
    return gb.DftFetchConfig(client=client, clock=lambda: PINNED_NOW, page_size=250)


def _count_points_adapter(client: httpx.AsyncClient) -> gb.GbDftCountPointsAdapter:
    return gb.GbDftCountPointsAdapter(fetch_config=_fetch(client))


def _aadf_adapter(client: httpx.AsyncClient) -> gb.GbDftAadfByDirectionAdapter:
    return gb.GbDftAadfByDirectionAdapter(fetch_config=_fetch(client))


def _snapshot(dataset_id: DatasetId) -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId(f"snap-{dataset_id}"),
        source_id=gb.SOURCE_ID,
        dataset_id=dataset_id,
        jurisdiction=gb.GB_DFT_JURISDICTION,
        fetched_at=PINNED_NOW,
        record_count=1,
    )


def _raw_fixture(name: str, index: int = 0, **overrides: Any) -> dict[str, Any]:
    raw: dict[str, Any] = json.loads((FIXTURES / name).read_text())[index]
    raw.update(overrides)

    return raw


def _record(dataset_id: DatasetId, raw: dict[str, Any]) -> RawRecord:
    return RawRecord(
        snapshot_id=SnapshotId(f"snap-{dataset_id}"),
        raw_data=raw,
        source_record_id=str(raw.get("id", "row-1")),
    )


class TestAdapters:
    async def test_count_points_adapter_paginates_and_preserves_source(self) -> None:
        rows = json.loads((FIXTURES / "count_points_page.json").read_text())
        requests: list[httpx.Request] = []

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(COUNT_POINTS_URL).mock(
                side_effect=_capture(
                    requests,
                    [httpx.Response(200, json=_envelope(rows))],
                )
            )

            async with httpx.AsyncClient() as client:
                adapter = _count_points_adapter(client)
                result = await adapter.fetch()
                records = [record async for record in result.records]

        assert isinstance(adapter, SourceAdapter)
        assert adapter.source_id == gb.SOURCE_ID
        assert adapter.dataset_id == gb.GB_DFT_COUNT_POINTS_DATASET_ID
        assert adapter.jurisdiction == gb.GB_DFT_JURISDICTION
        assert result.snapshot.dataset_id == gb.GB_DFT_COUNT_POINTS_DATASET_ID
        assert result.snapshot.fetch_params == {"endpoint": gb.GB_DFT_COUNT_POINTS_ENDPOINT}
        assert [r.source_record_id for r in records] == ["51", "942"]
        assert requests[0].url.params["page[number]"] == "1"
        assert requests[0].url.params["page[size]"] == "250"

    async def test_aadf_adapter_paginates_and_preserves_source(self) -> None:
        rows = json.loads((FIXTURES / "aadf_page.json").read_text())
        requests: list[httpx.Request] = []

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(AADF_URL).mock(
                side_effect=_capture(
                    requests,
                    [httpx.Response(200, json=_envelope(rows))],
                )
            )

            async with httpx.AsyncClient() as client:
                adapter = _aadf_adapter(client)
                result = await adapter.fetch()
                records = [record async for record in result.records]

        assert isinstance(adapter, SourceAdapter)
        assert adapter.source_id == gb.SOURCE_ID
        assert adapter.dataset_id == gb.GB_DFT_AADF_BY_DIRECTION_DATASET_ID
        assert result.snapshot.fetch_params == {
            "endpoint": gb.GB_DFT_AADF_BY_DIRECTION_ENDPOINT,
        }
        assert [r.source_record_id for r in records] == ["1001", "1002", "1003"]

    async def test_pagination_stops_when_next_page_url_is_null(self) -> None:
        rows = json.loads((FIXTURES / "count_points_page.json").read_text())
        responses = [httpx.Response(200, json=_envelope(rows))]

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(COUNT_POINTS_URL).mock(side_effect=responses)

            async with httpx.AsyncClient() as client:
                result = await _count_points_adapter(client).fetch()
                records = [r async for r in result.records]

        assert len(records) == 2


class TestSiteMapper:
    def test_count_point_row_maps_to_traffic_count_point_site(self) -> None:
        result = gb.GbDftCountPointSiteMapper()(
            _record(
                gb.GB_DFT_COUNT_POINTS_DATASET_ID,
                _raw_fixture("count_points_page.json"),
            ),
            _snapshot(gb.GB_DFT_COUNT_POINTS_DATASET_ID),
        )
        site = result.record

        assert site.site_id == "51"
        assert site.kind.value is MobilitySiteKind.TRAFFIC_COUNT_POINT
        assert site.kind.quality is FieldQuality.INFERRED
        assert site.footprint.value is not None
        assert site.footprint.value.point is not None
        assert site.footprint.value.point.latitude == 49.91550316
        assert site.road_names.value == ("A3111", "Pierhead, Hugh Town", "A3112")

    def test_site_direction_and_method_are_not_provided(self) -> None:
        result = gb.GbDftCountPointSiteMapper()(
            _record(
                gb.GB_DFT_COUNT_POINTS_DATASET_ID,
                _raw_fixture("count_points_page.json"),
            ),
            _snapshot(gb.GB_DFT_COUNT_POINTS_DATASET_ID),
        )
        site = result.record

        assert site.direction.value is None
        assert site.direction.quality is FieldQuality.NOT_PROVIDED
        assert site.measurement_method.value is None
        assert site.measurement_method.quality is FieldQuality.NOT_PROVIDED

    def test_invalid_coordinates_are_not_provided(self) -> None:
        result = gb.GbDftCountPointSiteMapper()(
            _record(
                gb.GB_DFT_COUNT_POINTS_DATASET_ID,
                _raw_fixture("count_points_page.json", latitude="999"),
            ),
            _snapshot(gb.GB_DFT_COUNT_POINTS_DATASET_ID),
        )

        assert result.record.footprint.value is None
        assert result.record.footprint.quality is FieldQuality.NOT_PROVIDED


class TestCountMapper:
    def _map(self, **overrides: Any) -> Any:
        raw = _raw_fixture("aadf_page.json", **overrides)

        return gb.GbDftAadfCountMapper()(
            _record(gb.GB_DFT_AADF_BY_DIRECTION_DATASET_ID, raw),
            _snapshot(gb.GB_DFT_AADF_BY_DIRECTION_DATASET_ID),
        )

    def test_one_row_emits_one_observation_per_supported_class_column(self) -> None:
        result = self._map()
        observations = result.record

        assert len(observations) == len(gb.AADF_VEHICLE_CLASS_COLUMNS)
        emitted_fields = {obs.value.source_fields[0] for obs in observations}
        expected_fields = {column.source_field for column in gb.AADF_VEHICLE_CLASS_COLUMNS}
        assert emitted_fields == expected_fields

    def test_all_motor_vehicles_is_never_emitted(self) -> None:
        result = self._map()

        for observation in result.record:
            assert gb.ALL_MOTOR_VEHICLES_FIELD not in observation.value.source_fields

        assert gb.ALL_MOTOR_VEHICLES_FIELD in result.report.unmapped_source_fields

    def test_hgv_subclasses_are_not_emitted(self) -> None:
        result = self._map()
        emitted = {observation.value.source_fields[0] for observation in result.record}

        assert "hgvs_2_rigid_axle" not in emitted
        assert "hgvs_5_articulated_axle" not in emitted
        assert "all_hgvs" in emitted

    def test_zero_value_is_a_real_observation(self) -> None:
        # row index 2 has pedal_cycles=0
        raw = _raw_fixture("aadf_page.json", index=2)
        result = gb.GbDftAadfCountMapper()(
            _record(gb.GB_DFT_AADF_BY_DIRECTION_DATASET_ID, raw),
            _snapshot(gb.GB_DFT_AADF_BY_DIRECTION_DATASET_ID),
        )
        pedal = next(obs for obs in result.record if "pedal_cycles" in obs.value.source_fields)

        assert pedal.value.value == Decimal("0")
        assert pedal.value.quality is FieldQuality.DIRECT

    def test_lgvs_emits_other_with_vans_caveat(self) -> None:
        result = self._map()
        lgv = next(obs for obs in result.record if "lgvs" in obs.value.source_fields)

        assert lgv.travel_mode.value is TravelMode.OTHER
        assert lgv.source_caveats.value is not None
        assert any(caveat.code == gb.LGV_CAVEAT_CODE for caveat in lgv.source_caveats.value)

    def test_two_wheeled_emits_other_with_motorcycle_caveat(self) -> None:
        result = self._map()
        twm = next(
            obs
            for obs in result.record
            if "two_wheeled_motor_vehicles" in obs.value.source_fields
        )

        assert twm.travel_mode.value is TravelMode.OTHER
        assert twm.source_caveats.value is not None
        assert any(
            caveat.code == gb.TWO_WHEELED_CAVEAT_CODE for caveat in twm.source_caveats.value
        )

    def test_all_hgvs_emits_truck_with_rollup_caveat(self) -> None:
        result = self._map()
        hgv = next(obs for obs in result.record if "all_hgvs" in obs.value.source_fields)

        assert hgv.travel_mode.value is TravelMode.TRUCK
        assert hgv.source_caveats.value is not None
        assert any(
            caveat.code == gb.HGV_SUBCLASS_CAVEAT_CODE for caveat in hgv.source_caveats.value
        )

    def test_manual_count_detail_maps_to_manual_count_standardized(self) -> None:
        result = self._map(index=0)
        observation = result.record[0]

        assert observation.measurement_method.value is MeasurementMethod.MANUAL_COUNT
        assert observation.measurement_method.quality is FieldQuality.STANDARDIZED
        assert observation.measurement_method.source_fields == ("estimation_method_detailed",)

    def test_estimated_previous_year_detail_maps_to_annualized_estimate(self) -> None:
        result = self._map(index=1)
        observation = result.record[0]

        assert observation.measurement_method.value is MeasurementMethod.ANNUALIZED_ESTIMATE
        assert observation.measurement_method.quality is FieldQuality.STANDARDIZED

    def test_estimated_nearby_links_detail_maps_to_annualized_estimate(self) -> None:
        result = self._map(index=2)
        observation = result.record[0]

        assert observation.measurement_method.value is MeasurementMethod.ANNUALIZED_ESTIMATE
        assert observation.measurement_method.quality is FieldQuality.STANDARDIZED

    def test_unknown_detail_with_counted_falls_back_to_manual_count_inferred(self) -> None:
        result = self._map(estimation_method_detailed="Some new manual variant")
        observation = result.record[0]

        assert observation.measurement_method.value is MeasurementMethod.MANUAL_COUNT
        assert observation.measurement_method.quality is FieldQuality.INFERRED
        assert observation.measurement_method.source_fields == ("estimation_method",)

    def test_unknown_detail_with_estimated_falls_back_to_estimate_inferred(self) -> None:
        result = self._map(
            index=1,
            estimation_method_detailed="Estimated using some new method",
        )
        observation = result.record[0]

        assert observation.measurement_method.value is MeasurementMethod.ANNUALIZED_ESTIMATE
        assert observation.measurement_method.quality is FieldQuality.INFERRED

    def test_observations_carry_aadf_vehicles_per_day_annual_average_daily(self) -> None:
        result = self._map()

        for observation in result.record:
            assert observation.metric_type.value is CountMetricType.AADF
            assert observation.unit.value is CountUnit.VEHICLES_PER_DAY
            assert observation.aggregation_window.value is AggregationWindow.ANNUAL_AVERAGE_DAILY

    def test_period_precision_year_with_year_value(self) -> None:
        result = self._map()
        observation = result.record[0]

        assert observation.period.value is not None
        assert observation.period.value.precision is TemporalPeriodPrecision.YEAR
        assert observation.period.value.year_value == 2024

    def test_direction_of_travel_n_maps_to_northbound(self) -> None:
        result = self._map(index=0)

        assert result.record[0].direction.value is ObservationDirection.NORTHBOUND
        assert result.record[0].direction.quality is FieldQuality.STANDARDIZED

    def test_direction_of_travel_c_maps_to_bidirectional(self) -> None:
        result = self._map(index=2)

        assert result.record[0].direction.value is ObservationDirection.BIDIRECTIONAL

    def test_unknown_direction_maps_to_source_specific(self) -> None:
        result = self._map(direction_of_travel="X")

        assert result.record[0].direction.value is ObservationDirection.SOURCE_SPECIFIC
        assert result.record[0].direction.quality is FieldQuality.DERIVED

    def test_invalid_year_fails_loudly(self) -> None:
        with pytest.raises(MappingError, match="invalid year"):
            self._map(year="not-a-year")

    def test_negative_volume_fails_loudly(self) -> None:
        with pytest.raises(MappingError, match="negative numeric"):
            self._map(cars_and_taxis=-1)

    def test_missing_count_point_id_fails_loudly(self) -> None:
        with pytest.raises(MappingError, match="missing required source field"):
            self._map(count_point_id=None)


class TestDrift:
    async def test_count_points_clean_fixture_has_no_drift(self) -> None:
        rows = json.loads((FIXTURES / "count_points_page.json").read_text())

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(COUNT_POINTS_URL).mock(
                return_value=httpx.Response(200, json=_envelope(rows))
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(
                    _count_points_adapter(client),
                    gb.GbDftCountPointSiteMapper(),
                )
                schema_obs = SchemaObserver(spec=gb.GB_DFT_COUNT_POINTS_SCHEMA)
                taxonomy_obs = TaxonomyObserver(specs=gb.GB_DFT_COUNT_POINTS_TAXONOMIES)
                observed = attach_observers(pipeline_result, [schema_obs, taxonomy_obs])
                async for _ in observed.records:
                    pass

        assert schema_obs.finalize(pipeline_result.snapshot).findings == ()
        assert taxonomy_obs.finalize(pipeline_result.snapshot).findings == ()

    async def test_aadf_clean_fixture_has_no_drift(self) -> None:
        rows = json.loads((FIXTURES / "aadf_page.json").read_text())

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(AADF_URL).mock(
                return_value=httpx.Response(200, json=_envelope(rows))
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(
                    _aadf_adapter(client),
                    gb.GbDftAadfCountMapper(),
                )
                schema_obs = SchemaObserver(spec=gb.GB_DFT_AADF_BY_DIRECTION_SCHEMA)
                taxonomy_obs = TaxonomyObserver(specs=gb.GB_DFT_AADF_BY_DIRECTION_TAXONOMIES)
                observed = attach_observers(pipeline_result, [schema_obs, taxonomy_obs])
                async for _ in observed.records:
                    pass

        assert schema_obs.finalize(pipeline_result.snapshot).findings == ()
        assert taxonomy_obs.finalize(pipeline_result.snapshot).findings == ()

    async def test_unknown_direction_of_travel_surfaces_as_taxonomy_drift(self) -> None:
        rows = json.loads((FIXTURES / "aadf_page.json").read_text())
        rows[0]["direction_of_travel"] = "X"

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(AADF_URL).mock(
                return_value=httpx.Response(200, json=_envelope(rows))
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(
                    _aadf_adapter(client),
                    gb.GbDftAadfCountMapper(),
                )
                taxonomy_obs = TaxonomyObserver(specs=gb.GB_DFT_AADF_BY_DIRECTION_TAXONOMIES)
                observed = attach_observers(pipeline_result, [taxonomy_obs])
                async for _ in observed.records:
                    pass

        report = taxonomy_obs.finalize(pipeline_result.snapshot)

        assert any(
            finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE
            and finding.source_field == "direction_of_travel"
            for finding in report.findings
        )

    async def test_unknown_estimation_method_detailed_surfaces_as_taxonomy_drift(self) -> None:
        rows = json.loads((FIXTURES / "aadf_page.json").read_text())
        rows[0]["estimation_method_detailed"] = "Estimated using a new method"

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(AADF_URL).mock(
                return_value=httpx.Response(200, json=_envelope(rows))
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(
                    _aadf_adapter(client),
                    gb.GbDftAadfCountMapper(),
                )
                taxonomy_obs = TaxonomyObserver(specs=gb.GB_DFT_AADF_BY_DIRECTION_TAXONOMIES)
                observed = attach_observers(pipeline_result, [taxonomy_obs])
                async for _ in observed.records:
                    pass

        report = taxonomy_obs.finalize(pipeline_result.snapshot)

        assert any(
            finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE
            and finding.source_field == "estimation_method_detailed"
            for finding in report.findings
        )

    async def test_new_vehicle_class_column_surfaces_as_schema_drift_and_is_skipped(
        self,
    ) -> None:
        rows = json.loads((FIXTURES / "aadf_page.json").read_text())
        for row in rows:
            row["e_scooters"] = 17

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(AADF_URL).mock(
                return_value=httpx.Response(200, json=_envelope(rows))
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(
                    _aadf_adapter(client),
                    gb.GbDftAadfCountMapper(),
                )
                schema_obs = SchemaObserver(spec=gb.GB_DFT_AADF_BY_DIRECTION_SCHEMA)
                observed = attach_observers(pipeline_result, [schema_obs])
                mapping_results = [result async for result in observed.records]

        report = schema_obs.finalize(pipeline_result.snapshot)

        assert any(
            finding.kind is SchemaDriftKind.UNEXPECTED_FIELD and finding.field_name == "e_scooters"
            for finding in report.findings
        )
        for mapping_result in mapping_results:
            assert "e_scooters" in mapping_result.mapped.report.unmapped_source_fields
            assert all(
                "e_scooters" not in observation.value.source_fields
                for observation in mapping_result.mapped.record
            )


def test_source_metadata_preserves_scope_and_caveats() -> None:
    assert gb.SOURCE_ID == "gb-dft-road-traffic"
    assert gb.GB_DFT_JURISDICTION.country == "GB"
    assert "AADF" in gb.GB_DFT_SOURCE_SCOPE
    assert "deferred" in gb.GB_DFT_SOURCE_SCOPE.casefold()
    assert any(
        "open government licence" in caveat.casefold() for caveat in gb.GB_DFT_RELEASE_CAVEATS
    )
    assert any("annual-average" in caveat.casefold() for caveat in gb.GB_DFT_RELEASE_CAVEATS)
    assert any("dft-specific" in caveat.casefold() for caveat in gb.GB_DFT_RELEASE_CAVEATS)
    assert any(
        "all_motor_vehicles" in caveat.casefold() and "sum" in caveat.casefold()
        for caveat in gb.GB_DFT_RELEASE_CAVEATS
    )
