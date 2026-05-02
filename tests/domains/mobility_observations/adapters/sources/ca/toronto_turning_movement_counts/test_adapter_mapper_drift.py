"""Tests for the Toronto turning-movement-count source slice."""

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
from civix.domains.mobility_observations.adapters.sources.ca import (
    toronto_turning_movement_counts as tmc,
)
from civix.domains.mobility_observations.models.common import (
    MovementType,
    ObservationDirection,
    TravelMode,
)
from civix.infra.sources.ckan import CkanFetchConfig

PINNED_NOW = datetime(2026, 5, 2, 12, 0, tzinfo=UTC)
SUMMARY_RESOURCE_ID = "summary-resource"
RAW_RESOURCE_ID = "raw-resource"
PACKAGE_SHOW_URL = f"{tmc.DEFAULT_BASE_URL}package_show"
DATASTORE_SEARCH_URL = f"{tmc.DEFAULT_BASE_URL}datastore_search"
FIXTURES = Path(__file__).parent / "fixtures"


def _package_payload() -> dict[str, Any]:
    return {
        "success": True,
        "result": {
            "resources": [
                {"id": "inactive", "name": "old-resource", "datastore_active": False},
                {
                    "id": SUMMARY_RESOURCE_ID,
                    "name": tmc.TORONTO_TMC_SUMMARY_RESOURCE_NAME,
                    "datastore_active": True,
                },
                {
                    "id": RAW_RESOURCE_ID,
                    "name": tmc.TORONTO_TMC_RAW_RESOURCE_NAME,
                    "datastore_active": True,
                },
            ]
        },
    }


def _datastore_payload(records: list[dict[str, Any]]) -> dict[str, Any]:
    return {"success": True, "result": {"total": len(records), "records": records}}


def _capture_sequence(
    requests: list[httpx.Request],
    responses: list[httpx.Response],
) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)

        return responses.pop(0)

    return handler


def _fetch(client: httpx.AsyncClient) -> CkanFetchConfig:
    return CkanFetchConfig(client=client, clock=lambda: PINNED_NOW, page_size=1000)


def _summary_adapter(client: httpx.AsyncClient) -> tmc.TorontoTmcSummaryAdapter:
    return tmc.TorontoTmcSummaryAdapter(fetch_config=_fetch(client))


def _raw_adapter(client: httpx.AsyncClient) -> tmc.TorontoTmcRawCountsAdapter:
    return tmc.TorontoTmcRawCountsAdapter(fetch_config=_fetch(client))


def _snapshot() -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId("snap-toronto-tmc"),
        source_id=tmc.SOURCE_ID,
        dataset_id=tmc.TORONTO_TMC_DATASET_ID,
        jurisdiction=tmc.TORONTO_TMC_JURISDICTION,
        fetched_at=PINNED_NOW,
        record_count=1,
    )


def _raw_fixture(name: str, **overrides: Any) -> dict[str, Any]:
    raw = json.loads((FIXTURES / name).read_text())[0]
    raw.update(overrides)

    return raw


def _record(dataset_id: DatasetId, raw: dict[str, Any]) -> RawRecord:
    return RawRecord(
        snapshot_id=SnapshotId(f"snap-{dataset_id}"),
        raw_data=raw,
        source_record_id="source-row-1",
    )


class TestAdapters:
    async def test_summary_adapter_fetches_named_resource(self) -> None:
        rows = json.loads((FIXTURES / "summary_records_page.json").read_text())
        requests: list[httpx.Request] = []

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(DATASTORE_SEARCH_URL).mock(
                side_effect=_capture_sequence(
                    requests,
                    [httpx.Response(200, json=_datastore_payload(rows))],
                )
            )

            async with httpx.AsyncClient() as client:
                adapter = _summary_adapter(client)
                result = await adapter.fetch()
                records = [record async for record in result.records]

        assert isinstance(adapter, SourceAdapter)
        assert result.snapshot.dataset_id == tmc.TORONTO_TMC_DATASET_ID
        assert result.snapshot.fetch_params == {"resource_id": SUMMARY_RESOURCE_ID}
        assert requests[0].url.params["resource_id"] == SUMMARY_RESOURCE_ID
        assert [record.source_record_id for record in records] == ["115877"]

    async def test_raw_adapter_fetches_named_resource(self) -> None:
        rows = json.loads((FIXTURES / "raw_records_page.json").read_text())
        requests: list[httpx.Request] = []

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(DATASTORE_SEARCH_URL).mock(
                side_effect=_capture_sequence(
                    requests,
                    [httpx.Response(200, json=_datastore_payload(rows))],
                )
            )

            async with httpx.AsyncClient() as client:
                adapter = _raw_adapter(client)
                result = await adapter.fetch()
                records = [record async for record in result.records]

        assert isinstance(adapter, SourceAdapter)
        assert result.snapshot.fetch_params == {"resource_id": RAW_RESOURCE_ID}
        assert requests[0].url.params["resource_id"] == RAW_RESOURCE_ID
        assert [record.source_record_id for record in records] == ["39337:2020-01-08T07:30:00"]


class TestMappers:
    def test_maps_summary_site(self) -> None:
        result = tmc.TorontoTmcSiteMapper()(
            _record(
                tmc.TORONTO_TMC_DATASET_ID,
                _raw_fixture("summary_records_page.json"),
            ),
            _snapshot(),
        )
        site = result.record

        assert site.site_id == "115877"
        assert site.kind.value is not None
        assert site.footprint.value is not None
        assert site.footprint.value.point is not None
        assert site.road_names.value == ("Victoria Park Ave", "Bracken Ave")
        assert site.source_caveats.value is not None
        assert any(caveat.code == "count-duration-14" for caveat in site.source_caveats.value)

    def test_maps_raw_row_into_many_observations_including_zero_counts(self) -> None:
        result = tmc.TorontoTmcRawCountMapper()(
            _record(
                tmc.TORONTO_TMC_DATASET_ID,
                _raw_fixture("raw_records_page.json"),
            ),
            _snapshot(),
        )
        observations = result.record
        zero_observation = next(
            obs for obs in observations if obs.observation_id.endswith("n_appr_cars_r")
        )
        through_observation = next(
            obs for obs in observations if obs.observation_id.endswith("n_appr_cars_t")
        )
        pedestrian_observation = next(
            obs for obs in observations if obs.observation_id.endswith("e_appr_peds")
        )
        bicycle_observation = next(
            obs for obs in observations if obs.observation_id.endswith("n_appr_bike")
        )

        assert len(observations) == len(tmc.TMC_COUNT_COLUMNS)
        assert zero_observation.value.value == Decimal("0")
        assert through_observation.travel_mode.value is TravelMode.PASSENGER_CAR
        assert through_observation.direction.value is ObservationDirection.SOUTHBOUND
        assert through_observation.movement_type.value is MovementType.THROUGH
        assert pedestrian_observation.travel_mode.value is TravelMode.PEDESTRIAN
        assert pedestrian_observation.movement_type.value is MovementType.CROSSING
        assert bicycle_observation.source_caveats.value is not None
        assert any(
            caveat.code == "post-2023-bicycle-definition"
            for caveat in bicycle_observation.source_caveats.value
        )

    def test_none_count_is_skipped_not_emitted(self) -> None:
        result = tmc.TorontoTmcRawCountMapper()(
            _record(
                tmc.TORONTO_TMC_DATASET_ID,
                _raw_fixture("raw_records_page.json", n_appr_cars_t=None),
            ),
            _snapshot(),
        )

        assert len(result.record) == len(tmc.TMC_COUNT_COLUMNS) - 1
        assert "n_appr_cars_t" in result.report.unmapped_source_fields

    def test_negative_count_fails_loudly(self) -> None:
        with pytest.raises(MappingError, match="negative numeric"):
            tmc.TorontoTmcRawCountMapper()(
                _record(
                    tmc.TORONTO_TMC_DATASET_ID,
                    _raw_fixture("raw_records_page.json", n_appr_cars_t=-1),
                ),
                _snapshot(),
            )

    def test_bad_timestamp_fails_loudly(self) -> None:
        with pytest.raises(MappingError, match="invalid datetime"):
            tmc.TorontoTmcRawCountMapper()(
                _record(
                    tmc.TORONTO_TMC_DATASET_ID,
                    _raw_fixture("raw_records_page.json", start_time="2020-99-99T07:30:00"),
                ),
                _snapshot(),
            )

    def test_timezone_aware_timestamp_fails_loudly(self) -> None:
        with pytest.raises(MappingError, match="timezone-aware datetime"):
            tmc.TorontoTmcRawCountMapper()(
                _record(
                    tmc.TORONTO_TMC_DATASET_ID,
                    _raw_fixture("raw_records_page.json", start_time="2020-01-08T07:30:00Z"),
                ),
                _snapshot(),
            )

    def test_date_only_timestamp_fails_loudly(self) -> None:
        with pytest.raises(MappingError, match="date-only datetime"):
            tmc.TorontoTmcRawCountMapper()(
                _record(
                    tmc.TORONTO_TMC_DATASET_ID,
                    _raw_fixture("raw_records_page.json", start_time="2020-01-08"),
                ),
                _snapshot(),
            )

    def test_invalid_coordinates_are_not_provided(self) -> None:
        result = tmc.TorontoTmcSiteMapper()(
            _record(
                tmc.TORONTO_TMC_DATASET_ID,
                _raw_fixture("summary_records_page.json", latitude=999),
            ),
            _snapshot(),
        )

        assert result.record.footprint.value is None
        assert result.record.footprint.quality is FieldQuality.NOT_PROVIDED


class TestDrift:
    async def test_summary_fixture_drift_clean(self) -> None:
        rows = json.loads((FIXTURES / "summary_records_page.json").read_text())

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(DATASTORE_SEARCH_URL).mock(
                return_value=httpx.Response(200, json=_datastore_payload(rows))
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(_summary_adapter(client), tmc.TorontoTmcSiteMapper())
                schema_obs = SchemaObserver(spec=tmc.TORONTO_TMC_SUMMARY_SCHEMA)
                taxonomy_obs = TaxonomyObserver(specs=tmc.TORONTO_TMC_SUMMARY_TAXONOMIES)
                observed = attach_observers(pipeline_result, [schema_obs, taxonomy_obs])
                records = [record async for record in observed.records]

        assert len(records) == 1
        assert schema_obs.finalize(pipeline_result.snapshot).findings == ()
        assert taxonomy_obs.finalize(pipeline_result.snapshot).findings == ()

    async def test_raw_fixture_drift_clean(self) -> None:
        rows = json.loads((FIXTURES / "raw_records_page.json").read_text())

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(DATASTORE_SEARCH_URL).mock(
                return_value=httpx.Response(200, json=_datastore_payload(rows))
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(_raw_adapter(client), tmc.TorontoTmcRawCountMapper())
                schema_obs = SchemaObserver(spec=tmc.TORONTO_TMC_RAW_SCHEMA)
                taxonomy_obs = TaxonomyObserver(specs=tmc.TORONTO_TMC_RAW_TAXONOMIES)
                observed = attach_observers(pipeline_result, [schema_obs, taxonomy_obs])
                records = [record async for record in observed.records]

        assert len(records) == 1
        assert schema_obs.finalize(pipeline_result.snapshot).findings == ()
        assert taxonomy_obs.finalize(pipeline_result.snapshot).findings == ()

    async def test_unknown_centreline_type_surfaces_as_taxonomy_drift(self) -> None:
        rows = json.loads((FIXTURES / "summary_records_page.json").read_text())
        rows[0]["centreline_type"] = "unexpected"

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(DATASTORE_SEARCH_URL).mock(
                return_value=httpx.Response(200, json=_datastore_payload(rows))
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(_summary_adapter(client), tmc.TorontoTmcSiteMapper())
                taxonomy_obs = TaxonomyObserver(specs=tmc.TORONTO_TMC_SUMMARY_TAXONOMIES)
                observed = attach_observers(pipeline_result, [taxonomy_obs])
                async for _ in observed.records:
                    pass

        report = taxonomy_obs.finalize(pipeline_result.snapshot)

        assert any(
            finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE for finding in report.findings
        )


def test_source_metadata_preserves_scope_and_caveats() -> None:
    assert tmc.SOURCE_ID == "toronto-open-data"
    assert tmc.TORONTO_TMC_DATASET_ID == "traffic-volumes-at-intersections-for-all-modes"
    assert tmc.TORONTO_TMC_SUMMARY_RESOURCE_NAME == "tmc_summary_data"
    assert tmc.TORONTO_TMC_RAW_RESOURCE_NAME == "tmc_raw_data_2020_2029"
    assert "year-bounded" in tmc.TORONTO_TMC_SOURCE_SCOPE.casefold()
    assert "deferred" in tmc.TORONTO_TMC_SOURCE_SCOPE.casefold()
    assert any(
        "license not specified" in caveat.casefold() for caveat in tmc.TORONTO_TMC_RELEASE_CAVEATS
    )
