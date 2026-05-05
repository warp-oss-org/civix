"""Tests for the Toronto permanent bicycle counter source slice."""

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
from civix.core.temporal import TemporalPeriodPrecision
from civix.domains.mobility_observations.adapters.sources.ca.toronto_bicycle_counters import (
    DEFAULT_BASE_URL,
    SOURCE_ID,
    TORONTO_BICYCLE_COUNTER_15MIN_RESOURCE_NAME,
    TORONTO_BICYCLE_COUNTER_15MIN_SCHEMA,
    TORONTO_BICYCLE_COUNTER_15MIN_TAXONOMIES,
    TORONTO_BICYCLE_COUNTER_LOCATIONS_RESOURCE_NAME,
    TORONTO_BICYCLE_COUNTER_LOCATIONS_SCHEMA,
    TORONTO_BICYCLE_COUNTER_LOCATIONS_TAXONOMIES,
    TORONTO_BICYCLE_COUNTERS_DATASET_ID,
    TORONTO_BICYCLE_COUNTERS_JURISDICTION,
    TORONTO_BICYCLE_COUNTERS_RELEASE_CAVEATS,
    TORONTO_BICYCLE_COUNTERS_SOURCE_SCOPE,
    TorontoBicycleCounter15MinAdapter,
    TorontoBicycleCounter15MinMapper,
    TorontoBicycleCounterLocationsAdapter,
    TorontoBicycleCounterSiteMapper,
)
from civix.domains.mobility_observations.models.common import ObservationDirection, TravelMode
from civix.infra.sources.ckan import CkanFetchConfig

PINNED_NOW = datetime(2026, 5, 2, 12, 0, tzinfo=UTC)
LOCATIONS_RESOURCE_ID = "locations-resource"
COUNTS_RESOURCE_ID = "counts-resource"
PACKAGE_SHOW_URL = f"{DEFAULT_BASE_URL}package_show"
DATASTORE_SEARCH_URL = f"{DEFAULT_BASE_URL}datastore_search"
FIXTURES = Path(__file__).parent / "fixtures"


def _package_payload() -> dict[str, Any]:
    return {
        "success": True,
        "result": {
            "resources": [
                {
                    "id": LOCATIONS_RESOURCE_ID,
                    "name": TORONTO_BICYCLE_COUNTER_LOCATIONS_RESOURCE_NAME,
                    "datastore_active": True,
                },
                {
                    "id": COUNTS_RESOURCE_ID,
                    "name": TORONTO_BICYCLE_COUNTER_15MIN_RESOURCE_NAME,
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


def _location_adapter(client: httpx.AsyncClient) -> TorontoBicycleCounterLocationsAdapter:
    return TorontoBicycleCounterLocationsAdapter(fetch_config=_fetch(client))


def _count_adapter(client: httpx.AsyncClient) -> TorontoBicycleCounter15MinAdapter:
    return TorontoBicycleCounter15MinAdapter(fetch_config=_fetch(client))


def _snapshot() -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId("snap-toronto-bike"),
        source_id=SOURCE_ID,
        dataset_id=TORONTO_BICYCLE_COUNTERS_DATASET_ID,
        jurisdiction=TORONTO_BICYCLE_COUNTERS_JURISDICTION,
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
    async def test_location_adapter_fetches_named_resource(self) -> None:
        rows = json.loads((FIXTURES / "location_records_page.json").read_text())
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
                adapter = _location_adapter(client)
                result = await adapter.fetch()
                records = [record async for record in result.records]

        assert isinstance(adapter, SourceAdapter)
        assert result.snapshot.fetch_params == {"resource_id": LOCATIONS_RESOURCE_ID}
        assert requests[0].url.params["resource_id"] == LOCATIONS_RESOURCE_ID
        assert [record.source_record_id for record in records] == ["1"]

    async def test_count_adapter_fetches_named_resource(self) -> None:
        rows = json.loads((FIXTURES / "count_records_page.json").read_text())
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
                adapter = _count_adapter(client)
                result = await adapter.fetch()
                records = [record async for record in result.records]

        assert isinstance(adapter, SourceAdapter)
        assert result.snapshot.fetch_params == {"resource_id": COUNTS_RESOURCE_ID}
        assert requests[0].url.params["resource_id"] == COUNTS_RESOURCE_ID
        assert [record.source_record_id for record in records] == [
            "3:2025-01-01T00:00:00",
            "3:2025-01-01T00:15:00",
        ]


class TestMappers:
    def test_maps_counter_site(self) -> None:
        result = TorontoBicycleCounterSiteMapper()(
            _record(
                TORONTO_BICYCLE_COUNTERS_DATASET_ID,
                _raw_fixture("location_records_page.json"),
            ),
            _snapshot(),
        )
        site = result.record

        assert site.site_id == "1"
        assert site.footprint.value is not None
        assert site.footprint.value.point is not None
        assert site.direction.value is ObservationDirection.EASTBOUND
        assert site.active_period.value is not None
        assert site.road_names.value == ("Bloor St E", "Castle Frank Rd")
        assert site.source_caveats.value is not None
        assert any(caveat.label == "Induction - Other" for caveat in site.source_caveats.value)

    def test_maps_zero_count_observation(self) -> None:
        result = TorontoBicycleCounter15MinMapper()(
            _record(
                TORONTO_BICYCLE_COUNTERS_DATASET_ID,
                _raw_fixture("count_records_page.json"),
            ),
            _snapshot(),
        )
        observation = result.record

        assert observation.observation_id == "3:2025-01-01T00:00:00"
        assert observation.site_id == "3"
        assert observation.period.value is not None
        assert observation.period.value.precision is TemporalPeriodPrecision.INTERVAL
        assert observation.period.value.start_datetime is not None
        assert observation.period.value.end_datetime is not None
        assert (
            observation.period.value.end_datetime - observation.period.value.start_datetime
        ).total_seconds() == 900

        assert observation.travel_mode.value is TravelMode.MICROMOBILITY
        assert observation.value.value == Decimal("0")
        assert observation.source_caveats.value is not None
        assert any(
            caveat.code == "micromobility-included" for caveat in observation.source_caveats.value
        )

    def test_negative_count_fails_loudly(self) -> None:
        with pytest.raises(MappingError, match="negative numeric"):
            TorontoBicycleCounter15MinMapper()(
                _record(
                    TORONTO_BICYCLE_COUNTERS_DATASET_ID,
                    _raw_fixture("count_records_page.json", bin_volume=-1),
                ),
                _snapshot(),
            )

    def test_timezone_aware_count_timestamp_fails_loudly(self) -> None:
        with pytest.raises(MappingError, match="timezone-aware datetime"):
            TorontoBicycleCounter15MinMapper()(
                _record(
                    TORONTO_BICYCLE_COUNTERS_DATASET_ID,
                    _raw_fixture("count_records_page.json", datetime_bin="2025-01-01T00:00:00Z"),
                ),
                _snapshot(),
            )

    def test_date_only_count_timestamp_fails_loudly(self) -> None:
        with pytest.raises(MappingError, match="date-only datetime"):
            TorontoBicycleCounter15MinMapper()(
                _record(
                    TORONTO_BICYCLE_COUNTERS_DATASET_ID,
                    _raw_fixture("count_records_page.json", datetime_bin="2025-01-01"),
                ),
                _snapshot(),
            )

    def test_missing_join_key_fails_loudly(self) -> None:
        with pytest.raises(MappingError, match="location_dir_id"):
            TorontoBicycleCounter15MinMapper()(
                _record(
                    TORONTO_BICYCLE_COUNTERS_DATASET_ID,
                    _raw_fixture("count_records_page.json", location_dir_id=None),
                ),
                _snapshot(),
            )

    def test_invalid_coordinates_are_not_provided(self) -> None:
        result = TorontoBicycleCounterSiteMapper()(
            _record(
                TORONTO_BICYCLE_COUNTERS_DATASET_ID,
                _raw_fixture("location_records_page.json", longitude=-999),
            ),
            _snapshot(),
        )

        assert result.record.footprint.value is None
        assert result.record.footprint.quality is FieldQuality.NOT_PROVIDED


class TestDrift:
    async def test_location_fixture_drift_clean(self) -> None:
        rows = json.loads((FIXTURES / "location_records_page.json").read_text())

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(DATASTORE_SEARCH_URL).mock(
                return_value=httpx.Response(200, json=_datastore_payload(rows))
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(
                    _location_adapter(client),
                    TorontoBicycleCounterSiteMapper(),
                )
                schema_obs = SchemaObserver(spec=TORONTO_BICYCLE_COUNTER_LOCATIONS_SCHEMA)
                taxonomy_obs = TaxonomyObserver(specs=TORONTO_BICYCLE_COUNTER_LOCATIONS_TAXONOMIES)
                observed = attach_observers(pipeline_result, [schema_obs, taxonomy_obs])
                records = [record async for record in observed.records]

        assert len(records) == 1
        assert schema_obs.finalize(pipeline_result.snapshot).findings == ()
        assert taxonomy_obs.finalize(pipeline_result.snapshot).findings == ()

    async def test_count_fixture_drift_clean(self) -> None:
        rows = json.loads((FIXTURES / "count_records_page.json").read_text())

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(DATASTORE_SEARCH_URL).mock(
                return_value=httpx.Response(200, json=_datastore_payload(rows))
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(
                    _count_adapter(client),
                    TorontoBicycleCounter15MinMapper(),
                )
                schema_obs = SchemaObserver(spec=TORONTO_BICYCLE_COUNTER_15MIN_SCHEMA)
                taxonomy_obs = TaxonomyObserver(specs=TORONTO_BICYCLE_COUNTER_15MIN_TAXONOMIES)
                observed = attach_observers(pipeline_result, [schema_obs, taxonomy_obs])
                records = [record async for record in observed.records]

        assert len(records) == 2
        assert schema_obs.finalize(pipeline_result.snapshot).findings == ()
        assert taxonomy_obs.finalize(pipeline_result.snapshot).findings == ()

    async def test_unknown_direction_surfaces_as_taxonomy_drift(self) -> None:
        rows = json.loads((FIXTURES / "location_records_page.json").read_text())
        rows[0]["direction"] = "Sideways"

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(DATASTORE_SEARCH_URL).mock(
                return_value=httpx.Response(200, json=_datastore_payload(rows))
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(
                    _location_adapter(client),
                    TorontoBicycleCounterSiteMapper(),
                )
                taxonomy_obs = TaxonomyObserver(specs=TORONTO_BICYCLE_COUNTER_LOCATIONS_TAXONOMIES)
                observed = attach_observers(pipeline_result, [taxonomy_obs])
                async for _ in observed.records:
                    pass

        report = taxonomy_obs.finalize(pipeline_result.snapshot)

        assert any(
            finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE for finding in report.findings
        )


def test_source_metadata_preserves_scope_and_caveats() -> None:
    assert SOURCE_ID == "toronto-open-data"
    assert TORONTO_BICYCLE_COUNTERS_DATASET_ID == "permanent-bicycle-counters"
    assert TORONTO_BICYCLE_COUNTER_15MIN_RESOURCE_NAME == "cycling_permanent_counts_15min_2025_2026"
    assert "year-bounded" in TORONTO_BICYCLE_COUNTERS_SOURCE_SCOPE.casefold()
    assert any(
        "license not specified" in caveat.casefold()
        for caveat in TORONTO_BICYCLE_COUNTERS_RELEASE_CAVEATS
    )
