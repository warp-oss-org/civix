"""Tests for the NYC vehicle source slice."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from civix.core.drift import SchemaObserver, TaxonomyDriftKind, TaxonomyObserver
from civix.core.identity.models.identifiers import SnapshotId
from civix.core.mapping.errors import MappingError
from civix.core.pipeline import attach_observers, run
from civix.core.ports.errors import FetchError
from civix.core.ports.models.adapter import SourceAdapter
from civix.core.quality.models.fields import FieldQuality
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.domains.transportation_safety.adapters.sources.us.nyc_vehicles import (
    DEFAULT_BASE_URL,
    NYC_JURISDICTION,
    NYC_VEHICLES_DATASET_ID,
    NYC_VEHICLES_RELEASE_CAVEATS,
    NYC_VEHICLES_SCHEMA,
    NYC_VEHICLES_SOURCE_SCOPE,
    NYC_VEHICLES_TAXONOMIES,
    SOCRATA_ORDER,
    SOURCE_ID,
    NycVehiclesAdapter,
    NycVehiclesMapper,
)
from civix.domains.transportation_safety.models.parties import RoadUserRole
from civix.domains.transportation_safety.models.vehicle import CollisionVehicle, VehicleCategory
from civix.infra.exporters.json import write_snapshot
from civix.infra.sources.socrata import SocrataFetchConfig

PINNED_NOW = datetime(2026, 5, 2, 12, 0, tzinfo=UTC)
RESOURCE_URL = f"{DEFAULT_BASE_URL}{NYC_VEHICLES_DATASET_ID}.json"
FIXTURES = Path(__file__).parent / "fixtures"


def _adapter(
    client: httpx.AsyncClient,
    *,
    page_size: int = 1000,
    app_token: str | None = None,
) -> NycVehiclesAdapter:
    return NycVehiclesAdapter(
        fetch_config=SocrataFetchConfig(
            client=client,
            clock=lambda: PINNED_NOW,
            page_size=page_size,
            app_token=app_token,
            order=SOCRATA_ORDER,
        ),
    )


def _snapshot() -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId("snap-nyc-vehicles"),
        source_id=SOURCE_ID,
        dataset_id=NYC_VEHICLES_DATASET_ID,
        jurisdiction=NYC_JURISDICTION,
        fetched_at=PINNED_NOW,
        record_count=1,
    )


def _raw(**overrides: Any) -> dict[str, Any]:
    base = json.loads((FIXTURES / "records_page.json").read_text())[0]
    base.update(overrides)

    return base


def _map(**overrides: Any) -> CollisionVehicle:
    snap = _snapshot()
    record = RawRecord(
        snapshot_id=snap.snapshot_id,
        raw_data=_raw(**overrides),
        source_record_id="vehicle-row-001",
    )

    return NycVehiclesMapper()(record, snap).record


def _capture_sequence(
    requests: list[httpx.Request],
    responses: list[httpx.Response],
) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)

        return responses.pop(0)

    return handler


class TestAdapter:
    async def test_fetches_ordered_pages_and_preserves_vehicle_row_ids(self) -> None:
        rows = json.loads((FIXTURES / "records_page.json").read_text())
        rows[0][":@computed_region_test"] = "transport"
        requests: list[httpx.Request] = []

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=_capture_sequence(
                    requests,
                    [
                        httpx.Response(200, json=[{"count": "2"}]),
                        httpx.Response(200, json=rows),
                    ],
                )
            )

            async with httpx.AsyncClient() as client:
                adapter = _adapter(client, app_token="token")
                result = await adapter.fetch()
                records = [r async for r in result.records]

        assert isinstance(adapter, SourceAdapter)
        assert result.snapshot.fetch_params == {"$order": SOCRATA_ORDER}
        assert [r.source_record_id for r in records] == ["vehicle-row-001", "vehicle-row-002"]
        assert requests[1].headers["X-App-Token"] == "token"
        assert all(not key.startswith(":@computed_region_") for key in records[0].raw_data)

    async def test_non_object_record_raises_when_streamed(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[
                    httpx.Response(200, json=[{"count": "1"}]),
                    httpx.Response(200, json=[["not", "object"]]),
                ]
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()

                with pytest.raises(FetchError, match="non-object record"):
                    _ = [r async for r in result.records]


class TestMapper:
    def test_maps_driver_vehicle_row(self) -> None:
        vehicle = _map()

        assert vehicle.collision_id == "4890001"
        assert vehicle.vehicle_id == "veh-001"
        assert vehicle.category.value is VehicleCategory.PASSENGER_CAR
        assert vehicle.road_user_role.value is RoadUserRole.DRIVER
        assert vehicle.occupant_count.value == 1
        assert vehicle.travel_direction.value is not None
        assert vehicle.maneuver.value is not None
        assert vehicle.damage.value is not None
        assert vehicle.contributing_factors.value is not None

    def test_maps_bicycle_vehicle_row(self) -> None:
        vehicle = _map(vehicle_type="Bike")

        assert vehicle.category.value is VehicleCategory.BICYCLE
        assert vehicle.road_user_role.value is RoadUserRole.CYCLIST

    def test_maps_micromobility_role_as_other(self) -> None:
        vehicle = _map(vehicle_type="E-Scooter")

        assert vehicle.category.value is VehicleCategory.MICROMOBILITY
        assert vehicle.road_user_role.value is RoadUserRole.OTHER

    def test_missing_vehicle_id_raises_mapping_error(self) -> None:
        snap = _snapshot()
        record = RawRecord(
            snapshot_id=snap.snapshot_id,
            raw_data=_raw(vehicle_id=None),
            source_record_id=None,
        )

        with pytest.raises(MappingError) as excinfo:
            NycVehiclesMapper()(record, snap)

        assert excinfo.value.source_record_id is None
        assert excinfo.value.source_fields == ("vehicle_id",)

    def test_make_model_year_and_license_fields_remain_unmapped(self) -> None:
        snap = _snapshot()
        record = RawRecord(
            snapshot_id=snap.snapshot_id,
            raw_data=_raw(),
            source_record_id="vehicle-row-001",
        )
        result = NycVehiclesMapper()(record, snap)

        assert "vehicle_make" in result.report.unmapped_source_fields
        assert "vehicle_model" in result.report.unmapped_source_fields
        assert "vehicle_year" in result.report.unmapped_source_fields
        assert "driver_license_status" in result.report.unmapped_source_fields

    def test_implausible_occupant_count_is_not_provided(self) -> None:
        vehicle = _map(vehicle_occupants="999999999")

        assert vehicle.occupant_count.value is None
        assert vehicle.occupant_count.quality is FieldQuality.NOT_PROVIDED


class TestDriftAndExport:
    async def test_fixture_drift_clean_and_export_writes_records(
        self,
        tmp_path: Path,
    ) -> None:
        count = json.loads((FIXTURES / "count_response.json").read_text())
        page = json.loads((FIXTURES / "records_page.json").read_text())

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[httpx.Response(200, json=count), httpx.Response(200, json=page)]
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(_adapter(client), NycVehiclesMapper())
                schema_obs = SchemaObserver(spec=NYC_VEHICLES_SCHEMA)
                taxonomy_obs = TaxonomyObserver(specs=NYC_VEHICLES_TAXONOMIES)
                observed = attach_observers(pipeline_result, [schema_obs, taxonomy_obs])
                manifest = await write_snapshot(
                    observed,
                    output_dir=tmp_path,
                    record_type=CollisionVehicle,
                )

        assert manifest.record_count == 2
        assert schema_obs.finalize(pipeline_result.snapshot).findings == ()
        assert taxonomy_obs.finalize(pipeline_result.snapshot).findings == ()

    async def test_unknown_taxonomy_value_surfaces_as_error(self) -> None:
        count = json.loads((FIXTURES / "count_response.json").read_text())
        page = json.loads((FIXTURES / "records_page.json").read_text())
        page[0]["vehicle_type"] = "Hovercraft"

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[httpx.Response(200, json=count), httpx.Response(200, json=page)]
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(_adapter(client), NycVehiclesMapper())
                taxonomy_obs = TaxonomyObserver(specs=NYC_VEHICLES_TAXONOMIES)
                observed = attach_observers(pipeline_result, [taxonomy_obs])

                async for _ in observed.records:
                    pass

        report = taxonomy_obs.finalize(pipeline_result.snapshot)

        assert any(f.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE for f in report.findings)
        assert report.has_errors


def test_source_metadata_preserves_scope_and_caveats() -> None:
    assert SOURCE_ID == "nyc-open-data"
    assert NYC_VEHICLES_DATASET_ID == "bm4k-52h4"
    assert "Vehicle-level records" in NYC_VEHICLES_SOURCE_SCOPE
    assert any("make, model, year" in caveat for caveat in NYC_VEHICLES_RELEASE_CAVEATS)
