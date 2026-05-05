"""Tests for the France BAAC / ONISR source slice."""

from __future__ import annotations

import csv
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from civix.core.drift import SchemaObserver, TaxonomyDriftKind, TaxonomyObserver
from civix.core.identity.models.identifiers import DatasetId, SnapshotId
from civix.core.pipeline import run
from civix.core.ports.errors import FetchError
from civix.core.ports.models.adapter import SourceAdapter
from civix.core.quality.models.fields import FieldQuality
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.domains.transportation_safety.adapters.sources.fr.baac import (
    BAAC_CHARACTERISTICS_DATASET_ID,
    BAAC_CHARACTERISTICS_RESOURCE_ID,
    BAAC_CHARACTERISTICS_RESOURCE_TITLE,
    BAAC_CHARACTERISTICS_SCHEMA,
    BAAC_CHARACTERISTICS_TAXONOMIES,
    BAAC_CHARACTERISTICS_URL,
    BAAC_DATASET_LAST_UPDATE,
    BAAC_JURISDICTION,
    BAAC_LICENCE,
    BAAC_RELEASE,
    BAAC_RELEASE_CAVEATS,
    BAAC_SOURCE_SCOPE,
    BAAC_SOURCE_YEAR,
    BAAC_USERS_DATASET_ID,
    BAAC_USERS_RESOURCE_ID,
    BAAC_USERS_RESOURCE_TITLE,
    BAAC_USERS_SCHEMA,
    BAAC_USERS_TAXONOMIES,
    BAAC_USERS_URL,
    BAAC_VEHICLES_DATASET_ID,
    BAAC_VEHICLES_RESOURCE_ID,
    BAAC_VEHICLES_RESOURCE_TITLE,
    BAAC_VEHICLES_SCHEMA,
    BAAC_VEHICLES_TAXONOMIES,
    BAAC_VEHICLES_URL,
    COLLISION_MAPPER_ID,
    MAPPER_VERSION,
    SOURCE_ID,
    USER_MAPPER_ID,
    VEHICLE_MAPPER_ID,
    BaacCharacteristicsAdapter,
    BaacCollisionMapper,
    BaacFetchConfig,
    BaacUserMapper,
    BaacUsersAdapter,
    BaacVehicleMapper,
    BaacVehiclesAdapter,
)
from civix.domains.transportation_safety.models.parties import RoadUserRole
from civix.domains.transportation_safety.models.person import InjuryOutcome
from civix.domains.transportation_safety.models.time import OccurrenceTimePrecision
from civix.domains.transportation_safety.models.vehicle import VehicleCategory

PINNED_NOW = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
FIXTURES = Path(__file__).parent / "fixtures"
CHARACTERISTICS_CSV = FIXTURES / "caracteristiques.csv"
VEHICLES_CSV = FIXTURES / "vehicules.csv"
USERS_CSV = FIXTURES / "usagers.csv"


def _csv_rows(path: Path) -> list[dict[str, Any]]:
    with path.open() as handle:
        return [dict(row) for row in csv.DictReader(handle, delimiter=";")]


def _fixture(name: str) -> list[dict[str, Any]]:
    return _csv_rows(FIXTURES / name)


def _snapshot(snapshot_id: str, dataset_id: DatasetId, record_count: int) -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId(snapshot_id),
        source_id=SOURCE_ID,
        dataset_id=dataset_id,
        jurisdiction=BAAC_JURISDICTION,
        fetched_at=PINNED_NOW,
        record_count=record_count,
    )


def _records(
    rows: list[dict[str, Any]],
    *,
    snapshot: SourceSnapshot,
    source_id_fields: tuple[str, ...],
) -> tuple[RawRecord, ...]:
    return tuple(
        RawRecord(
            snapshot_id=snapshot.snapshot_id,
            raw_data=row,
            source_record_id=":".join(str(row[field]) for field in source_id_fields),
        )
        for row in rows
    )


def _characteristic_records() -> tuple[RawRecord, ...]:
    rows = _fixture("caracteristiques.csv")
    snapshot = _snapshot("snap-baac-caracteristiques", BAAC_CHARACTERISTICS_DATASET_ID, len(rows))

    return _records(rows, snapshot=snapshot, source_id_fields=("Num_Acc",))


def _vehicle_records() -> tuple[RawRecord, ...]:
    rows = _fixture("vehicules.csv")
    snapshot = _snapshot("snap-baac-vehicules", BAAC_VEHICLES_DATASET_ID, len(rows))

    return _records(rows, snapshot=snapshot, source_id_fields=("Num_Acc", "id_vehicule"))


def _user_records() -> tuple[RawRecord, ...]:
    rows = _fixture("usagers.csv")
    snapshot = _snapshot("snap-baac-usagers", BAAC_USERS_DATASET_ID, len(rows))

    return _records(rows, snapshot=snapshot, source_id_fields=("Num_Acc", "id_usager"))


def _fetch_config(client: httpx.AsyncClient) -> BaacFetchConfig:
    return BaacFetchConfig(client=client, clock=lambda: PINNED_NOW)


def test_source_metadata_preserves_data_gouv_release_identity_scope_and_caveats() -> None:
    assert SOURCE_ID == "onisr-open-data"
    assert BAAC_SOURCE_YEAR == "2024"
    assert BAAC_RELEASE == "2024-data-gouv-2025-12-29"
    assert BAAC_DATASET_LAST_UPDATE == "2025-12-29T09:29:20.308000+00:00"
    assert BAAC_JURISDICTION.country == "FR"
    assert BAAC_CHARACTERISTICS_RESOURCE_TITLE == "Caract_2024.csv"
    assert BAAC_VEHICLES_RESOURCE_TITLE == "Vehicules_2024.csv"
    assert BAAC_USERS_RESOURCE_TITLE == "Usagers_2024.csv"
    assert BAAC_CHARACTERISTICS_RESOURCE_ID == "83f0fb0e-e0ef-47fe-93dd-9aaee851674a"
    assert BAAC_VEHICLES_RESOURCE_ID == "fd30513c-6b11-4a56-b6dc-5ac87728794b"
    assert BAAC_USERS_RESOURCE_ID == "f57b1f58-386d-4048-8f78-2ebe435df868"
    assert BAAC_LICENCE == "Licence Ouverte / Open Licence"
    assert "Injury road traffic collisions" in BAAC_SOURCE_SCOPE
    assert any("property-damage-only" in caveat for caveat in BAAC_RELEASE_CAVEATS)
    assert any("Hospitalised-injury" in caveat for caveat in BAAC_RELEASE_CAVEATS)
    assert COLLISION_MAPPER_ID == "baac-collisions"
    assert VEHICLE_MAPPER_ID == "baac-vehicles"
    assert USER_MAPPER_ID == "baac-users"
    assert MAPPER_VERSION == "0.1.0"


def test_upstream_urls_target_data_gouv_resources() -> None:
    assert BAAC_CHARACTERISTICS_URL == (
        f"https://www.data.gouv.fr/fr/datasets/r/{BAAC_CHARACTERISTICS_RESOURCE_ID}"
    )

    assert BAAC_VEHICLES_URL == (
        f"https://www.data.gouv.fr/fr/datasets/r/{BAAC_VEHICLES_RESOURCE_ID}"
    )

    assert BAAC_USERS_URL == (f"https://www.data.gouv.fr/fr/datasets/r/{BAAC_USERS_RESOURCE_ID}")


def test_collision_mapper_decodes_characteristics_and_marks_join_fields_unmapped() -> None:
    records = _characteristic_records()
    snapshot = _snapshot(
        "snap-baac-caracteristiques",
        BAAC_CHARACTERISTICS_DATASET_ID,
        len(records),
    )

    result = BaacCollisionMapper()(records[0], snapshot)
    collision = result.record

    assert collision.collision_id == "202400000001"
    assert collision.severity.quality is FieldQuality.UNMAPPED
    assert collision.occurred_at.value is not None
    assert collision.occurred_at.value.precision is OccurrenceTimePrecision.DATETIME
    assert collision.coordinate.value is not None
    assert collision.coordinate.value.latitude == 48.8566
    assert collision.coordinate.value.longitude == 2.3522
    assert collision.intersection_related.value is True
    assert collision.location_description.value == "Built-up area"
    assert collision.weather.value is not None
    assert collision.weather.value.label == "Normal"
    assert collision.lighting.value is not None
    assert collision.lighting.value.label == "Daylight"
    assert collision.road_names.quality is FieldQuality.UNMAPPED
    assert collision.road_surface.quality is FieldQuality.UNMAPPED
    assert collision.speed_limit.quality is FieldQuality.UNMAPPED
    assert collision.total_injured_count.quality is FieldQuality.UNMAPPED
    assert collision.vehicle_count.quality is FieldQuality.UNMAPPED
    assert collision.person_count.quality is FieldQuality.UNMAPPED
    assert "dep" in result.report.unmapped_source_fields


def test_collision_mapper_handles_date_only_and_missing_coordinates() -> None:
    records = _characteristic_records()
    snapshot = _snapshot(
        "snap-baac-caracteristiques",
        BAAC_CHARACTERISTICS_DATASET_ID,
        len(records),
    )

    collision = BaacCollisionMapper()(records[1], snapshot).record

    assert collision.occurred_at.value is not None
    assert collision.occurred_at.value.precision is OccurrenceTimePrecision.DATE
    assert collision.coordinate.quality is FieldQuality.NOT_PROVIDED
    assert collision.intersection_related.value is False
    assert collision.weather.value is not None
    assert collision.weather.value.code == "2"
    assert collision.weather.value.label == "Light rain"


def test_collision_mapper_does_not_treat_intersection_topology_as_traffic_control() -> None:
    rows = _fixture("caracteristiques.csv")
    rows[0]["int"] = "3"
    rows[0]["atm"] = "99"
    snapshot = _snapshot("snap-baac-caracteristiques", BAAC_CHARACTERISTICS_DATASET_ID, len(rows))
    records = _records(rows, snapshot=snapshot, source_id_fields=("Num_Acc",))

    result = BaacCollisionMapper()(records[0], snapshot)

    assert result.record.intersection_related.value is True
    assert result.record.intersection_related.quality is FieldQuality.STANDARDIZED
    assert result.record.traffic_control.quality is FieldQuality.UNMAPPED
    assert result.record.weather.value is not None
    assert result.record.weather.value.label == "Unsupported code 99"
    assert result.record.weather.quality is FieldQuality.INFERRED


def test_vehicle_mapper_decodes_categories_roles_and_unmapped_source_fields() -> None:
    records = _vehicle_records()
    snapshot = _snapshot("snap-baac-vehicules", BAAC_VEHICLES_DATASET_ID, len(records))

    car = BaacVehicleMapper()(records[0], snapshot)
    bicycle = BaacVehicleMapper()(records[1], snapshot).record
    motorcycle = BaacVehicleMapper()(records[2], snapshot).record

    assert car.record.category.value is VehicleCategory.PASSENGER_CAR
    assert car.record.road_user_role.value is RoadUserRole.UNKNOWN
    assert car.record.road_user_role.quality is FieldQuality.INFERRED
    assert car.record.occupant_count.value == 1
    assert car.record.maneuver.value is not None
    assert car.record.maneuver.value.label == "Straight ahead"
    assert "obs" in car.report.unmapped_source_fields
    assert "choc" in car.report.unmapped_source_fields
    assert bicycle.category.value is VehicleCategory.BICYCLE
    assert bicycle.road_user_role.value is RoadUserRole.CYCLIST
    assert motorcycle.category.value is VehicleCategory.MOTORCYCLE
    assert motorcycle.road_user_role.value is RoadUserRole.MOTORCYCLIST


def test_user_mapper_decodes_role_injury_and_optional_age_context() -> None:
    records = _user_records()
    snapshot = _snapshot("snap-baac-usagers", BAAC_USERS_DATASET_ID, len(records))

    driver = BaacUserMapper()(records[0], snapshot, occurrence_year=2024)
    pedestrian = BaacUserMapper()(records[2], snapshot, occurrence_year=2024)

    assert driver.record.role.value is RoadUserRole.DRIVER
    assert driver.record.injury_outcome.value is InjuryOutcome.SERIOUS
    assert driver.record.age.value == 44
    assert driver.record.safety_equipment.value is not None
    assert driver.record.safety_equipment.value.label == "Seat belt"
    assert driver.record.contributing_factors.quality is FieldQuality.NOT_PROVIDED
    assert pedestrian.record.role.value is RoadUserRole.PEDESTRIAN
    assert pedestrian.record.injury_outcome.value is InjuryOutcome.FATAL
    assert pedestrian.record.contributing_factors.value is not None
    assert pedestrian.record.contributing_factors.value[0].raw_label == "Crossing"
    assert pedestrian.record.contributing_factors.value[0].category is not None
    assert pedestrian.record.contributing_factors.value[0].category.taxonomy_id == "baac-actp"
    assert "locp" in pedestrian.report.unmapped_source_fields
    assert "etatp" in pedestrian.report.unmapped_source_fields


def test_fixture_raw_records_match_schema_and_taxonomies() -> None:
    cases = (
        (
            _characteristic_records(),
            BAAC_CHARACTERISTICS_SCHEMA,
            BAAC_CHARACTERISTICS_TAXONOMIES,
            BAAC_CHARACTERISTICS_DATASET_ID,
        ),
        (
            _vehicle_records(),
            BAAC_VEHICLES_SCHEMA,
            BAAC_VEHICLES_TAXONOMIES,
            BAAC_VEHICLES_DATASET_ID,
        ),
        (
            _user_records(),
            BAAC_USERS_SCHEMA,
            BAAC_USERS_TAXONOMIES,
            BAAC_USERS_DATASET_ID,
        ),
    )

    for records, schema, taxonomies, dataset_id in cases:
        snapshot = _snapshot(f"snap-{schema.spec_id}", dataset_id, len(records))
        schema_observer = SchemaObserver(spec=schema)
        taxonomy_observer = TaxonomyObserver(specs=taxonomies)

        for record in records:
            schema_observer.observe(record)
            taxonomy_observer.observe(record)

        schema_report = schema_observer.finalize(snapshot)
        taxonomy_report = taxonomy_observer.finalize(snapshot)

        assert schema_report.findings == ()
        assert taxonomy_report.findings == ()


def test_unknown_baac_code_surfaces_as_taxonomy_drift() -> None:
    rows = _fixture("vehicules.csv")
    rows[0]["catv"] = "999"
    snapshot = _snapshot("snap-baac-vehicules", BAAC_VEHICLES_DATASET_ID, len(rows))
    records = _records(rows, snapshot=snapshot, source_id_fields=("Num_Acc", "id_vehicule"))
    taxonomy_observer = TaxonomyObserver(specs=BAAC_VEHICLES_TAXONOMIES)

    for record in records:
        taxonomy_observer.observe(record)

    report = taxonomy_observer.finalize(snapshot)

    assert any(
        finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE and finding.taxonomy_id == "baac-catv"
        for finding in report.findings
    )

    assert report.has_errors


def test_unknown_baac_place_surfaces_as_taxonomy_drift() -> None:
    rows = _fixture("usagers.csv")
    rows[0]["place"] = "99"
    snapshot = _snapshot("snap-baac-usagers", BAAC_USERS_DATASET_ID, len(rows))
    records = _records(rows, snapshot=snapshot, source_id_fields=("Num_Acc", "id_usager"))
    taxonomy_observer = TaxonomyObserver(specs=BAAC_USERS_TAXONOMIES)

    for record in records:
        taxonomy_observer.observe(record)

    report = taxonomy_observer.finalize(snapshot)

    assert any(
        finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE and finding.taxonomy_id == "baac-place"
        for finding in report.findings
    )

    assert report.has_errors


class TestCharacteristicsAdapter:
    async def test_pipeline_runs_characteristics_adapter_collision_mapper_pair(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(BAAC_CHARACTERISTICS_URL).mock(
                return_value=httpx.Response(200, content=CHARACTERISTICS_CSV.read_bytes())
            )

            async with httpx.AsyncClient() as client:
                result = await run(
                    BaacCharacteristicsAdapter(fetch_config=_fetch_config(client)),
                    BaacCollisionMapper(),
                )
                records = [record async for record in result.records]

        assert result.snapshot.dataset_id == BAAC_CHARACTERISTICS_DATASET_ID
        assert records[0].mapped.record.collision_id == "202400000001"
        assert records[0].mapped.record.provenance.source_record_id == "202400000001"

    async def test_adapter_fetches_csv_and_streams_records(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(BAAC_CHARACTERISTICS_URL).mock(
                return_value=httpx.Response(200, content=CHARACTERISTICS_CSV.read_bytes())
            )

            async with httpx.AsyncClient() as client:
                adapter = BaacCharacteristicsAdapter(fetch_config=_fetch_config(client))
                result = await adapter.fetch()
                records = [record async for record in result.records]

        assert isinstance(adapter, SourceAdapter)
        assert result.snapshot.source_id == SOURCE_ID
        assert result.snapshot.dataset_id == BAAC_CHARACTERISTICS_DATASET_ID
        assert result.snapshot.jurisdiction.country == "FR"
        assert result.snapshot.source_url == BAAC_CHARACTERISTICS_URL
        assert result.snapshot.fetch_params == {
            "resource_id": BAAC_CHARACTERISTICS_RESOURCE_ID,
            "release": BAAC_RELEASE,
        }

        assert result.snapshot.content_hash is not None
        assert result.snapshot.record_count == 2
        assert records[0].source_record_id == "202400000001"
        assert records[1].source_record_id == "202400000002"
        assert records[0].raw_data["lat"] == "48,8566"
        assert records[1].raw_data["hrmn"] == ""

    async def test_adapter_follows_data_gouv_redirect(self) -> None:
        final_url = "https://static.data.gouv.fr/resources/baac/2024/Caract_2024.csv"

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(BAAC_CHARACTERISTICS_URL).mock(
                return_value=httpx.Response(302, headers={"location": final_url})
            )
            respx_mock.get(final_url).mock(
                return_value=httpx.Response(200, content=CHARACTERISTICS_CSV.read_bytes())
            )

            async with httpx.AsyncClient() as client:
                adapter = BaacCharacteristicsAdapter(fetch_config=_fetch_config(client))
                result = await adapter.fetch()

        assert result.snapshot.record_count == 2

    async def test_adapter_http_failure_is_fetch_error(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(BAAC_CHARACTERISTICS_URL).mock(return_value=httpx.Response(503))

            async with httpx.AsyncClient() as client:
                adapter = BaacCharacteristicsAdapter(fetch_config=_fetch_config(client))

                with pytest.raises(FetchError, match="failed to read BAAC CSV"):
                    await adapter.fetch()

    async def test_adapter_falls_back_to_cp1252(self) -> None:
        text = CHARACTERISTICS_CSV.read_text().replace("12 RUE DE RIVOLI", "12 RUE DE RIVOLI é")

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(BAAC_CHARACTERISTICS_URL).mock(
                return_value=httpx.Response(200, content=text.encode("cp1252"))
            )

            async with httpx.AsyncClient() as client:
                adapter = BaacCharacteristicsAdapter(fetch_config=_fetch_config(client))
                result = await adapter.fetch()
                records = [record async for record in result.records]

        assert records[0].raw_data["adr"] == "12 RUE DE RIVOLI é"

    async def test_adapter_missing_header_field_is_fetch_error(self) -> None:
        bad_csv = CHARACTERISTICS_CSV.read_text().replace("Num_Acc", "num_acc")

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(BAAC_CHARACTERISTICS_URL).mock(
                return_value=httpx.Response(200, content=bad_csv.encode())
            )

            async with httpx.AsyncClient() as client:
                adapter = BaacCharacteristicsAdapter(fetch_config=_fetch_config(client))

                with pytest.raises(FetchError, match="missing required fields"):
                    await adapter.fetch()

    async def test_adapter_rejects_empty_csv(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(BAAC_CHARACTERISTICS_URL).mock(
                return_value=httpx.Response(200, content=b"")
            )

            async with httpx.AsyncClient() as client:
                adapter = BaacCharacteristicsAdapter(fetch_config=_fetch_config(client))

                with pytest.raises(FetchError, match="empty BAAC CSV"):
                    await adapter.fetch()

    async def test_adapter_rejects_non_csv_200_response(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(BAAC_CHARACTERISTICS_URL).mock(
                return_value=httpx.Response(200, content=b"<html>maintenance</html>")
            )

            async with httpx.AsyncClient() as client:
                adapter = BaacCharacteristicsAdapter(fetch_config=_fetch_config(client))

                with pytest.raises(FetchError, match="non-CSV BAAC response"):
                    await adapter.fetch()


class TestVehiclesAdapter:
    async def test_pipeline_runs_vehicle_adapter_mapper_pair(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(BAAC_VEHICLES_URL).mock(
                return_value=httpx.Response(200, content=VEHICLES_CSV.read_bytes())
            )

            async with httpx.AsyncClient() as client:
                result = await run(
                    BaacVehiclesAdapter(fetch_config=_fetch_config(client)),
                    BaacVehicleMapper(),
                )
                records = [record async for record in result.records]

        assert result.snapshot.dataset_id == BAAC_VEHICLES_DATASET_ID
        assert {record.mapped.record.vehicle_id for record in records} == {
            "202400000001:veh-001",
            "202400000001:veh-002",
            "202400000002:veh-003",
        }

    async def test_adapter_fetches_and_yields_three_vehicles(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(BAAC_VEHICLES_URL).mock(
                return_value=httpx.Response(200, content=VEHICLES_CSV.read_bytes())
            )

            async with httpx.AsyncClient() as client:
                adapter = BaacVehiclesAdapter(fetch_config=_fetch_config(client))
                result = await adapter.fetch()
                records = [record async for record in result.records]

        assert result.snapshot.dataset_id == BAAC_VEHICLES_DATASET_ID
        assert result.snapshot.record_count == 3
        assert records[0].source_record_id == "202400000001:veh-001"
        assert records[2].source_record_id == "202400000002:veh-003"


class TestUsersAdapter:
    async def test_pipeline_runs_user_adapter_mapper_pair(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(BAAC_USERS_URL).mock(
                return_value=httpx.Response(200, content=USERS_CSV.read_bytes())
            )

            async with httpx.AsyncClient() as client:
                result = await run(
                    BaacUsersAdapter(fetch_config=_fetch_config(client)),
                    BaacUserMapper(),
                )
                records = [record async for record in result.records]

        assert result.snapshot.dataset_id == BAAC_USERS_DATASET_ID
        assert {record.mapped.record.person_id for record in records} == {
            "202400000001:usr-001",
            "202400000001:usr-002",
            "202400000002:usr-003",
        }
        assert records[0].mapped.record.age.quality is FieldQuality.UNMAPPED

    async def test_adapter_fetches_and_yields_three_users(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(BAAC_USERS_URL).mock(
                return_value=httpx.Response(200, content=USERS_CSV.read_bytes())
            )

            async with httpx.AsyncClient() as client:
                adapter = BaacUsersAdapter(fetch_config=_fetch_config(client))
                result = await adapter.fetch()
                records = [record async for record in result.records]

        assert result.snapshot.dataset_id == BAAC_USERS_DATASET_ID
        assert result.snapshot.record_count == 3
        assert records[0].source_record_id == "202400000001:usr-001"
        assert records[2].raw_data["place"] == ""
