"""Tests for the France BAAC / ONISR source slice."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from civix.core.drift import SchemaObserver, TaxonomyDriftKind, TaxonomyObserver
from civix.core.identity.models.identifiers import DatasetId, SnapshotId
from civix.core.quality.models.fields import FieldQuality
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.domains.transportation_safety.adapters.sources.fr.baac import (
    BAAC_CHARACTERISTICS_DATASET_ID,
    BAAC_CHARACTERISTICS_RESOURCE_ID,
    BAAC_CHARACTERISTICS_RESOURCE_TITLE,
    BAAC_CHARACTERISTICS_SCHEMA,
    BAAC_CHARACTERISTICS_TAXONOMIES,
    BAAC_DATASET_LAST_UPDATE,
    BAAC_JURISDICTION,
    BAAC_LICENCE,
    BAAC_LOCATIONS_DATASET_ID,
    BAAC_LOCATIONS_RESOURCE_ID,
    BAAC_LOCATIONS_RESOURCE_TITLE,
    BAAC_LOCATIONS_SCHEMA,
    BAAC_LOCATIONS_TAXONOMIES,
    BAAC_RELEASE,
    BAAC_RELEASE_CAVEATS,
    BAAC_SOURCE_SCOPE,
    BAAC_SOURCE_YEAR,
    BAAC_USERS_DATASET_ID,
    BAAC_USERS_RESOURCE_ID,
    BAAC_USERS_RESOURCE_TITLE,
    BAAC_USERS_SCHEMA,
    BAAC_USERS_TAXONOMIES,
    BAAC_VEHICLES_DATASET_ID,
    BAAC_VEHICLES_RESOURCE_ID,
    BAAC_VEHICLES_RESOURCE_TITLE,
    BAAC_VEHICLES_SCHEMA,
    BAAC_VEHICLES_TAXONOMIES,
    COLLISION_MAPPER_ID,
    MAPPER_VERSION,
    SOURCE_ID,
    USER_MAPPER_ID,
    VEHICLE_MAPPER_ID,
    BaacCollisionMapper,
    BaacLinkedMapper,
)
from civix.domains.transportation_safety.models.collision import CollisionSeverity
from civix.domains.transportation_safety.models.parties import RoadUserRole
from civix.domains.transportation_safety.models.person import InjuryOutcome
from civix.domains.transportation_safety.models.road import SpeedLimitUnit
from civix.domains.transportation_safety.models.time import OccurrenceTimePrecision
from civix.domains.transportation_safety.models.vehicle import VehicleCategory

PINNED_NOW = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
FIXTURES = Path(__file__).parent / "fixtures"


def _fixture(name: str) -> list[dict[str, Any]]:
    return json.loads((FIXTURES / name).read_text())


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
    rows = _fixture("caracteristiques.json")
    snapshot = _snapshot("snap-baac-caracteristiques", BAAC_CHARACTERISTICS_DATASET_ID, len(rows))

    return _records(rows, snapshot=snapshot, source_id_fields=("Num_Acc",))


def _location_records() -> tuple[RawRecord, ...]:
    rows = _fixture("lieux.json")
    snapshot = _snapshot("snap-baac-lieux", BAAC_LOCATIONS_DATASET_ID, len(rows))

    return _records(rows, snapshot=snapshot, source_id_fields=("Num_Acc",))


def _vehicle_records() -> tuple[RawRecord, ...]:
    rows = _fixture("vehicules.json")
    snapshot = _snapshot("snap-baac-vehicules", BAAC_VEHICLES_DATASET_ID, len(rows))

    return _records(rows, snapshot=snapshot, source_id_fields=("Num_Acc", "id_vehicule"))


def _user_records() -> tuple[RawRecord, ...]:
    rows = _fixture("usagers.json")
    snapshot = _snapshot("snap-baac-usagers", BAAC_USERS_DATASET_ID, len(rows))

    return _records(rows, snapshot=snapshot, source_id_fields=("Num_Acc", "id_usager"))


def _linked_results():
    characteristics = _characteristic_records()
    locations = _location_records()
    vehicles = _vehicle_records()
    users = _user_records()

    return BaacLinkedMapper().map_records(
        characteristics=characteristics,
        locations=locations,
        vehicles=vehicles,
        users=users,
        characteristics_snapshot=_snapshot(
            "snap-baac-caracteristiques",
            BAAC_CHARACTERISTICS_DATASET_ID,
            len(characteristics),
        ),
        locations_snapshot=_snapshot("snap-baac-lieux", BAAC_LOCATIONS_DATASET_ID, len(locations)),
        vehicles_snapshot=_snapshot(
            "snap-baac-vehicules",
            BAAC_VEHICLES_DATASET_ID,
            len(vehicles),
        ),
        users_snapshot=_snapshot("snap-baac-usagers", BAAC_USERS_DATASET_ID, len(users)),
    )


def test_source_metadata_preserves_data_gouv_release_identity_scope_and_caveats() -> None:
    assert SOURCE_ID == "onisr-open-data"
    assert BAAC_SOURCE_YEAR == "2024"
    assert BAAC_RELEASE == "2024-data-gouv-2025-12-29"
    assert BAAC_DATASET_LAST_UPDATE == "2025-12-29T09:29:20.308000+00:00"
    assert BAAC_JURISDICTION.country == "FR"
    assert BAAC_CHARACTERISTICS_RESOURCE_TITLE == "Caract_2024.csv"
    assert BAAC_LOCATIONS_RESOURCE_TITLE == "Lieux_2024.csv"
    assert BAAC_VEHICLES_RESOURCE_TITLE == "Vehicules_2024.csv"
    assert BAAC_USERS_RESOURCE_TITLE == "Usagers_2024.csv"
    assert BAAC_CHARACTERISTICS_RESOURCE_ID == "83f0fb0e-e0ef-47fe-93dd-9aaee851674a"
    assert BAAC_LOCATIONS_RESOURCE_ID == "228b3cda-fdfb-4677-bd54-ab2107028d2d"
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


def test_linked_fixture_maps_collisions_vehicles_and_users() -> None:
    results = _linked_results()
    first = results[0]
    second = results[1]

    assert len(results) == 2
    assert first.collision.record.collision_id == "202400000001"
    assert first.collision.record.provenance.source_record_id == "202400000001"
    assert {vehicle.record.vehicle_id for vehicle in first.vehicles} == {
        "202400000001:veh-001",
        "202400000001:veh-002",
    }
    assert {person.record.person_id for person in first.people} == {
        "202400000001:usr-001",
        "202400000001:usr-002",
    }
    assert first.people[0].record.vehicle_id == "202400000001:veh-001"
    assert first.vehicles[0].record.provenance.source_record_id == "202400000001:veh-001"
    assert first.people[0].record.provenance.source_record_id == "202400000001:usr-001"
    assert second.people[0].record.vehicle_id is None


def test_collision_mapper_decodes_context_counts_and_french_decimal_coordinates() -> None:
    first = _linked_results()[0].collision
    collision = first.record

    assert collision.severity.value is CollisionSeverity.SERIOUS_INJURY
    assert collision.severity.quality is FieldQuality.DERIVED
    assert collision.occurred_at.value is not None
    assert collision.occurred_at.value.precision is OccurrenceTimePrecision.DATETIME
    assert collision.coordinate.value is not None
    assert collision.coordinate.value.latitude == 48.8566
    assert collision.coordinate.value.longitude == 2.3522
    assert collision.intersection_related.value is True
    assert collision.road_names.value == ("D1",)
    assert collision.weather.value is not None
    assert collision.weather.value.code == "1"
    assert collision.weather.value.label == "Normal"
    assert collision.lighting.value is not None
    assert collision.lighting.value.code == "1"
    assert collision.lighting.value.label == "Daylight"
    assert collision.road_surface.value is not None
    assert collision.road_surface.value.code == "1"
    assert collision.road_surface.value.label == "Normal"
    assert collision.traffic_control.quality is FieldQuality.UNMAPPED
    assert collision.speed_limit.value is not None
    assert collision.speed_limit.value.value == 50
    assert collision.speed_limit.value.unit is SpeedLimitUnit.KILOMETRES_PER_HOUR
    assert collision.serious_injury_count.value == 1
    assert collision.minor_injury_count.value == 1
    assert collision.total_injured_count.value == 2
    assert collision.vehicle_count.value == 2
    assert collision.person_count.value == 2
    assert "catr" in first.report.unmapped_source_fields
    assert "dep" in first.report.unmapped_source_fields


def test_collision_mapper_handles_date_only_and_missing_coordinates() -> None:
    collision = _linked_results()[1].collision.record

    assert collision.severity.value is CollisionSeverity.FATAL
    assert collision.occurred_at.value is not None
    assert collision.occurred_at.value.precision is OccurrenceTimePrecision.DATE
    assert collision.coordinate.quality is FieldQuality.NOT_PROVIDED
    assert collision.intersection_related.value is False
    assert collision.weather.value is not None
    assert collision.weather.value.code == "2"
    assert collision.weather.value.label == "Light rain"
    assert collision.fatal_count.value == 1


def test_collision_mapper_does_not_treat_intersection_topology_as_traffic_control() -> None:
    characteristic_rows = _fixture("caracteristiques.json")
    location_rows = _fixture("lieux.json")
    characteristic_rows[0]["int"] = "3"
    characteristic_rows[0]["atm"] = "99"
    characteristic_snapshot = _snapshot(
        "snap-baac-caracteristiques",
        BAAC_CHARACTERISTICS_DATASET_ID,
        len(characteristic_rows),
    )
    location_snapshot = _snapshot("snap-baac-lieux", BAAC_LOCATIONS_DATASET_ID, len(location_rows))
    characteristic_records = _records(
        [characteristic_rows[0]],
        snapshot=characteristic_snapshot,
        source_id_fields=("Num_Acc",),
    )
    location_records = _records(
        [location_rows[0]],
        snapshot=location_snapshot,
        source_id_fields=("Num_Acc",),
    )

    result = BaacCollisionMapper()(
        characteristic_records[0],
        location_records[0],
        characteristic_snapshot,
    )

    assert result.record.intersection_related.value is True
    assert result.record.intersection_related.quality is FieldQuality.STANDARDIZED
    assert result.record.traffic_control.quality is FieldQuality.UNMAPPED
    assert result.record.weather.value is not None
    assert result.record.weather.value.label == "Unsupported code 99"
    assert result.record.weather.quality is FieldQuality.INFERRED


def test_unknown_intersection_topology_still_marks_intersection_related() -> None:
    characteristic_rows = _fixture("caracteristiques.json")
    location_rows = _fixture("lieux.json")
    characteristic_rows[0]["int"] = "99"
    characteristic_snapshot = _snapshot(
        "snap-baac-caracteristiques",
        BAAC_CHARACTERISTICS_DATASET_ID,
        len(characteristic_rows),
    )
    location_snapshot = _snapshot("snap-baac-lieux", BAAC_LOCATIONS_DATASET_ID, len(location_rows))
    characteristic_records = _records(
        [characteristic_rows[0]],
        snapshot=characteristic_snapshot,
        source_id_fields=("Num_Acc",),
    )
    location_records = _records(
        [location_rows[0]],
        snapshot=location_snapshot,
        source_id_fields=("Num_Acc",),
    )

    result = BaacCollisionMapper()(
        characteristic_records[0],
        location_records[0],
        characteristic_snapshot,
    )

    assert result.record.intersection_related.value is True
    assert result.record.intersection_related.quality is FieldQuality.INFERRED


def test_vehicle_mapper_decodes_categories_roles_and_unmapped_source_fields() -> None:
    first = _linked_results()[0]
    car = first.vehicles[0]
    bicycle = first.vehicles[1]
    motorcycle = _linked_results()[1].vehicles[0].record

    assert car.record.category.value is VehicleCategory.PASSENGER_CAR
    assert car.record.road_user_role.value is RoadUserRole.UNKNOWN
    assert car.record.road_user_role.quality is FieldQuality.INFERRED
    assert car.record.occupant_count.value == 1
    assert car.record.maneuver.value is not None
    assert car.record.maneuver.value.label == "Straight ahead"
    assert "obs" in car.report.unmapped_source_fields
    assert "choc" in car.report.unmapped_source_fields
    assert bicycle.record.category.value is VehicleCategory.BICYCLE
    assert bicycle.record.road_user_role.value is RoadUserRole.CYCLIST
    assert motorcycle.category.value is VehicleCategory.MOTORCYCLE
    assert motorcycle.road_user_role.value is RoadUserRole.MOTORCYCLIST


def test_user_mapper_decodes_role_injury_safety_equipment_and_pedestrian_action() -> None:
    first = _linked_results()[0]
    second = _linked_results()[1]
    driver = first.people[0]
    pedestrian = second.people[0]

    assert driver.record.role.value is RoadUserRole.DRIVER
    assert driver.record.injury_outcome.value is InjuryOutcome.SERIOUS
    assert driver.record.age.value == 44
    assert driver.record.safety_equipment.value is not None
    assert driver.record.safety_equipment.value.label == "Seat belt"
    assert driver.record.contributing_factors.quality is FieldQuality.NOT_PROVIDED
    assert pedestrian.record.role.value is RoadUserRole.PEDESTRIAN
    assert pedestrian.record.injury_outcome.value is InjuryOutcome.FATAL
    assert pedestrian.record.safety_equipment.value is not None
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
            _location_records(),
            BAAC_LOCATIONS_SCHEMA,
            BAAC_LOCATIONS_TAXONOMIES,
            BAAC_LOCATIONS_DATASET_ID,
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
    rows = _fixture("vehicules.json")
    rows[0]["catv"] = "999"
    snapshot = _snapshot("snap-baac-vehicules", BAAC_VEHICLES_DATASET_ID, len(rows))
    records = _records(rows, snapshot=snapshot, source_id_fields=("Num_Acc", "id_vehicule"))
    taxonomy_observer = TaxonomyObserver(specs=BAAC_VEHICLES_TAXONOMIES)

    for record in records:
        taxonomy_observer.observe(record)

    report = taxonomy_observer.finalize(snapshot)

    assert any(
        finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE
        and finding.taxonomy_id == "baac-catv"
        for finding in report.findings
    )
    assert report.has_errors


def test_unknown_baac_place_surfaces_as_taxonomy_drift() -> None:
    rows = _fixture("usagers.json")
    rows[0]["place"] = "99"
    snapshot = _snapshot("snap-baac-usagers", BAAC_USERS_DATASET_ID, len(rows))
    records = _records(rows, snapshot=snapshot, source_id_fields=("Num_Acc", "id_usager"))
    taxonomy_observer = TaxonomyObserver(specs=BAAC_USERS_TAXONOMIES)

    for record in records:
        taxonomy_observer.observe(record)

    report = taxonomy_observer.finalize(snapshot)

    assert any(
        finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE
        and finding.taxonomy_id == "baac-place"
        for finding in report.findings
    )
    assert report.has_errors
