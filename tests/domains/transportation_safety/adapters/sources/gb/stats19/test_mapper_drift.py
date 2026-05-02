"""Tests for the Great Britain STATS19 source slice."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from civix.core.drift import SchemaObserver, TaxonomyDriftKind, TaxonomyObserver
from civix.core.identity.models.identifiers import DatasetId, SnapshotId
from civix.core.mapping.errors import MappingError
from civix.core.quality.models.fields import FieldQuality
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.domains.transportation_safety.adapters.sources.gb.stats19 import (
    CASUALTY_MAPPER_ID,
    COLLISION_MAPPER_ID,
    MAPPER_VERSION,
    SOURCE_ID,
    STATS19_CASUALTIES_DATASET_ID,
    STATS19_CASUALTIES_SCHEMA,
    STATS19_CASUALTIES_TAXONOMIES,
    STATS19_COLLISIONS_DATASET_ID,
    STATS19_COLLISIONS_SCHEMA,
    STATS19_COLLISIONS_TAXONOMIES,
    STATS19_JURISDICTION,
    STATS19_RELEASE,
    STATS19_RELEASE_CAVEATS,
    STATS19_SOURCE_SCOPE,
    STATS19_VEHICLES_DATASET_ID,
    STATS19_VEHICLES_SCHEMA,
    STATS19_VEHICLES_TAXONOMIES,
    VEHICLE_MAPPER_ID,
    Stats19CasualtyMapper,
    Stats19CollisionMapper,
    Stats19LinkedMapper,
    Stats19VehicleMapper,
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
        jurisdiction=STATS19_JURISDICTION,
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


def _collision_records() -> tuple[RawRecord, ...]:
    rows = _fixture("collisions.json")
    snapshot = _snapshot("snap-stats19-collisions", STATS19_COLLISIONS_DATASET_ID, len(rows))

    return _records(rows, snapshot=snapshot, source_id_fields=("accident_index",))


def _vehicle_records() -> tuple[RawRecord, ...]:
    rows = _fixture("vehicles.json")
    snapshot = _snapshot("snap-stats19-vehicles", STATS19_VEHICLES_DATASET_ID, len(rows))

    return _records(
        rows,
        snapshot=snapshot,
        source_id_fields=("accident_index", "vehicle_reference"),
    )


def _casualty_records() -> tuple[RawRecord, ...]:
    rows = _fixture("casualties.json")
    snapshot = _snapshot("snap-stats19-casualties", STATS19_CASUALTIES_DATASET_ID, len(rows))

    return _records(
        rows,
        snapshot=snapshot,
        source_id_fields=("accident_index", "casualty_reference"),
    )


def test_source_metadata_preserves_release_scope_and_caveats() -> None:
    assert SOURCE_ID == "dft-open-data"
    assert STATS19_RELEASE == "2024-final"
    assert STATS19_JURISDICTION.country == "GB"
    assert "Personal injury road collisions" in STATS19_SOURCE_SCOPE
    assert any("contributory factors" in caveat for caveat in STATS19_RELEASE_CAVEATS)
    assert COLLISION_MAPPER_ID == "stats19-collisions"
    assert VEHICLE_MAPPER_ID == "stats19-vehicles"
    assert CASUALTY_MAPPER_ID == "stats19-casualties"
    assert MAPPER_VERSION == "0.1.0"


def test_linked_fixture_maps_collision_vehicles_and_casualties() -> None:
    collision_records = _collision_records()
    vehicle_records = _vehicle_records()
    casualty_records = _casualty_records()
    result = Stats19LinkedMapper().map_records(
        collisions=collision_records,
        vehicles=vehicle_records,
        casualties=casualty_records,
        collision_snapshot=_snapshot(
            "snap-stats19-collisions",
            STATS19_COLLISIONS_DATASET_ID,
            len(collision_records),
        ),
        vehicle_snapshot=_snapshot(
            "snap-stats19-vehicles",
            STATS19_VEHICLES_DATASET_ID,
            len(vehicle_records),
        ),
        casualty_snapshot=_snapshot(
            "snap-stats19-casualties",
            STATS19_CASUALTIES_DATASET_ID,
            len(casualty_records),
        ),
    )

    linked = result[0]
    collision = linked.collision.record
    vehicles = [vehicle.record for vehicle in linked.vehicles]
    people = [person.record for person in linked.people]

    assert len(result) == 1
    assert collision.collision_id == "2024010000001"
    assert {vehicle.vehicle_id for vehicle in vehicles} == {
        "2024010000001:1",
        "2024010000001:2",
    }
    assert {person.person_id for person in people} == {
        "2024010000001:1",
        "2024010000001:2",
    }
    assert people[0].vehicle_id == "2024010000001:1"
    assert people[1].vehicle_id is None
    assert linked.collision.record.provenance.source_record_id == "2024010000001"
    assert linked.vehicles[0].record.provenance.source_record_id == "2024010000001:1"
    assert linked.people[0].record.provenance.source_record_id == "2024010000001:1"


def test_collision_mapper_decodes_core_context_fields() -> None:
    records = _collision_records()
    snapshot = _snapshot("snap-stats19-collisions", STATS19_COLLISIONS_DATASET_ID, len(records))

    result = Stats19CollisionMapper()(records[0], snapshot)
    collision = result.record

    assert collision.severity.value is CollisionSeverity.SERIOUS_INJURY
    assert collision.occurred_at.value is not None
    assert collision.occurred_at.value.precision is OccurrenceTimePrecision.DATETIME
    assert collision.coordinate.value is not None
    assert collision.coordinate.value.latitude == 51.5074
    assert collision.intersection_related.value is True
    assert collision.speed_limit.value is not None
    assert collision.speed_limit.value.value == 30
    assert collision.speed_limit.value.unit is SpeedLimitUnit.MILES_PER_HOUR
    assert collision.weather.value is not None
    assert collision.weather.value.label == "Fine no high winds"
    assert collision.lighting.value is not None
    assert collision.road_surface.value is not None
    assert collision.traffic_control.value is not None
    assert collision.contributing_factors.quality is FieldQuality.NOT_PROVIDED
    assert "road_type" in result.report.unmapped_source_fields
    assert "first_road_class" in result.report.unmapped_source_fields


def test_vehicle_mapper_decodes_category_role_and_manoeuvre() -> None:
    records = _vehicle_records()
    snapshot = _snapshot("snap-stats19-vehicles", STATS19_VEHICLES_DATASET_ID, len(records))

    car = Stats19VehicleMapper()(records[0], snapshot).record
    cycle = Stats19VehicleMapper()(records[1], snapshot).record

    assert car.category.value is VehicleCategory.PASSENGER_CAR
    assert car.road_user_role.value is RoadUserRole.UNKNOWN
    assert car.road_user_role.quality is FieldQuality.INFERRED
    assert car.maneuver.value is not None
    assert car.maneuver.value.label == "Going ahead other"
    assert car.travel_direction.value is not None
    assert car.travel_direction.value.taxonomy_id == "stats19-vehicle-direction-to"
    assert cycle.category.value is VehicleCategory.BICYCLE
    assert cycle.road_user_role.value is RoadUserRole.CYCLIST


def test_casualty_mapper_handles_age_band_and_pedestrian_no_vehicle_case() -> None:
    records = _casualty_records()
    snapshot = _snapshot("snap-stats19-casualties", STATS19_CASUALTIES_DATASET_ID, len(records))

    passenger = Stats19CasualtyMapper()(records[0], snapshot)
    pedestrian = Stats19CasualtyMapper()(records[1], snapshot)

    assert passenger.record.vehicle_id == "2024010000001:1"
    assert passenger.record.role.value is RoadUserRole.PASSENGER
    assert passenger.record.injury_outcome.value is InjuryOutcome.SERIOUS
    assert passenger.record.age.value == 34
    assert "age_band_of_casualty" in passenger.report.unmapped_source_fields
    assert pedestrian.record.vehicle_id is None
    assert pedestrian.record.role.value is RoadUserRole.PEDESTRIAN
    assert pedestrian.record.injury_outcome.value is InjuryOutcome.MINOR
    assert pedestrian.record.age.quality is FieldQuality.NOT_PROVIDED


def test_unsupported_codes_are_not_reported_as_standardized() -> None:
    collision_rows = _fixture("collisions.json")
    collision_rows[0]["junction_detail"] = "42"
    collision_rows[0]["weather_conditions"] = "42"
    collision_snapshot = _snapshot(
        "snap-stats19-collisions",
        STATS19_COLLISIONS_DATASET_ID,
        len(collision_rows),
    )
    collision_records = _records(
        collision_rows,
        snapshot=collision_snapshot,
        source_id_fields=("accident_index",),
    )
    collision = Stats19CollisionMapper()(collision_records[0], collision_snapshot)

    assert collision.record.intersection_related.quality is FieldQuality.UNMAPPED
    assert collision.record.weather.value is not None
    assert collision.record.weather.value.label == "Unsupported code 42"
    assert collision.record.weather.quality is FieldQuality.INFERRED
    assert "junction_detail" in collision.report.unmapped_source_fields

    vehicle_rows = _fixture("vehicles.json")
    vehicle_rows[0]["vehicle_type"] = "42"
    vehicle_snapshot = _snapshot("snap-stats19-vehicles", STATS19_VEHICLES_DATASET_ID, 1)
    vehicle_records = _records(
        [vehicle_rows[0]],
        snapshot=vehicle_snapshot,
        source_id_fields=("accident_index", "vehicle_reference"),
    )
    vehicle = Stats19VehicleMapper()(vehicle_records[0], vehicle_snapshot).record

    assert vehicle.category.value is VehicleCategory.UNKNOWN
    assert vehicle.category.quality is FieldQuality.INFERRED
    assert vehicle.road_user_role.value is RoadUserRole.UNKNOWN
    assert vehicle.road_user_role.quality is FieldQuality.INFERRED

    casualty_rows = _fixture("casualties.json")
    casualty_rows[0]["casualty_severity"] = "42"
    casualty_snapshot = _snapshot("snap-stats19-casualties", STATS19_CASUALTIES_DATASET_ID, 1)
    casualty_records = _records(
        [casualty_rows[0]],
        snapshot=casualty_snapshot,
        source_id_fields=("accident_index", "casualty_reference"),
    )
    casualty = Stats19CasualtyMapper()(casualty_records[0], casualty_snapshot).record

    assert casualty.injury_outcome.value is InjuryOutcome.UNKNOWN
    assert casualty.injury_outcome.quality is FieldQuality.INFERRED


def test_documented_unknown_codes_are_not_provided_not_standardized_unknowns() -> None:
    rows = _fixture("collisions.json")
    rows[0]["weather_conditions"] = "9"
    rows[0]["junction_control"] = "9"
    snapshot = _snapshot("snap-stats19-collisions", STATS19_COLLISIONS_DATASET_ID, len(rows))
    records = _records(rows, snapshot=snapshot, source_id_fields=("accident_index",))

    collision = Stats19CollisionMapper()(records[0], snapshot).record

    assert collision.weather.quality is FieldQuality.NOT_PROVIDED
    assert collision.traffic_control.quality is FieldQuality.NOT_PROVIDED


def test_unsupported_negative_casualty_vehicle_reference_raises_mapping_error() -> None:
    rows = _fixture("casualties.json")
    rows[0]["vehicle_reference"] = "-2"
    snapshot = _snapshot("snap-stats19-casualties", STATS19_CASUALTIES_DATASET_ID, len(rows))
    records = _records(
        rows,
        snapshot=snapshot,
        source_id_fields=("accident_index", "casualty_reference"),
    )

    with pytest.raises(MappingError) as excinfo:
        Stats19CasualtyMapper()(records[0], snapshot)

    assert excinfo.value.source_fields == ("vehicle_reference",)


def test_fixture_raw_records_match_schema_and_taxonomies() -> None:
    cases = (
        (
            _collision_records(),
            STATS19_COLLISIONS_SCHEMA,
            STATS19_COLLISIONS_TAXONOMIES,
            STATS19_COLLISIONS_DATASET_ID,
        ),
        (
            _vehicle_records(),
            STATS19_VEHICLES_SCHEMA,
            STATS19_VEHICLES_TAXONOMIES,
            STATS19_VEHICLES_DATASET_ID,
        ),
        (
            _casualty_records(),
            STATS19_CASUALTIES_SCHEMA,
            STATS19_CASUALTIES_TAXONOMIES,
            STATS19_CASUALTIES_DATASET_ID,
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


def test_unknown_stats19_code_surfaces_as_taxonomy_drift() -> None:
    rows = _fixture("collisions.json")
    rows[0]["weather_conditions"] = "42"
    snapshot = _snapshot("snap-stats19-collisions", STATS19_COLLISIONS_DATASET_ID, len(rows))
    records = _records(rows, snapshot=snapshot, source_id_fields=("accident_index",))
    taxonomy_observer = TaxonomyObserver(specs=STATS19_COLLISIONS_TAXONOMIES)

    for record in records:
        taxonomy_observer.observe(record)

    report = taxonomy_observer.finalize(snapshot)

    assert any(
        finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE
        and finding.taxonomy_id == "stats19-weather-conditions"
        for finding in report.findings
    )
    assert report.has_errors
