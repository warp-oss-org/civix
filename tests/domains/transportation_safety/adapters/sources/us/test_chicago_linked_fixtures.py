"""Linked fixture tests for the three Chicago traffic-crash source slices."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from civix.core.identity.models.identifiers import DatasetId, SnapshotId
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.domains.transportation_safety.adapters.sources.us.chicago_crashes import (
    CHICAGO_CRASHES_DATASET_ID,
    CHICAGO_JURISDICTION,
    SOURCE_ID,
    ChicagoCrashesMapper,
)
from civix.domains.transportation_safety.adapters.sources.us.chicago_people import (
    CHICAGO_PEOPLE_DATASET_ID,
    ChicagoPeopleMapper,
)
from civix.domains.transportation_safety.adapters.sources.us.chicago_vehicles import (
    CHICAGO_VEHICLES_DATASET_ID,
    ChicagoVehiclesMapper,
)

PINNED_NOW = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
BASE = Path(__file__).parent


def _snapshot(snapshot_id: str, dataset_id: DatasetId) -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId(snapshot_id),
        source_id=SOURCE_ID,
        dataset_id=dataset_id,
        jurisdiction=CHICAGO_JURISDICTION,
        fetched_at=PINNED_NOW,
        record_count=2,
    )


def test_chicago_fixture_records_link_by_source_join_keys() -> None:
    crash_rows = json.loads(
        (BASE / "chicago_crashes" / "fixtures" / "records_page.json").read_text()
    )
    vehicle_rows = json.loads(
        (BASE / "chicago_vehicles" / "fixtures" / "records_page.json").read_text()
    )
    person_rows = json.loads(
        (BASE / "chicago_people" / "fixtures" / "records_page.json").read_text()
    )
    crash_snapshot = _snapshot("snap-crashes", CHICAGO_CRASHES_DATASET_ID)
    vehicle_snapshot = _snapshot("snap-vehicles", CHICAGO_VEHICLES_DATASET_ID)
    person_snapshot = _snapshot("snap-people", CHICAGO_PEOPLE_DATASET_ID)

    collision = ChicagoCrashesMapper()(
        RawRecord(
            snapshot_id=crash_snapshot.snapshot_id,
            raw_data=crash_rows[0],
            source_record_id=crash_rows[0]["crash_record_id"],
        ),
        crash_snapshot,
    ).record
    vehicles = [
        ChicagoVehiclesMapper()(
            RawRecord(
                snapshot_id=vehicle_snapshot.snapshot_id,
                raw_data=row,
                source_record_id=row["crash_unit_id"],
            ),
            vehicle_snapshot,
        ).record
        for row in vehicle_rows
    ]
    people = [
        ChicagoPeopleMapper()(
            RawRecord(
                snapshot_id=person_snapshot.snapshot_id,
                raw_data=row,
                source_record_id=row["person_id"],
            ),
            person_snapshot,
        ).record
        for row in person_rows
    ]

    assert {vehicle.collision_id for vehicle in vehicles} == {collision.collision_id}
    assert {person.collision_id for person in people} == {collision.collision_id}
    linked_person_vehicle_ids = {person.vehicle_id for person in people if person.vehicle_id}

    assert linked_person_vehicle_ids <= {vehicle.vehicle_id for vehicle in vehicles}
    assert any(person.vehicle_id is None for person in people)
