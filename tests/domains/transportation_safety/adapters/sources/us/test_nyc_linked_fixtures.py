"""Linked fixture tests for NYC motor-vehicle-collision source slices."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from civix.core.identity.models.identifiers import DatasetId, SnapshotId
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.domains.transportation_safety.adapters.sources.us.nyc_crashes import (
    NYC_CRASHES_DATASET_ID,
    NYC_JURISDICTION,
    SOURCE_ID,
    NycCrashesMapper,
)
from civix.domains.transportation_safety.adapters.sources.us.nyc_persons import (
    NYC_PERSONS_DATASET_ID,
    NycPersonsMapper,
)
from civix.domains.transportation_safety.adapters.sources.us.nyc_vehicles import (
    NYC_VEHICLES_DATASET_ID,
    NycVehiclesMapper,
)

PINNED_NOW = datetime(2026, 5, 2, 12, 0, tzinfo=UTC)
BASE = Path(__file__).parent


def _snapshot(snapshot_id: str, dataset_id: DatasetId) -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId(snapshot_id),
        source_id=SOURCE_ID,
        dataset_id=dataset_id,
        jurisdiction=NYC_JURISDICTION,
        fetched_at=PINNED_NOW,
        record_count=2,
    )


def test_nyc_fixture_records_link_by_source_join_keys_without_linked_mapper() -> None:
    crash_rows = json.loads((BASE / "nyc_crashes" / "fixtures" / "records_page.json").read_text())
    person_rows = json.loads((BASE / "nyc_persons" / "fixtures" / "records_page.json").read_text())
    vehicle_rows = json.loads(
        (BASE / "nyc_vehicles" / "fixtures" / "records_page.json").read_text()
    )
    crash_snapshot = _snapshot("snap-nyc-crashes", NYC_CRASHES_DATASET_ID)
    person_snapshot = _snapshot("snap-nyc-persons", NYC_PERSONS_DATASET_ID)
    vehicle_snapshot = _snapshot("snap-nyc-vehicles", NYC_VEHICLES_DATASET_ID)

    collision = NycCrashesMapper()(
        RawRecord(
            snapshot_id=crash_snapshot.snapshot_id,
            raw_data=crash_rows[0],
            source_record_id=crash_rows[0]["collision_id"],
        ),
        crash_snapshot,
    ).record
    people = [
        NycPersonsMapper()(
            RawRecord(
                snapshot_id=person_snapshot.snapshot_id,
                raw_data=row,
                source_record_id=row["unique_id"],
            ),
            person_snapshot,
        ).record
        for row in person_rows
    ]
    vehicles = [
        NycVehiclesMapper()(
            RawRecord(
                snapshot_id=vehicle_snapshot.snapshot_id,
                raw_data=row,
                source_record_id=row["unique_id"],
            ),
            vehicle_snapshot,
        ).record
        for row in vehicle_rows
    ]

    assert {person.collision_id for person in people} == {collision.collision_id}
    assert {vehicle.collision_id for vehicle in vehicles} == {collision.collision_id}
    linked_person_vehicle_ids = {person.vehicle_id for person in people if person.vehicle_id}

    assert linked_person_vehicle_ids <= {vehicle.vehicle_id for vehicle in vehicles}
    assert any(person.vehicle_id is None for person in people)
