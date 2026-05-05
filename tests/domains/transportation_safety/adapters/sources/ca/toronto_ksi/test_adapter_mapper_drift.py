"""Tests for the Toronto KSI source slice."""

from __future__ import annotations

import json
from collections.abc import AsyncIterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from civix.core.drift import SchemaObserver, TaxonomyDriftKind, TaxonomyObserver
from civix.core.identity.models.identifiers import SnapshotId
from civix.core.mapping.errors import MappingError
from civix.core.pipeline import PipelineRecord, PipelineResult
from civix.core.ports.models.adapter import SourceAdapter
from civix.core.quality.models.fields import FieldQuality
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.domains.transportation_safety.adapters.sources.ca.toronto_ksi import (
    DEFAULT_BASE_URL,
    SOURCE_ID,
    TORONTO_KSI_DATASET_ID,
    TORONTO_KSI_JURISDICTION,
    TORONTO_KSI_SCHEMA,
    TORONTO_KSI_TAXONOMIES,
    TorontoKsiAdapter,
    TorontoKsiGroupedMapper,
)
from civix.domains.transportation_safety.models.collision import (
    CollisionSeverity,
    TrafficCollision,
)
from civix.domains.transportation_safety.models.parties import RoadUserRole
from civix.domains.transportation_safety.models.person import InjuryOutcome
from civix.domains.transportation_safety.models.vehicle import VehicleCategory
from civix.infra.exporters.json import write_snapshot
from civix.infra.sources.ckan import CkanFetchConfig

PINNED_NOW = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
RESOURCE_ID = "c9b88f1f-863e-42f1-ada0-2c09b1e2eaa4"
PACKAGE_SHOW_URL = f"{DEFAULT_BASE_URL}package_show"
DATASTORE_SEARCH_URL = f"{DEFAULT_BASE_URL}datastore_search"
FIXTURES = Path(__file__).parent / "fixtures"


def _adapter(client: httpx.AsyncClient, *, page_size: int = 1000) -> TorontoKsiAdapter:
    return TorontoKsiAdapter(
        fetch_config=CkanFetchConfig(
            client=client,
            clock=lambda: PINNED_NOW,
            page_size=page_size,
        ),
    )


def _snapshot(record_count: int = 2) -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId("snap-toronto-ksi"),
        source_id=SOURCE_ID,
        dataset_id=TORONTO_KSI_DATASET_ID,
        jurisdiction=TORONTO_KSI_JURISDICTION,
        fetched_at=PINNED_NOW,
        record_count=record_count,
    )


def _fixture_rows() -> list[dict[str, Any]]:
    return json.loads((FIXTURES / "records_page.json").read_text())


def _raw_records(*rows: dict[str, Any]) -> tuple[RawRecord, ...]:
    snapshot = _snapshot(record_count=len(rows))

    return tuple(
        RawRecord(
            snapshot_id=snapshot.snapshot_id,
            raw_data=row,
            source_record_id=f"{row['collision_id']}:{row['per_no']}",
        )
        for row in rows
    )


def _group(*row_overrides: dict[str, Any]) -> tuple[RawRecord, ...]:
    rows = _fixture_rows()

    for row, overrides in zip(rows, row_overrides, strict=False):
        row.update(overrides)

    return _raw_records(*rows)


def _package_payload() -> dict[str, Any]:
    return {
        "success": True,
        "result": {
            "is_retired": False,
            "state": "active",
            "refresh_rate": "Annually",
            "last_refreshed": "2024-10-15 17:18:20.899444",
            "resources": [
                {"id": "inactive", "datastore_active": False},
                {"id": RESOURCE_ID, "datastore_active": True},
            ],
        },
    }


def _datastore_payload(records: list[dict[str, Any]]) -> dict[str, Any]:
    return {"success": True, "result": {"total": len(records), "records": records}}


class TestAdapter:
    async def test_fetches_ckan_records_and_preserves_person_level_source_ids(self) -> None:
        rows = _fixture_rows()

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(DATASTORE_SEARCH_URL).mock(
                return_value=httpx.Response(200, json=_datastore_payload(rows))
            )

            async with httpx.AsyncClient() as client:
                adapter = _adapter(client)
                result = await adapter.fetch()
                records = [record async for record in result.records]

        assert isinstance(adapter, SourceAdapter)
        assert result.snapshot.source_id == SOURCE_ID
        assert result.snapshot.dataset_id == TORONTO_KSI_DATASET_ID
        assert result.snapshot.fetch_params == {"resource_id": RESOURCE_ID}
        assert [record.source_record_id for record in records] == ["900001:1", "900001:2"]
        assert records[0].raw_data["stname1"] == "BLOOR ST W"


class TestGroupedMapper:
    def test_maps_grouped_collision_vehicle_and_people(self) -> None:
        result = TorontoKsiGroupedMapper().map_group(_group(), _snapshot())

        collision = result.collision.record
        vehicles = [vehicle.record for vehicle in result.vehicles]
        people = [person.record for person in result.people]

        assert collision.collision_id == "900001"
        assert collision.severity.value is CollisionSeverity.SERIOUS_INJURY
        assert collision.person_count.value == 2
        assert collision.vehicle_count.value == 1
        assert collision.address.value is not None
        assert collision.address.value.locality == "Toronto"
        assert collision.coordinate.value is not None
        assert collision.coordinate.value.latitude == 43.6708
        assert collision.weather.value is not None
        assert collision.weather.value.label == "Clear"
        assert collision.road_condition.quality is FieldQuality.UNMAPPED
        assert collision.contributing_factors.quality is FieldQuality.UNMAPPED
        assert "failtorem" in result.collision.report.unmapped_source_fields
        assert len(result.collision.report.conflicts) >= 1
        assert vehicles[0].vehicle_id == "900001:1"
        assert vehicles[0].category.value is VehicleCategory.PASSENGER_CAR
        assert vehicles[0].road_user_role.value is RoadUserRole.UNKNOWN
        assert {person.person_id for person in people} == {"900001:1", "900001:2"}
        assert people[0].role.value is RoadUserRole.DRIVER
        assert people[1].role.value is RoadUserRole.PEDESTRIAN
        assert people[0].vehicle_id == "900001:1"
        assert people[1].vehicle_id is None

    def test_maps_fatal_collision_from_grouped_person_outcomes(self) -> None:
        records = _group({"acclass": "Fatal", "injury": "Fatal"}, {"acclass": "Fatal"})
        result = TorontoKsiGroupedMapper().map_group(records, _snapshot())

        assert result.collision.record.severity.value is CollisionSeverity.FATAL
        assert result.collision.record.fatal_count.value == 1
        assert result.people[0].record.injury_outcome.value is InjuryOutcome.FATAL

    def test_group_records_sorts_by_collision_id(self) -> None:
        rows = _fixture_rows()
        other = dict(rows[0], collision_id="800001", per_no="1")
        records = _raw_records(rows[0], other, rows[1])

        grouped = TorontoKsiGroupedMapper().group_records(records)

        assert [group[0].raw_data["collision_id"] for group in grouped] == ["800001", "900001"]

    def test_missing_person_number_raises_mapping_error(self) -> None:
        records = _group({}, {"per_no": None})

        with pytest.raises(MappingError) as excinfo:
            TorontoKsiGroupedMapper().map_group(records, _snapshot())

        assert excinfo.value.source_fields == ("per_no",)

    def test_soft_repeated_field_variants_are_reported_not_failed(self) -> None:
        result = TorontoKsiGroupedMapper().map_group(_group(), _snapshot())

        conflict_fields = {conflict.field_name for conflict in result.collision.report.conflicts}

        assert "road_names" in conflict_fields
        assert "traffic_control" not in conflict_fields

    def test_non_intersection_does_not_match_intersection_substring(self) -> None:
        records = _group({"accloc": "Non Intersection"}, {"accloc": "Non Intersection"})

        result = TorontoKsiGroupedMapper().map_group(records, _snapshot())

        assert result.collision.record.intersection_related.value is False

    def test_unknown_vehicle_type_does_not_imply_driver_role(self) -> None:
        records = _group({"vehtype": "Truck"})

        result = TorontoKsiGroupedMapper().map_group(records, _snapshot())

        assert result.vehicles[0].record.category.value is VehicleCategory.TRUCK
        assert result.vehicles[0].record.road_user_role.value is RoadUserRole.UNKNOWN

    def test_hard_accdate_conflict_raises_mapping_error(self) -> None:
        records = _group({}, {"accdate": "2024-06-02T00:00:00"})

        with pytest.raises(MappingError) as excinfo:
            TorontoKsiGroupedMapper().map_group(records, _snapshot())

        assert excinfo.value.source_fields == ("accdate",)

    def test_hard_coordinate_conflict_raises_mapping_error(self) -> None:
        records = _group({}, {"latitude": "43.9999"})

        with pytest.raises(MappingError) as excinfo:
            TorontoKsiGroupedMapper().map_group(records, _snapshot())

        assert excinfo.value.source_fields == ("latitude",)


class TestDriftAndExport:
    def test_fixture_raw_records_match_schema_and_taxonomies(self) -> None:
        records = _group()
        snapshot = _snapshot()
        schema_observer = SchemaObserver(spec=TORONTO_KSI_SCHEMA)
        taxonomy_observer = TaxonomyObserver(specs=TORONTO_KSI_TAXONOMIES)

        for record in records:
            schema_observer.observe(record)
            taxonomy_observer.observe(record)

        schema_report = schema_observer.finalize(snapshot)
        taxonomy_report = taxonomy_observer.finalize(snapshot)

        assert schema_report.findings == ()
        assert taxonomy_report.findings == ()

    def test_unknown_taxonomy_value_surfaces_as_error(self) -> None:
        records = _group({}, {"road_user": "Scooter Rider"})
        snapshot = _snapshot()
        taxonomy_observer = TaxonomyObserver(specs=TORONTO_KSI_TAXONOMIES)

        for record in records:
            taxonomy_observer.observe(record)

        report = taxonomy_observer.finalize(snapshot)

        assert any(
            finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE for finding in report.findings
        )

        assert report.has_errors

    def test_unknown_accident_location_surfaces_as_taxonomy_drift(self) -> None:
        records = _group({"accloc": "Underpass"}, {"accloc": "Underpass"})
        snapshot = _snapshot()
        taxonomy_observer = TaxonomyObserver(specs=TORONTO_KSI_TAXONOMIES)

        for record in records:
            taxonomy_observer.observe(record)

        report = taxonomy_observer.finalize(snapshot)

        assert any(
            finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE
            and finding.taxonomy_id == "toronto-ksi-accloc"
            for finding in report.findings
        )

        assert report.has_errors

    async def test_grouped_collision_export_preserves_traceable_records(
        self, tmp_path: Path
    ) -> None:
        records = _group()
        snapshot = _snapshot()
        result = TorontoKsiGroupedMapper().map_group(records, snapshot)

        async def _records() -> AsyncIterable[PipelineRecord[TrafficCollision]]:
            yield PipelineRecord(raw=records[0], mapped=result.collision)

        manifest = await write_snapshot(
            PipelineResult(snapshot=snapshot, records=_records()),
            output_dir=tmp_path,
            record_type=TrafficCollision,
        )

        assert manifest.record_count == 1
        assert manifest.mapping_summary.conflicts_total >= 1
        assert result.collision.record.provenance.source_record_id == "900001"
        assert result.vehicles[0].record.provenance.source_record_id == "900001:1"
        assert result.people[0].record.provenance.source_record_id == "900001:1"
        assert result.people[1].record.provenance.source_record_id == "900001:2"
        assert result.people[1].record.vehicle_id is None
        assert result.people[1].record.position_in_vehicle.quality is FieldQuality.UNMAPPED
