"""Tests for the Chicago people source slice."""

from __future__ import annotations

import json
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
from civix.core.quality.models.fields import FieldQuality
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.domains.transportation_safety.adapters.sources.us.chicago_people import (
    CHICAGO_JURISDICTION,
    CHICAGO_PEOPLE_DATASET_CONFIG,
    CHICAGO_PEOPLE_DATASET_ID,
    CHICAGO_PEOPLE_SCHEMA,
    CHICAGO_PEOPLE_TAXONOMIES,
    DEFAULT_BASE_URL,
    SOCRATA_ORDER,
    SOURCE_ID,
    ChicagoPeopleMapper,
)
from civix.domains.transportation_safety.models.parties import RoadUserRole
from civix.domains.transportation_safety.models.person import CollisionPerson, InjuryOutcome
from civix.infra.exporters.json import write_snapshot
from civix.infra.sources.socrata import SocrataFetchConfig, SocrataSourceAdapter

PINNED_NOW = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
RESOURCE_URL = f"{DEFAULT_BASE_URL}{CHICAGO_PEOPLE_DATASET_ID}.json"
FIXTURES = Path(__file__).parent / "fixtures"


def _adapter(client: httpx.AsyncClient, *, page_size: int = 1000) -> SocrataSourceAdapter:
    return SocrataSourceAdapter(
        dataset=CHICAGO_PEOPLE_DATASET_CONFIG,
        fetch_config=SocrataFetchConfig(
            client=client,
            clock=lambda: PINNED_NOW,
            page_size=page_size,
            order=SOCRATA_ORDER,
        ),
    )


def _snapshot() -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId("snap-chicago-people"),
        source_id=SOURCE_ID,
        dataset_id=CHICAGO_PEOPLE_DATASET_ID,
        jurisdiction=CHICAGO_JURISDICTION,
        fetched_at=PINNED_NOW,
        record_count=1,
    )


def _raw(**overrides: Any) -> dict[str, Any]:
    base = json.loads((FIXTURES / "records_page.json").read_text())[0]
    base.update(overrides)

    return base


def _map(**overrides: Any) -> CollisionPerson:
    snap = _snapshot()
    record = RawRecord(
        snapshot_id=snap.snapshot_id,
        raw_data=_raw(**overrides),
        source_record_id="person-001",
    )

    return ChicagoPeopleMapper()(record, snap).record


class TestAdapter:
    async def test_fetches_ordered_pages_and_preserves_person_ids(self) -> None:
        rows = json.loads((FIXTURES / "records_page.json").read_text())
        rows[0][":@computed_region_test"] = "transport"

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[
                    httpx.Response(200, json=[{"count": "2"}]),
                    httpx.Response(200, json=rows),
                ]
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                records = [r async for r in result.records]

        assert result.snapshot.fetch_params == {"$order": SOCRATA_ORDER}
        assert [r.source_record_id for r in records] == ["person-001", "person-002"]
        assert all(not key.startswith(":@computed_region_") for key in records[0].raw_data)

    async def test_non_json_count_raises_fetch_error(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(httpx.Response(200, text="not json"))

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="non-JSON response"):
                    await _adapter(client).fetch()


class TestMapper:
    def test_maps_driver_person_row(self) -> None:
        person = _map()

        assert person.collision_id == "crash-001"
        assert person.person_id == "person-001"
        assert person.vehicle_id == "veh-001"
        assert person.role.value is RoadUserRole.DRIVER
        assert person.injury_outcome.value is InjuryOutcome.UNINJURED
        assert person.age.value == 34
        assert person.safety_equipment.value is not None

    def test_maps_pedestrian_without_vehicle_id(self) -> None:
        person = _map(
            person_type="PEDESTRIAN",
            vehicle_id=None,
            injury_classification="INCAPACITATING INJURY",
        )

        assert person.vehicle_id is None
        assert person.role.value is RoadUserRole.PEDESTRIAN
        assert person.injury_outcome.value is InjuryOutcome.SERIOUS

    def test_missing_person_id_raises_mapping_error(self) -> None:
        snap = _snapshot()
        record = RawRecord(
            snapshot_id=snap.snapshot_id,
            raw_data=_raw(person_id=None),
            source_record_id=None,
        )

        with pytest.raises(MappingError) as excinfo:
            ChicagoPeopleMapper()(record, snap)

        assert excinfo.value.source_record_id is None
        assert excinfo.value.source_fields == ("person_id",)

    def test_missing_crash_record_id_raises_mapping_error(self) -> None:
        snap = _snapshot()
        record = RawRecord(
            snapshot_id=snap.snapshot_id,
            raw_data=_raw(crash_record_id=None),
            source_record_id="person-001",
        )

        with pytest.raises(MappingError) as excinfo:
            ChicagoPeopleMapper()(record, snap)

        assert excinfo.value.source_record_id == "person-001"
        assert excinfo.value.source_fields == ("crash_record_id",)

    def test_unknown_injury_is_inferred_unknown(self) -> None:
        person = _map(injury_classification="MYSTERY")

        assert person.injury_outcome.value is InjuryOutcome.UNKNOWN
        assert person.injury_outcome.quality is FieldQuality.INFERRED

    def test_demographic_fields_remain_unmapped(self) -> None:
        snap = _snapshot()
        record = RawRecord(
            snapshot_id=snap.snapshot_id,
            raw_data=_raw(sex="X"),
            source_record_id="person-001",
        )
        result = ChicagoPeopleMapper()(record, snap)

        assert "sex" in result.report.unmapped_source_fields


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
                pipeline_result = await run(_adapter(client), ChicagoPeopleMapper())
                schema_obs = SchemaObserver(spec=CHICAGO_PEOPLE_SCHEMA)
                taxonomy_obs = TaxonomyObserver(specs=CHICAGO_PEOPLE_TAXONOMIES)
                observed = attach_observers(pipeline_result, [schema_obs, taxonomy_obs])
                manifest = await write_snapshot(
                    observed,
                    output_dir=tmp_path,
                    record_type=CollisionPerson,
                )

        assert manifest.record_count == 2
        assert schema_obs.finalize(pipeline_result.snapshot).findings == ()
        assert taxonomy_obs.finalize(pipeline_result.snapshot).findings == ()

    async def test_unknown_taxonomy_value_surfaces_as_error(self) -> None:
        count = json.loads((FIXTURES / "count_response.json").read_text())
        page = json.loads((FIXTURES / "records_page.json").read_text())
        page[0]["person_type"] = "SKATEBOARDER"

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[httpx.Response(200, json=count), httpx.Response(200, json=page)]
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(_adapter(client), ChicagoPeopleMapper())
                taxonomy_obs = TaxonomyObserver(specs=CHICAGO_PEOPLE_TAXONOMIES)
                observed = attach_observers(pipeline_result, [taxonomy_obs])

                async for _ in observed.records:
                    pass

        report = taxonomy_obs.finalize(pipeline_result.snapshot)

        assert any(f.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE for f in report.findings)
        assert report.has_errors
