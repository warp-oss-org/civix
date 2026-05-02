"""Tests for the NYC person source slice."""

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
from civix.core.quality.models.fields import FieldQuality
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.domains.transportation_safety.adapters.sources.us.nyc_persons import (
    DEFAULT_BASE_URL,
    NYC_JURISDICTION,
    NYC_PERSONS_DATASET_CONFIG,
    NYC_PERSONS_DATASET_ID,
    NYC_PERSONS_RELEASE_CAVEATS,
    NYC_PERSONS_SCHEMA,
    NYC_PERSONS_SOURCE_SCOPE,
    NYC_PERSONS_TAXONOMIES,
    SOCRATA_ORDER,
    SOURCE_ID,
    NycPersonsMapper,
)
from civix.domains.transportation_safety.models.parties import RoadUserRole
from civix.domains.transportation_safety.models.person import CollisionPerson, InjuryOutcome
from civix.infra.exporters.json import write_snapshot
from civix.infra.sources.socrata import SocrataFetchConfig, SocrataSourceAdapter

PINNED_NOW = datetime(2026, 5, 2, 12, 0, tzinfo=UTC)
RESOURCE_URL = f"{DEFAULT_BASE_URL}{NYC_PERSONS_DATASET_ID}.json"
FIXTURES = Path(__file__).parent / "fixtures"


def _adapter(
    client: httpx.AsyncClient,
    *,
    page_size: int = 1000,
    app_token: str | None = None,
) -> SocrataSourceAdapter:
    return SocrataSourceAdapter(
        dataset=NYC_PERSONS_DATASET_CONFIG,
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
        snapshot_id=SnapshotId("snap-nyc-persons"),
        source_id=SOURCE_ID,
        dataset_id=NYC_PERSONS_DATASET_ID,
        jurisdiction=NYC_JURISDICTION,
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
        source_record_id="person-row-001",
    )

    return NycPersonsMapper()(record, snap).record


def _capture_sequence(
    requests: list[httpx.Request],
    responses: list[httpx.Response],
) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)

        return responses.pop(0)

    return handler


class TestAdapter:
    async def test_fetches_ordered_pages_and_preserves_person_row_ids(self) -> None:
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
                result = await _adapter(client, app_token="token").fetch()
                records = [r async for r in result.records]

        assert result.snapshot.fetch_params == {"$order": SOCRATA_ORDER}
        assert [r.source_record_id for r in records] == ["person-row-001", "person-row-002"]
        assert requests[1].headers["X-App-Token"] == "token"
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

        assert person.collision_id == "4890001"
        assert person.person_id == "per-001"
        assert person.vehicle_id == "veh-001"
        assert person.role.value is RoadUserRole.DRIVER
        assert person.injury_outcome.value is InjuryOutcome.UNKNOWN
        assert person.injury_outcome.quality is FieldQuality.INFERRED
        assert person.age.value == 34
        assert person.safety_equipment.value is not None
        assert person.contributing_factors.value is not None
        assert [f.rank for f in person.contributing_factors.value] == [1, 2]

    def test_maps_bicyclist_without_vehicle_id(self) -> None:
        person = _map(
            person_type="Bicyclist",
            person_injury="Injured",
            vehicle_id=None,
            ped_role=None,
        )

        assert person.vehicle_id is None
        assert person.role.value is RoadUserRole.CYCLIST
        assert person.injury_outcome.value is InjuryOutcome.POSSIBLE

    def test_missing_person_id_raises_mapping_error(self) -> None:
        snap = _snapshot()
        record = RawRecord(
            snapshot_id=snap.snapshot_id,
            raw_data=_raw(person_id=None),
            source_record_id=None,
        )

        with pytest.raises(MappingError) as excinfo:
            NycPersonsMapper()(record, snap)

        assert excinfo.value.source_record_id is None
        assert excinfo.value.source_fields == ("person_id",)

    def test_demographic_fields_remain_unmapped(self) -> None:
        snap = _snapshot()
        record = RawRecord(
            snapshot_id=snap.snapshot_id,
            raw_data=_raw(person_sex="M"),
            source_record_id="person-row-001",
        )
        result = NycPersonsMapper()(record, snap)

        assert "person_sex" in result.report.unmapped_source_fields


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
                pipeline_result = await run(_adapter(client), NycPersonsMapper())
                schema_obs = SchemaObserver(spec=NYC_PERSONS_SCHEMA)
                taxonomy_obs = TaxonomyObserver(specs=NYC_PERSONS_TAXONOMIES)
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
        page[0]["person_type"] = "Skateboarder"

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[httpx.Response(200, json=count), httpx.Response(200, json=page)]
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(_adapter(client), NycPersonsMapper())
                taxonomy_obs = TaxonomyObserver(specs=NYC_PERSONS_TAXONOMIES)
                observed = attach_observers(pipeline_result, [taxonomy_obs])

                async for _ in observed.records:
                    pass

        report = taxonomy_obs.finalize(pipeline_result.snapshot)

        assert any(f.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE for f in report.findings)
        assert report.has_errors


def test_source_metadata_preserves_scope_and_caveats() -> None:
    assert SOURCE_ID == "nyc-open-data"
    assert NYC_PERSONS_DATASET_ID == "f55k-p6yu"
    assert "Person-level records" in NYC_PERSONS_SOURCE_SCOPE
    assert any("preliminary" in caveat for caveat in NYC_PERSONS_RELEASE_CAVEATS)
