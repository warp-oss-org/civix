"""Tests for the Chicago crash source slice."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from civix.core.drift import SchemaDriftKind, SchemaObserver, TaxonomyDriftKind, TaxonomyObserver
from civix.core.identity.models.identifiers import SnapshotId
from civix.core.mapping.errors import MappingError
from civix.core.pipeline import attach_observers, run
from civix.core.ports.errors import FetchError
from civix.core.ports.models.adapter import SourceAdapter
from civix.core.quality.models.fields import FieldQuality
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.domains.transportation_safety.adapters.sources.us.chicago_crashes import (
    CHICAGO_CRASHES_DATASET_ID,
    CHICAGO_CRASHES_SCHEMA,
    CHICAGO_CRASHES_TAXONOMIES,
    CHICAGO_JURISDICTION,
    DEFAULT_BASE_URL,
    SOCRATA_ORDER,
    SOURCE_ID,
    ChicagoCrashesAdapter,
    ChicagoCrashesMapper,
)
from civix.domains.transportation_safety.models.collision import (
    CollisionSeverity,
    TrafficCollision,
)
from civix.domains.transportation_safety.models.time import OccurrenceTimezoneStatus
from civix.infra.exporters.json import write_snapshot
from civix.infra.sources.socrata import SocrataFetchConfig

PINNED_NOW = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
RESOURCE_URL = f"{DEFAULT_BASE_URL}{CHICAGO_CRASHES_DATASET_ID}.json"
FIXTURES = Path(__file__).parent / "fixtures"


def _adapter(
    client: httpx.AsyncClient,
    *,
    page_size: int = 1000,
    app_token: str | None = None,
) -> ChicagoCrashesAdapter:
    return ChicagoCrashesAdapter(
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
        snapshot_id=SnapshotId("snap-chicago-crashes"),
        source_id=SOURCE_ID,
        dataset_id=CHICAGO_CRASHES_DATASET_ID,
        jurisdiction=CHICAGO_JURISDICTION,
        fetched_at=PINNED_NOW,
        record_count=1,
    )


def _raw(**overrides: Any) -> dict[str, Any]:
    base = json.loads((FIXTURES / "records_page.json").read_text())[0]
    base.update(overrides)

    return base


def _map(**overrides: Any) -> TrafficCollision:
    snap = _snapshot()
    record = RawRecord(
        snapshot_id=snap.snapshot_id,
        raw_data=_raw(**overrides),
        source_record_id="crash-001",
    )

    return ChicagoCrashesMapper()(record, snap).record


def _capture_sequence(
    requests: list[httpx.Request],
    responses: list[httpx.Response],
) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)

        return responses.pop(0)

    return handler


class TestAdapter:
    async def test_fetches_ordered_pages_and_preserves_source_ids(self) -> None:
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
        assert result.snapshot.source_id == SOURCE_ID
        assert result.snapshot.fetch_params == {"$order": SOCRATA_ORDER}
        assert [r.source_record_id for r in records] == ["crash-001", "crash-002"]
        assert "$order" not in requests[0].url.params
        assert requests[1].url.params["$order"] == SOCRATA_ORDER
        assert requests[1].headers["X-App-Token"] == "token"
        assert all(not key.startswith(":@computed_region_") for key in records[0].raw_data)

    async def test_page_size_must_be_positive(self) -> None:
        async with httpx.AsyncClient() as client:
            with pytest.raises(ValueError, match="page_size"):
                _adapter(client, page_size=0)

    async def test_non_list_records_raise_fetch_error(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[
                    httpx.Response(200, json=[{"count": "1"}]),
                    httpx.Response(200, json={"records": []}),
                ]
            )

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="non-list JSON body"):
                    await _adapter(client).fetch()


class TestMapper:
    def test_maps_full_crash_row(self) -> None:
        collision = _map()

        assert collision.collision_id == "crash-001"
        assert collision.occurred_at.value is not None
        assert collision.occurred_at.value.timezone_status is OccurrenceTimezoneStatus.NAMED_LOCAL
        assert collision.occurred_at.value.timezone == "America/Chicago"
        assert collision.severity.value is CollisionSeverity.SERIOUS_INJURY
        assert collision.person_count.quality is FieldQuality.UNMAPPED
        assert collision.vehicle_count.value == 2
        assert collision.locality.value == "Chicago"
        assert collision.locality.quality is FieldQuality.INFERRED
        assert collision.address.value is not None
        assert collision.address.value.country == "US"
        assert collision.address.value.street == "1 N STATE ST"
        assert collision.address.value.region is None
        assert collision.address.value.locality is None
        assert collision.address.quality is FieldQuality.DERIVED
        assert collision.address.source_fields == (
            "street_no",
            "street_direction",
            "street_name",
        )

        assert collision.coordinate.value is not None
        assert collision.coordinate.value.latitude == 41.8837
        assert collision.contributing_factors.value is not None
        assert [f.rank for f in collision.contributing_factors.value] == [1, 2]

    def test_missing_crash_record_id_raises_mapping_error(self) -> None:
        snap = _snapshot()
        record = RawRecord(
            snapshot_id=snap.snapshot_id,
            raw_data=_raw(crash_record_id=None),
            source_record_id=None,
        )

        with pytest.raises(MappingError) as excinfo:
            ChicagoCrashesMapper()(record, snap)

        assert excinfo.value.source_record_id is None
        assert excinfo.value.source_fields == ("crash_record_id",)

    def test_unknown_severity_is_inferred_unknown(self) -> None:
        collision = _map(most_severe_injury="MYSTERY")

        assert collision.severity.value is CollisionSeverity.UNKNOWN
        assert collision.severity.quality is FieldQuality.INFERRED

    def test_bad_coordinate_is_not_provided(self) -> None:
        collision = _map(latitude="999")

        assert collision.coordinate.value is None
        assert collision.coordinate.quality is FieldQuality.NOT_PROVIDED

    def test_unmapped_report_preserves_unconsumed_raw_fields(self) -> None:
        snap = _snapshot()
        record = RawRecord(
            snapshot_id=snap.snapshot_id,
            raw_data=_raw(extra_source_field="kept raw"),
            source_record_id="crash-001",
        )
        result = ChicagoCrashesMapper()(record, snap)

        assert "extra_source_field" in result.report.unmapped_source_fields


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
                pipeline_result = await run(_adapter(client), ChicagoCrashesMapper())
                schema_obs = SchemaObserver(spec=CHICAGO_CRASHES_SCHEMA)
                taxonomy_obs = TaxonomyObserver(specs=CHICAGO_CRASHES_TAXONOMIES)
                observed = attach_observers(pipeline_result, [schema_obs, taxonomy_obs])
                manifest = await write_snapshot(
                    observed,
                    output_dir=tmp_path,
                    record_type=TrafficCollision,
                )

        schema_report = schema_obs.finalize(pipeline_result.snapshot)
        taxonomy_report = taxonomy_obs.finalize(pipeline_result.snapshot)

        assert manifest.record_count == 2
        assert schema_report.findings == ()
        assert taxonomy_report.findings == ()

    async def test_unknown_taxonomy_value_surfaces_as_error(self) -> None:
        count = json.loads((FIXTURES / "count_response.json").read_text())
        page = json.loads((FIXTURES / "records_page.json").read_text())
        page[0]["weather_condition"] = "MYSTERY WEATHER"

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[httpx.Response(200, json=count), httpx.Response(200, json=page)]
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(_adapter(client), ChicagoCrashesMapper())
                taxonomy_obs = TaxonomyObserver(specs=CHICAGO_CRASHES_TAXONOMIES)
                observed = attach_observers(pipeline_result, [taxonomy_obs])

                async for _ in observed.records:
                    pass

        report = taxonomy_obs.finalize(pipeline_result.snapshot)

        assert any(f.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE for f in report.findings)
        assert report.has_errors

    async def test_unexpected_source_field_surfaces_as_schema_warning(self) -> None:
        count = json.loads((FIXTURES / "count_response.json").read_text())
        page = json.loads((FIXTURES / "records_page.json").read_text())
        page[0]["new_column"] = "new"

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[httpx.Response(200, json=count), httpx.Response(200, json=page)]
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(_adapter(client), ChicagoCrashesMapper())
                schema_obs = SchemaObserver(spec=CHICAGO_CRASHES_SCHEMA)
                observed = attach_observers(pipeline_result, [schema_obs])

                async for _ in observed.records:
                    pass

        report = schema_obs.finalize(pipeline_result.snapshot)

        assert any(f.kind is SchemaDriftKind.UNEXPECTED_FIELD for f in report.findings)

    async def test_pipeline_fail_fast_mapping_error_carries_row_metadata(self) -> None:
        rows = json.loads((FIXTURES / "records_page.json").read_text())
        rows[1]["crash_record_id"] = None

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[
                    httpx.Response(200, json=[{"count": "2"}]),
                    httpx.Response(200, json=rows),
                ]
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(_adapter(client), ChicagoCrashesMapper())
                iterator = aiter(pipeline_result.records)

                first = await anext(iterator)

                with pytest.raises(MappingError) as excinfo:
                    await anext(iterator)

        assert first.mapped.record.collision_id == "crash-001"
        assert excinfo.value.source_record_id is None
        assert excinfo.value.source_fields == ("crash_record_id",)
