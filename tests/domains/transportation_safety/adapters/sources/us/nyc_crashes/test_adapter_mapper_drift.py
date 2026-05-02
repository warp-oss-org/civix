"""Tests for the NYC crash source slice."""

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
from civix.domains.transportation_safety.adapters.sources.us.nyc_crashes import (
    DEFAULT_BASE_URL,
    NYC_COLLISIONS_RELEASE_CAVEATS,
    NYC_COLLISIONS_SOURCE_SCOPE,
    NYC_CRASHES_DATASET_CONFIG,
    NYC_CRASHES_DATASET_ID,
    NYC_CRASHES_SCHEMA,
    NYC_CRASHES_TAXONOMIES,
    NYC_JURISDICTION,
    SOCRATA_ORDER,
    SOURCE_ID,
    NycCrashesMapper,
)
from civix.domains.transportation_safety.models.collision import (
    CollisionSeverity,
    TrafficCollision,
)
from civix.domains.transportation_safety.models.time import OccurrenceTimezoneStatus
from civix.infra.exporters.json import write_snapshot
from civix.infra.sources.socrata import SocrataFetchConfig, SocrataSourceAdapter

PINNED_NOW = datetime(2026, 5, 2, 12, 0, tzinfo=UTC)
RESOURCE_URL = f"{DEFAULT_BASE_URL}{NYC_CRASHES_DATASET_ID}.json"
FIXTURES = Path(__file__).parent / "fixtures"


def _adapter(
    client: httpx.AsyncClient,
    *,
    page_size: int = 1000,
    app_token: str | None = None,
) -> SocrataSourceAdapter:
    return SocrataSourceAdapter(
        dataset=NYC_CRASHES_DATASET_CONFIG,
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
        snapshot_id=SnapshotId("snap-nyc-crashes"),
        source_id=SOURCE_ID,
        dataset_id=NYC_CRASHES_DATASET_ID,
        jurisdiction=NYC_JURISDICTION,
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
        source_record_id="4890001",
    )

    return NycCrashesMapper()(record, snap).record


def _capture_sequence(
    requests: list[httpx.Request],
    responses: list[httpx.Response],
) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)

        return responses.pop(0)

    return handler


class TestAdapter:
    async def test_fetches_ordered_pages_and_preserves_collision_ids(self) -> None:
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

        assert result.snapshot.source_id == SOURCE_ID
        assert result.snapshot.dataset_id == NYC_CRASHES_DATASET_ID
        assert result.snapshot.fetch_params == {"$order": SOCRATA_ORDER}
        assert [r.source_record_id for r in records] == ["4890001", "4890002"]
        assert "$order" not in requests[0].url.params
        assert requests[1].url.params["$order"] == SOCRATA_ORDER
        assert requests[1].headers["X-App-Token"] == "token"
        assert all(not key.startswith(":@computed_region_") for key in records[0].raw_data)

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

        assert collision.collision_id == "4890001"
        assert collision.occurred_at.value is not None
        assert collision.occurred_at.value.timezone_status is OccurrenceTimezoneStatus.NAMED_LOCAL
        assert collision.occurred_at.value.timezone == "America/New_York"
        assert collision.severity.value is CollisionSeverity.POSSIBLE_INJURY
        assert collision.fatal_count.value == 0
        assert collision.total_injured_count.value == 1
        assert collision.possible_injury_count.quality is FieldQuality.UNMAPPED
        assert collision.vehicle_count.quality is FieldQuality.UNMAPPED
        assert collision.locality.value == "New York City"
        assert collision.address.value is not None
        assert collision.address.value.locality == "BROOKLYN"
        assert collision.address.value.street == "ADAMS STREET"
        assert collision.coordinate.value is not None
        assert collision.road_names.value == ("ADAMS STREET", "TILLARY STREET")
        assert collision.intersection_related.value is True
        assert collision.contributing_factors.value is not None
        assert [f.rank for f in collision.contributing_factors.value] == [1, 2, 3]

    def test_zero_zero_coordinate_is_not_provided(self) -> None:
        collision = _map(latitude="0", longitude="0")

        assert collision.coordinate.value is None
        assert collision.coordinate.quality is FieldQuality.NOT_PROVIDED

    def test_off_street_address_is_not_intersection_related(self) -> None:
        collision = _map(cross_street_name=None, off_street_name="123 ADAMS STREET")

        assert collision.address.value is not None
        assert collision.address.value.street == "123 ADAMS STREET"
        assert collision.intersection_related.value is False

    def test_partial_null_counts_produce_unknown_severity(self) -> None:
        collision = _map(number_of_persons_killed="0", number_of_persons_injured=None)

        assert collision.severity.value is CollisionSeverity.UNKNOWN

    def test_missing_collision_id_raises_mapping_error(self) -> None:
        snap = _snapshot()
        record = RawRecord(
            snapshot_id=snap.snapshot_id,
            raw_data=_raw(collision_id=None),
            source_record_id=None,
        )

        with pytest.raises(MappingError) as excinfo:
            NycCrashesMapper()(record, snap)

        assert excinfo.value.source_record_id is None
        assert excinfo.value.source_fields == ("collision_id",)

    def test_mode_counts_and_crash_vehicle_types_remain_unmapped(self) -> None:
        snap = _snapshot()
        record = RawRecord(
            snapshot_id=snap.snapshot_id,
            raw_data=_raw(),
            source_record_id="4890001",
        )
        result = NycCrashesMapper()(record, snap)

        assert "number_of_cyclist_injured" in result.report.unmapped_source_fields
        assert "vehicle_type_code1" in result.report.unmapped_source_fields


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
                pipeline_result = await run(_adapter(client), NycCrashesMapper())
                schema_obs = SchemaObserver(spec=NYC_CRASHES_SCHEMA)
                taxonomy_obs = TaxonomyObserver(specs=NYC_CRASHES_TAXONOMIES)
                observed = attach_observers(pipeline_result, [schema_obs, taxonomy_obs])
                manifest = await write_snapshot(
                    observed,
                    output_dir=tmp_path,
                    record_type=TrafficCollision,
                )

        assert manifest.record_count == 2
        assert schema_obs.finalize(pipeline_result.snapshot).findings == ()
        assert taxonomy_obs.finalize(pipeline_result.snapshot).findings == ()

    async def test_unknown_taxonomy_value_surfaces_as_error(self) -> None:
        count = json.loads((FIXTURES / "count_response.json").read_text())
        page = json.loads((FIXTURES / "records_page.json").read_text())
        page[0]["borough"] = "MYSTERY"

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[httpx.Response(200, json=count), httpx.Response(200, json=page)]
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(_adapter(client), NycCrashesMapper())
                taxonomy_obs = TaxonomyObserver(specs=NYC_CRASHES_TAXONOMIES)
                observed = attach_observers(pipeline_result, [taxonomy_obs])

                async for _ in observed.records:
                    pass

        report = taxonomy_obs.finalize(pipeline_result.snapshot)

        assert any(f.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE for f in report.findings)
        assert report.has_errors


def test_source_metadata_preserves_scope_and_caveats() -> None:
    assert SOURCE_ID == "nyc-open-data"
    assert NYC_CRASHES_DATASET_ID == "h9gi-nx95"
    assert NYC_JURISDICTION.locality == "New York City"
    assert "MV-104AN" in NYC_COLLISIONS_SOURCE_SCOPE
    assert any("$1000" in caveat for caveat in NYC_COLLISIONS_RELEASE_CAVEATS)
    assert any("observed for drift" in caveat for caveat in NYC_COLLISIONS_RELEASE_CAVEATS)
