"""Tests for the NYC traffic speeds source slice."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from civix.core.drift import SchemaObserver, TaxonomyDriftKind, TaxonomyObserver
from civix.core.identity.models.identifiers import SnapshotId
from civix.core.mapping.errors import MappingError
from civix.core.pipeline import attach_observers, run
from civix.core.ports.models.adapter import SourceAdapter
from civix.core.quality.models.fields import FieldQuality
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.domains.mobility_observations.adapters.sources.us.nyc_traffic_speeds import (
    DEFAULT_BASE_URL,
    NYC_JURISDICTION,
    NYC_TRAFFIC_SPEEDS_DATASET_ID,
    NYC_TRAFFIC_SPEEDS_RELEASE_CAVEATS,
    NYC_TRAFFIC_SPEEDS_SCHEMA,
    NYC_TRAFFIC_SPEEDS_SOURCE_SCOPE,
    NYC_TRAFFIC_SPEEDS_TAXONOMIES,
    SOCRATA_ORDER,
    SOURCE_ID,
    NycTrafficSpeedsAdapter,
    NycTrafficSpeedsMapper,
)
from civix.domains.mobility_observations.models.common import (
    SpeedMetricType,
    SpeedUnit,
    TravelMode,
)
from civix.infra.sources.socrata import SocrataFetchConfig

PINNED_NOW = datetime(2026, 5, 2, 12, 0, tzinfo=UTC)
RESOURCE_URL = f"{DEFAULT_BASE_URL}{NYC_TRAFFIC_SPEEDS_DATASET_ID}.json"
FIXTURES = Path(__file__).parent / "fixtures"


def _adapter(client: httpx.AsyncClient, *, page_size: int = 1000) -> NycTrafficSpeedsAdapter:
    return NycTrafficSpeedsAdapter(
        fetch_config=SocrataFetchConfig(
            client=client,
            clock=lambda: PINNED_NOW,
            page_size=page_size,
            order=SOCRATA_ORDER,
        ),
    )


def _snapshot() -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId("snap-speeds"),
        source_id=SOURCE_ID,
        dataset_id=NYC_TRAFFIC_SPEEDS_DATASET_ID,
        jurisdiction=NYC_JURISDICTION,
        fetched_at=PINNED_NOW,
        record_count=1,
    )


def _raw(**overrides: Any) -> dict[str, Any]:
    raw = json.loads((FIXTURES / "records_page.json").read_text())[0]
    raw.update(overrides)

    return raw


def _record(**overrides: Any) -> RawRecord:
    snap = _snapshot()

    return RawRecord(
        snapshot_id=snap.snapshot_id,
        raw_data=_raw(**overrides),
        source_record_id="4616334",
    )


def _capture_sequence(
    requests: list[httpx.Request],
    responses: list[httpx.Response],
) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)

        return responses.pop(0)

    return handler


class TestAdapter:
    async def test_fetches_records_and_preserves_link_ids(self) -> None:
        count = json.loads((FIXTURES / "count_response.json").read_text())
        rows = json.loads((FIXTURES / "records_page.json").read_text())
        rows[0][":@computed_region_test"] = "drop"
        requests: list[httpx.Request] = []

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=_capture_sequence(
                    requests,
                    [httpx.Response(200, json=count), httpx.Response(200, json=rows)],
                )
            )

            async with httpx.AsyncClient() as client:
                adapter = _adapter(client)
                result = await adapter.fetch()
                records = [record async for record in result.records]

        assert isinstance(adapter, SourceAdapter)
        assert result.snapshot.dataset_id == NYC_TRAFFIC_SPEEDS_DATASET_ID
        assert [record.source_record_id for record in records] == ["4616334", "4616335"]
        assert requests[1].url.params["$order"] == SOCRATA_ORDER
        assert all(not key.startswith(":@computed_region_") for key in records[0].raw_data)


class TestMapper:
    def test_maps_speed_and_travel_time_metrics(self) -> None:
        result = NycTrafficSpeedsMapper()(_record(), _snapshot())
        observation = result.record

        assert observation.observation_id == "4616334:2026-04-03T09:45:00"
        assert observation.site_id == "4616334"
        assert observation.travel_mode.value is TravelMode.MIXED_TRAFFIC
        assert observation.direction.quality is FieldQuality.NOT_PROVIDED
        assert observation.movement_type.quality is FieldQuality.UNMAPPED
        assert [metric.metric_type.value for metric in observation.metrics] == [
            SpeedMetricType.OBSERVED_SPEED,
            SpeedMetricType.TRAVEL_TIME,
        ]
        assert [metric.unit.value for metric in observation.metrics] == [
            SpeedUnit.MILES_PER_HOUR,
            SpeedUnit.SECONDS,
        ]
        assert [metric.value.value for metric in observation.metrics] == [
            Decimal("22.4"),
            Decimal("140"),
        ]
        assert "STATUS" in result.report.unmapped_source_fields
        assert "LINK_POINTS" in result.report.unmapped_source_fields
        assert "LINK_NAME" in result.report.unmapped_source_fields

    def test_negative_travel_time_fails_loudly(self) -> None:
        with pytest.raises(MappingError, match="negative numeric"):
            NycTrafficSpeedsMapper()(_record(TRAVEL_TIME="-1"), _snapshot())


class TestDrift:
    async def test_fixture_drift_clean(self) -> None:
        count = json.loads((FIXTURES / "count_response.json").read_text())
        page = json.loads((FIXTURES / "records_page.json").read_text())

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[httpx.Response(200, json=count), httpx.Response(200, json=page)]
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(_adapter(client), NycTrafficSpeedsMapper())
                schema_obs = SchemaObserver(spec=NYC_TRAFFIC_SPEEDS_SCHEMA)
                taxonomy_obs = TaxonomyObserver(specs=NYC_TRAFFIC_SPEEDS_TAXONOMIES)
                observed = attach_observers(pipeline_result, [schema_obs, taxonomy_obs])
                records = [record async for record in observed.records]

        assert len(records) == 2
        assert schema_obs.finalize(pipeline_result.snapshot).findings == ()
        assert taxonomy_obs.finalize(pipeline_result.snapshot).findings == ()

    async def test_unknown_status_surfaces_as_taxonomy_drift(self) -> None:
        count = json.loads((FIXTURES / "count_response.json").read_text())
        page = json.loads((FIXTURES / "records_page.json").read_text())
        page[0]["STATUS"] = "99"

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[httpx.Response(200, json=count), httpx.Response(200, json=page)]
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(_adapter(client), NycTrafficSpeedsMapper())
                taxonomy_obs = TaxonomyObserver(specs=NYC_TRAFFIC_SPEEDS_TAXONOMIES)
                observed = attach_observers(pipeline_result, [taxonomy_obs])
                async for _ in observed.records:
                    pass

        report = taxonomy_obs.finalize(pipeline_result.snapshot)

        assert any(
            finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE for finding in report.findings
        )


def test_source_metadata_preserves_scope_and_caveats() -> None:
    assert SOURCE_ID == "nyc-open-data"
    assert NYC_TRAFFIC_SPEEDS_DATASET_ID == "i4gi-tjb9"
    assert "speed" in NYC_TRAFFIC_SPEEDS_SOURCE_SCOPE.casefold()
    assert any("TRAVEL_TIME" in caveat for caveat in NYC_TRAFFIC_SPEEDS_RELEASE_CAVEATS)
