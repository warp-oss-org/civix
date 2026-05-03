"""Tests for the Environment Agency RoFRS source slice."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import respx

from civix.core.drift import SchemaObserver, TaxonomyObserver
from civix.core.identity.models.identifiers import SnapshotId
from civix.core.pipeline import attach_observers, run
from civix.core.ports.models.adapter import SourceAdapter
from civix.core.quality.models.fields import FieldQuality
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.spatial.models.geometry import GeometryType
from civix.domains.hazard_risk.adapters.sources.gb import ea_rofrs as rofrs
from civix.domains.hazard_risk.models import (
    HazardRiskHazardType,
    HazardRiskZoneStatus,
    build_hazard_risk_zone_key,
)

PINNED_NOW = datetime(2026, 5, 3, 12, 0, tzinfo=UTC)
FIXTURES = Path(__file__).parent / "fixtures"
JSON_FIXTURE = FIXTURES / "rofrs_records.json"


def _json_bytes() -> bytes:
    return JSON_FIXTURE.read_bytes()


def _payload() -> dict[str, Any]:
    return json.loads(JSON_FIXTURE.read_text())


def _row() -> dict[str, Any]:
    return dict(_payload()["records"][0])


def _record(raw: dict[str, Any] | None = None) -> RawRecord:
    payload = _row() if raw is None else raw

    return RawRecord(
        snapshot_id=SnapshotId("snap-ea-rofrs"),
        raw_data=payload,
        source_record_id="rofrs-eng-0001",
    )


def _snapshot() -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId("snap-ea-rofrs"),
        source_id=rofrs.SOURCE_ID,
        dataset_id=rofrs.EA_ROFRS_DATASET_ID,
        jurisdiction=rofrs.GB_ENGLAND_JURISDICTION,
        fetched_at=PINNED_NOW,
        record_count=1,
        source_url=rofrs.EA_ROFRS_DATASET_PAGE_URL,
    )


def _adapter(client: httpx.AsyncClient) -> rofrs.EaRofrsAdapter:
    return rofrs.EaRofrsAdapter(
        fetch_config=rofrs.EaRofrsFetchConfig(client=client, clock=lambda: PINNED_NOW)
    )


class TestAdapter:
    async def test_fetches_traceable_json_rows(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(rofrs.EA_ROFRS_DATASET_PAGE_URL).mock(
                return_value=httpx.Response(200, content=_json_bytes())
            )

            async with httpx.AsyncClient() as client:
                adapter = _adapter(client)
                result = await adapter.fetch()
                records = [record async for record in result.records]

        assert isinstance(adapter, SourceAdapter)
        assert result.snapshot.source_id == rofrs.SOURCE_ID
        assert result.snapshot.dataset_id == rofrs.EA_ROFRS_DATASET_ID
        assert result.snapshot.jurisdiction == rofrs.GB_ENGLAND_JURISDICTION
        assert result.snapshot.content_hash == hashlib.sha256(_json_bytes()).hexdigest()
        source_record_id = records[0].source_record_id
        assert source_record_id is not None
        assert source_record_id.startswith("rofrs-eng-0001:sha256-")


class TestZoneMapper:
    def test_maps_rofrs_band_to_flood_zone(self) -> None:
        result = rofrs.EaRofrsZoneMapper()(_record(), _snapshot())
        zone = result.record

        assert zone.zone_key == build_hazard_risk_zone_key(
            rofrs.SOURCE_ID,
            rofrs.EA_ROFRS_DATASET_ID,
            "rofrs-eng-0001",
        )
        assert zone.hazard_type.value is HazardRiskHazardType.FLOOD
        assert zone.source_hazard.value is not None
        assert zone.source_hazard.value.code == "rivers-and-sea"
        assert zone.source_zone.value is not None
        assert zone.source_zone.value.code == "high"
        assert zone.status.value is HazardRiskZoneStatus.EFFECTIVE
        assert zone.geometry_ref.value is not None
        assert zone.geometry_ref.value.geometry_type is GeometryType.POLYGON
        assert zone.source_caveats.quality is FieldQuality.STANDARDIZED


class TestDrift:
    async def test_fixture_drift_clean(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(rofrs.EA_ROFRS_DATASET_PAGE_URL).mock(
                return_value=httpx.Response(200, content=_json_bytes())
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(_adapter(client), rofrs.EaRofrsZoneMapper())
                schema_obs = SchemaObserver(spec=rofrs.EA_ROFRS_SCHEMA)
                taxonomy_obs = TaxonomyObserver(specs=rofrs.EA_ROFRS_TAXONOMIES)
                observed = attach_observers(pipeline_result, [schema_obs, taxonomy_obs])
                records = [record async for record in observed.records]

        assert len(records) == 1
        assert schema_obs.finalize(pipeline_result.snapshot).findings == ()
        assert taxonomy_obs.finalize(pipeline_result.snapshot).findings == ()


def test_source_metadata_preserves_rofrs_scope() -> None:
    assert rofrs.SOURCE_ID == "environment-agency"
    assert rofrs.EA_ROFRS_DATASET_ID == "risk_of_flooding_from_rivers_and_sea"
    assert rofrs.GB_ENGLAND_JURISDICTION.region == "England"
