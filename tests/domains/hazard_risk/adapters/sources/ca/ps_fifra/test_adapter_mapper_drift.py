"""Tests for the Public Safety Canada FIFRA source slice."""

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
from civix.domains.hazard_risk.adapters.sources.ca import ps_fifra as fifra
from civix.domains.hazard_risk.models import (
    HazardRiskHazardType,
    HazardRiskZoneStatus,
    build_hazard_risk_zone_key,
)

PINNED_NOW = datetime(2026, 5, 3, 12, 0, tzinfo=UTC)
FIXTURES = Path(__file__).parent / "fixtures"
JSON_FIXTURE = FIXTURES / "fifra_records.json"


def _json_bytes() -> bytes:
    return JSON_FIXTURE.read_bytes()


def _payload() -> dict[str, Any]:
    return json.loads(JSON_FIXTURE.read_text())


def _row() -> dict[str, Any]:
    return dict(_payload()["records"][0])


def _record(raw: dict[str, Any] | None = None) -> RawRecord:
    payload = _row() if raw is None else raw

    return RawRecord(
        snapshot_id=SnapshotId("snap-ps-fifra"),
        raw_data=payload,
        source_record_id="fifra-on-0001",
    )


def _snapshot() -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId("snap-ps-fifra"),
        source_id=fifra.SOURCE_ID,
        dataset_id=fifra.PS_FIFRA_DATASET_ID,
        jurisdiction=fifra.CA_JURISDICTION,
        fetched_at=PINNED_NOW,
        record_count=1,
        source_url=fifra.PS_FIFRA_DATASET_PAGE_URL,
    )


def _adapter(client: httpx.AsyncClient) -> fifra.PsFifraAdapter:
    return fifra.PsFifraAdapter(
        fetch_config=fifra.PsFifraFetchConfig(
            client=client,
            source_url=fifra.PS_FIFRA_DATASET_PAGE_URL,
            clock=lambda: PINNED_NOW,
        )
    )


class TestAdapter:
    async def test_fetches_traceable_json_rows(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(fifra.PS_FIFRA_DATASET_PAGE_URL).mock(
                return_value=httpx.Response(200, content=_json_bytes())
            )

            async with httpx.AsyncClient() as client:
                adapter = _adapter(client)
                result = await adapter.fetch()
                records = [record async for record in result.records]

        assert isinstance(adapter, SourceAdapter)
        assert result.snapshot.source_id == fifra.SOURCE_ID
        assert result.snapshot.dataset_id == fifra.PS_FIFRA_DATASET_ID
        assert result.snapshot.content_hash == hashlib.sha256(_json_bytes()).hexdigest()
        source_record_id = records[0].source_record_id
        assert source_record_id is not None
        assert source_record_id.startswith("fifra-on-0001:sha256-")


class TestZoneMapper:
    def test_maps_fifra_area_to_flood_zone_with_source_mechanism(self) -> None:
        result = fifra.PsFifraZoneMapper()(_record(), _snapshot())
        zone = result.record

        assert zone.zone_key == build_hazard_risk_zone_key(
            fifra.SOURCE_ID,
            fifra.PS_FIFRA_DATASET_ID,
            "fifra-on-0001",
        )
        assert zone.hazard_type.value is HazardRiskHazardType.FLOOD
        assert zone.source_hazard.value is not None
        assert zone.source_hazard.value.code == "riverine"
        assert zone.source_zone.value is not None
        assert zone.source_zone.value.code == "extreme"
        assert zone.status.value is HazardRiskZoneStatus.EFFECTIVE
        assert zone.effective_period.value is not None
        assert zone.geometry_ref.value is not None
        assert zone.geometry_ref.value.geometry_type is GeometryType.POLYGON
        assert zone.source_caveats.quality is FieldQuality.STANDARDIZED


class TestDrift:
    async def test_fixture_drift_clean(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(fifra.PS_FIFRA_DATASET_PAGE_URL).mock(
                return_value=httpx.Response(200, content=_json_bytes())
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(_adapter(client), fifra.PsFifraZoneMapper())
                schema_obs = SchemaObserver(spec=fifra.PS_FIFRA_SCHEMA)
                taxonomy_obs = TaxonomyObserver(specs=fifra.PS_FIFRA_TAXONOMIES)
                observed = attach_observers(pipeline_result, [schema_obs, taxonomy_obs])
                records = [record async for record in observed.records]

        assert len(records) == 1
        assert schema_obs.finalize(pipeline_result.snapshot).findings == ()
        assert taxonomy_obs.finalize(pipeline_result.snapshot).findings == ()


def test_source_metadata_preserves_fifra_scope() -> None:
    assert fifra.SOURCE_ID == "public-safety-canada"
    assert fifra.PS_FIFRA_DATASET_ID == "federally_identified_flood_risk_areas"
    assert "screening areas" in fifra.PS_FIFRA_SOURCE_SCOPE
