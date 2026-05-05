"""Tests for the NRCan Flood Susceptibility Index source slice."""

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
from civix.domains.hazard_risk.adapters.sources.ca import nrcan_fsi as fsi
from civix.domains.hazard_risk.models import (
    HazardRiskAreaKind,
    HazardRiskHazardType,
    HazardRiskScoreType,
    NumericScoreMeasure,
    build_hazard_risk_area_key,
)

PINNED_NOW = datetime(2026, 5, 3, 12, 0, tzinfo=UTC)
FIXTURES = Path(__file__).parent / "fixtures"
JSON_FIXTURE = FIXTURES / "fsi_records.json"


def _json_bytes() -> bytes:
    return JSON_FIXTURE.read_bytes()


def _payload() -> dict[str, Any]:
    return json.loads(JSON_FIXTURE.read_text())


def _row() -> dict[str, Any]:
    return dict(_payload()["records"][0])


def _record(raw: dict[str, Any] | None = None) -> RawRecord:
    payload = _row() if raw is None else raw

    return RawRecord(
        snapshot_id=SnapshotId("snap-nrcan-fsi"),
        raw_data=payload,
        source_record_id="fsi-cell-0001",
    )


def _snapshot() -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId("snap-nrcan-fsi"),
        source_id=fsi.SOURCE_ID,
        dataset_id=fsi.NRCAN_FSI_DATASET_ID,
        jurisdiction=fsi.CA_JURISDICTION,
        fetched_at=PINNED_NOW,
        record_count=1,
        source_url=fsi.NRCAN_FSI_DATASET_PAGE_URL,
    )


def _adapter(client: httpx.AsyncClient) -> fsi.NrcanFsiAdapter:
    return fsi.NrcanFsiAdapter(
        fetch_config=fsi.NrcanFsiFetchConfig(
            client=client,
            source_url=fsi.NRCAN_FSI_DATASET_PAGE_URL,
            clock=lambda: PINNED_NOW,
        )
    )


class TestAdapter:
    async def test_fetches_traceable_json_rows(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(fsi.NRCAN_FSI_DATASET_PAGE_URL).mock(
                return_value=httpx.Response(200, content=_json_bytes())
            )

            async with httpx.AsyncClient() as client:
                adapter = _adapter(client)
                result = await adapter.fetch()
                records = [record async for record in result.records]

        assert isinstance(adapter, SourceAdapter)
        assert result.snapshot.source_id == fsi.SOURCE_ID
        assert result.snapshot.dataset_id == fsi.NRCAN_FSI_DATASET_ID
        assert result.snapshot.jurisdiction == fsi.CA_JURISDICTION
        assert result.snapshot.content_hash == hashlib.sha256(_json_bytes()).hexdigest()
        assert result.snapshot.fetch_params == {
            "format": "json",
            "records_key": "records",
            "source_family": "nrcan-fsi",
        }
        source_record_id = records[0].source_record_id

        assert source_record_id is not None
        assert source_record_id.startswith("fsi-cell-0001:sha256-")


class TestMappers:
    def test_maps_flood_susceptibility_area_with_raster_geometry_ref(self) -> None:
        result = fsi.NrcanFsiAreaMapper()(_record(), _snapshot())
        area = result.record

        assert area.area_key == build_hazard_risk_area_key(
            fsi.SOURCE_ID,
            fsi.NRCAN_FSI_DATASET_ID,
            "fsi-cell-0001",
        )

        assert area.area_kind.value is HazardRiskAreaKind.RISK_INDEX_AREA
        assert area.jurisdiction.value is not None
        assert area.jurisdiction.value.country == "CA"
        assert area.jurisdiction.value.region == "ON"
        assert area.geometry_ref.value is not None
        assert area.geometry_ref.value.geometry_type is GeometryType.RASTER
        assert area.source_hazards.value is not None
        assert area.source_hazards.value[0].code == "flood-prone"

    def test_maps_rating_and_numeric_scores_with_methodology_context(self) -> None:
        result = fsi.NrcanFsiScoresMapper()(_record(), _snapshot())
        scores = result.record

        assert len(scores) == 2
        assert scores[0].hazard_type.value is HazardRiskHazardType.FLOOD
        assert scores[0].score_type.value is HazardRiskScoreType.RATING
        assert scores[0].methodology_version.value == "fsi-2025"
        assert scores[1].score_type.value is HazardRiskScoreType.PER_HAZARD_SCORE
        assert isinstance(scores[1].score_measure.value, NumericScoreMeasure)
        assert scores[1].score_scale.value is not None
        assert scores[1].score_scale.value.maximum == 1
        assert scores[1].source_caveats.quality is FieldQuality.STANDARDIZED


class TestDrift:
    async def test_fixture_drift_clean(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(fsi.NRCAN_FSI_DATASET_PAGE_URL).mock(
                return_value=httpx.Response(200, content=_json_bytes())
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(_adapter(client), fsi.NrcanFsiScoresMapper())
                schema_obs = SchemaObserver(spec=fsi.NRCAN_FSI_SCHEMA)
                taxonomy_obs = TaxonomyObserver(specs=fsi.NRCAN_FSI_TAXONOMIES)
                observed = attach_observers(pipeline_result, [schema_obs, taxonomy_obs])
                records = [record async for record in observed.records]

        assert len(records) == 1
        assert schema_obs.finalize(pipeline_result.snapshot).findings == ()
        assert taxonomy_obs.finalize(pipeline_result.snapshot).findings == ()


def test_source_metadata_preserves_fsi_scope() -> None:
    assert fsi.SOURCE_ID == "nrcan-open-maps"
    assert fsi.NRCAN_FSI_DATASET_ID == "flood_susceptibility_index"
    assert "screening layer" in fsi.NRCAN_FSI_SOURCE_SCOPE
