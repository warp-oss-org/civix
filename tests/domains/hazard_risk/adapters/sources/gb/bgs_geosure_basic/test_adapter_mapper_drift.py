"""Tests for the BGS GeoSure Basic source slice."""

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
from civix.domains.hazard_risk.adapters.sources.gb import bgs_geosure_basic as geosure
from civix.domains.hazard_risk.models import (
    HazardRiskAreaKind,
    HazardRiskHazardType,
    HazardRiskScoreType,
    NumericScoreMeasure,
    build_hazard_risk_area_key,
)

PINNED_NOW = datetime(2026, 5, 3, 12, 0, tzinfo=UTC)
FIXTURES = Path(__file__).parent / "fixtures"
JSON_FIXTURE = FIXTURES / "geosure_basic_records.json"


def _json_bytes() -> bytes:
    return JSON_FIXTURE.read_bytes()


def _payload() -> dict[str, Any]:
    return json.loads(JSON_FIXTURE.read_text())


def _row() -> dict[str, Any]:
    return dict(_payload()["records"][0])


def _record(raw: dict[str, Any] | None = None) -> RawRecord:
    payload = _row() if raw is None else raw

    return RawRecord(
        snapshot_id=SnapshotId("snap-bgs-geosure"),
        raw_data=payload,
        source_record_id="geosure-hex-0001",
    )


def _snapshot() -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId("snap-bgs-geosure"),
        source_id=geosure.SOURCE_ID,
        dataset_id=geosure.BGS_GEOSURE_BASIC_DATASET_ID,
        jurisdiction=geosure.GB_JURISDICTION,
        fetched_at=PINNED_NOW,
        record_count=1,
        source_url=geosure.BGS_GEOSURE_BASIC_DATASET_PAGE_URL,
    )


def _adapter(client: httpx.AsyncClient) -> geosure.BgsGeosureBasicAdapter:
    return geosure.BgsGeosureBasicAdapter(
        fetch_config=geosure.BgsGeosureBasicFetchConfig(client=client, clock=lambda: PINNED_NOW)
    )


class TestAdapter:
    async def test_fetches_traceable_json_rows(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(geosure.BGS_GEOSURE_BASIC_DATASET_PAGE_URL).mock(
                return_value=httpx.Response(200, content=_json_bytes())
            )

            async with httpx.AsyncClient() as client:
                adapter = _adapter(client)
                result = await adapter.fetch()
                records = [record async for record in result.records]

        assert isinstance(adapter, SourceAdapter)
        assert result.snapshot.source_id == geosure.SOURCE_ID
        assert result.snapshot.dataset_id == geosure.BGS_GEOSURE_BASIC_DATASET_ID
        assert result.snapshot.content_hash == hashlib.sha256(_json_bytes()).hexdigest()
        source_record_id = records[0].source_record_id
        assert source_record_id is not None
        assert source_record_id.startswith("geosure-hex-0001:sha256-")


class TestMappers:
    def test_maps_geosure_area_with_polygon_geometry_ref(self) -> None:
        result = geosure.BgsGeosureBasicAreaMapper()(_record(), _snapshot())
        area = result.record

        assert area.area_key == build_hazard_risk_area_key(
            geosure.SOURCE_ID,
            geosure.BGS_GEOSURE_BASIC_DATASET_ID,
            "geosure-hex-0001",
        )
        assert area.area_kind.value is HazardRiskAreaKind.RISK_INDEX_AREA
        assert area.jurisdiction.value is not None
        assert area.jurisdiction.value.country == "GB"
        assert area.jurisdiction.value.region == "Wales"
        assert area.geometry_ref.value is not None
        assert area.geometry_ref.value.geometry_type is GeometryType.POLYGON
        assert area.source_hazards.value is not None
        assert area.source_hazards.value[0].code == "combined-geohazard"

    def test_maps_combined_geohazard_scores_as_source_specific(self) -> None:
        result = geosure.BgsGeosureBasicScoresMapper()(_record(), _snapshot())
        scores = result.record

        assert len(scores) == 2
        assert scores[0].hazard_type.value is HazardRiskHazardType.SOURCE_SPECIFIC
        assert scores[0].score_type.value is HazardRiskScoreType.RATING
        assert scores[0].methodology_version.value == "GeoSure Basic v8"
        assert scores[1].score_type.value is HazardRiskScoreType.PER_HAZARD_SCORE
        assert isinstance(scores[1].score_measure.value, NumericScoreMeasure)
        assert scores[1].score_scale.value is not None
        assert scores[1].score_scale.value.maximum == 3
        assert scores[1].source_caveats.quality is FieldQuality.STANDARDIZED

    def test_landslide_theme_maps_to_landslide_hazard_type(self) -> None:
        raw = _row()
        raw["geohazard_theme"] = "Landslides"
        result = geosure.BgsGeosureBasicScoresMapper()(_record(raw), _snapshot())

        assert result.record[0].hazard_type.value is HazardRiskHazardType.LANDSLIDE


class TestDrift:
    async def test_fixture_drift_clean(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(geosure.BGS_GEOSURE_BASIC_DATASET_PAGE_URL).mock(
                return_value=httpx.Response(200, content=_json_bytes())
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(_adapter(client), geosure.BgsGeosureBasicScoresMapper())
                schema_obs = SchemaObserver(spec=geosure.BGS_GEOSURE_BASIC_SCHEMA)
                taxonomy_obs = TaxonomyObserver(specs=geosure.BGS_GEOSURE_BASIC_TAXONOMIES)
                observed = attach_observers(pipeline_result, [schema_obs, taxonomy_obs])
                records = [record async for record in observed.records]

        assert len(records) == 1
        assert schema_obs.finalize(pipeline_result.snapshot).findings == ()
        assert taxonomy_obs.finalize(pipeline_result.snapshot).findings == ()


def test_source_metadata_preserves_geosure_scope() -> None:
    assert geosure.SOURCE_ID == "british-geological-survey"
    assert geosure.BGS_GEOSURE_BASIC_DATASET_ID == "geosure_basic"
    assert "generalized Great Britain geohazard" in geosure.BGS_GEOSURE_BASIC_SOURCE_SCOPE
