"""Tests for the FEMA NRI source slice."""

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
from civix.domains.hazard_risk.adapters.sources.us.fema_nri import (
    FEMA_NRI_SOURCE_SCOPE,
    FEMA_NRI_TRACTS_DATASET_ID,
    FEMA_NRI_TRACTS_ORDER,
    FEMA_NRI_TRACTS_QUERY_URL,
    FEMA_NRI_TRACTS_SCHEMA,
    FEMA_NRI_TRACTS_SOURCE_CRS,
    FEMA_NRI_TRACTS_TAXONOMIES,
    NRI_HAZARD_PREFIXES,
    SOURCE_ID,
    US_JURISDICTION,
    FemaNriAreaMapper,
    FemaNriScoresMapper,
    FemaNriTractsAdapter,
    FemaNriTractsFetchConfig,
)
from civix.domains.hazard_risk.models import (
    CategoryScoreMeasure,
    HazardRiskAreaKind,
    HazardRiskHazardType,
    HazardRiskScoreDirection,
    HazardRiskScoreType,
    NumericScoreMeasure,
    build_hazard_risk_area_key,
)

PINNED_NOW = datetime(2026, 5, 3, 12, 0, tzinfo=UTC)
FIXTURES = Path(__file__).parent / "fixtures"


def _query_response() -> dict[str, Any]:
    return json.loads((FIXTURES / "tract_query_response.json").read_text())


def _row() -> dict[str, Any]:
    return dict(_query_response()["features"][0]["attributes"])


def _record(raw: dict[str, Any] | None = None) -> RawRecord:
    payload = _row() if raw is None else raw

    return RawRecord(
        snapshot_id=SnapshotId("snap-fema-nri"),
        raw_data=payload,
        source_record_id=str(payload["NRI_ID"]),
    )


def _snapshot() -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId("snap-fema-nri"),
        source_id=SOURCE_ID,
        dataset_id=FEMA_NRI_TRACTS_DATASET_ID,
        jurisdiction=US_JURISDICTION,
        fetched_at=PINNED_NOW,
        record_count=1,
        source_url=FEMA_NRI_TRACTS_QUERY_URL,
    )


def _adapter(client: httpx.AsyncClient, *, page_size: int = 1000) -> FemaNriTractsAdapter:
    return FemaNriTractsAdapter(
        fetch_config=FemaNriTractsFetchConfig(
            client=client,
            clock=lambda: PINNED_NOW,
            page_size=page_size,
            where="NRI_ID='T01001020100'",
        )
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
    async def test_fetches_traceable_arcgis_attribute_rows(self) -> None:
        requests: list[httpx.Request] = []

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(FEMA_NRI_TRACTS_QUERY_URL).mock(
                side_effect=_capture_sequence(
                    requests,
                    [
                        httpx.Response(200, json={"count": 1}),
                        httpx.Response(200, json=_query_response()),
                    ],
                )
            )

            async with httpx.AsyncClient() as client:
                adapter = _adapter(client)
                result = await adapter.fetch()
                records = [record async for record in result.records]

        assert isinstance(adapter, SourceAdapter)
        assert result.snapshot.source_id == SOURCE_ID
        assert result.snapshot.dataset_id == FEMA_NRI_TRACTS_DATASET_ID
        assert result.snapshot.source_url == FEMA_NRI_TRACTS_QUERY_URL
        assert result.snapshot.fetch_params is not None
        assert result.snapshot.fetch_params["where"] == "NRI_ID='T01001020100'"
        assert result.snapshot.fetch_params["returnGeometry"] == "false"
        assert result.snapshot.fetch_params["orderByFields"] == FEMA_NRI_TRACTS_ORDER
        assert requests[0].url.params["returnCountOnly"] == "true"
        assert requests[1].url.params["returnGeometry"] == "false"
        assert [record.source_record_id for record in records] == ["T01001020100"]
        assert records[0].raw_data["TRACTFIPS"] == "01001020100"
        assert "features" not in records[0].raw_data


class TestAreaMapper:
    def test_maps_tract_area_keyed_by_tract_fips_with_geometry_ref(self) -> None:
        result = FemaNriAreaMapper()(_record(), _snapshot())
        area = result.record

        assert area.area_key == build_hazard_risk_area_key(
            SOURCE_ID,
            FEMA_NRI_TRACTS_DATASET_ID,
            "01001020100",
        )

        assert area.area_kind.value is HazardRiskAreaKind.CENSUS_UNIT
        assert area.jurisdiction.value is not None
        assert area.jurisdiction.value.region == "AL"
        assert area.footprint.quality is FieldQuality.UNMAPPED
        assert area.geometry_ref.value is not None
        assert area.geometry_ref.value.geometry_id == "T01001020100"
        assert area.geometry_ref.value.source_crs == FEMA_NRI_TRACTS_SOURCE_CRS

        identifiers = area.source_area_identifiers.value

        assert identifiers is not None
        assert [identifier.value for identifier in identifiers] == [
            "01001020100",
            "T01001020100",
            "01",
            "001",
            "01001",
        ]

    def test_missing_required_area_field_raises_mapper_scoped_error(self) -> None:
        raw = _row()
        raw["STATE"] = None

        with pytest.raises(MappingError) as exc_info:
            FemaNriAreaMapper()(_record(raw), _snapshot())

        assert exc_info.value.source_record_id == "T01001020100"
        assert exc_info.value.source_fields == ("STATE",)


class TestScoresMapper:
    def test_maps_core_and_hazard_scores_with_methodology_context(self) -> None:
        result = FemaNriScoresMapper()(_record(), _snapshot())
        scores = result.record

        assert len(scores) == 32
        assert {score.area_key for score in scores} == {
            build_hazard_risk_area_key(SOURCE_ID, FEMA_NRI_TRACTS_DATASET_ID, "01001020100")
        }

        risk_score = next(score for score in scores if score.score_id.endswith(":risk_score"))

        assert risk_score.score_type.value is HazardRiskScoreType.COMPOSITE_INDEX
        assert risk_score.hazard_type.value is HazardRiskHazardType.MULTI_HAZARD
        assert isinstance(risk_score.score_measure.value, NumericScoreMeasure)
        assert risk_score.score_measure.value.value == Decimal("16.48531982448004")
        assert risk_score.methodology_version.value == "1.20.0"
        assert risk_score.publication_vintage.value is not None
        assert risk_score.publication_vintage.value.year_value == 2025
        assert risk_score.publication_vintage.value.month_value == 12

        inland_flooding = next(score for score in scores if score.score_id.endswith(":ifld_risks"))

        assert inland_flooding.hazard_type.value is HazardRiskHazardType.FLOOD
        assert inland_flooding.score_type.value is HazardRiskScoreType.PER_HAZARD_SCORE
        assert inland_flooding.source_hazard.value is not None
        assert inland_flooding.source_hazard.value.label == "Inland Flooding"

        resilience = next(score for score in scores if score.score_id.endswith(":resl_score"))

        assert resilience.score_direction.value is HazardRiskScoreDirection.HIGHER_IS_BETTER

    def test_hazard_prefix_type_mapping_is_explicit_for_all_nri_hazards(self) -> None:
        scores = FemaNriScoresMapper()(_record(), _snapshot()).record
        hazard_types = {
            score.source_hazard.value.code: score.hazard_type.value
            for score in scores
            if score.source_hazard.value is not None
            and score.source_hazard.value.code != "all-hazards"
        }

        assert hazard_types == {
            "avln": HazardRiskHazardType.SOURCE_SPECIFIC,
            "cfld": HazardRiskHazardType.COASTAL,
            "cwav": HazardRiskHazardType.WINTER_WEATHER,
            "drgt": HazardRiskHazardType.DROUGHT,
            "erqk": HazardRiskHazardType.EARTHQUAKE,
            "hail": HazardRiskHazardType.STORM,
            "hrcn": HazardRiskHazardType.STORM,
            "hwav": HazardRiskHazardType.HEAT,
            "ifld": HazardRiskHazardType.FLOOD,
            "istm": HazardRiskHazardType.WINTER_WEATHER,
            "lnds": HazardRiskHazardType.LANDSLIDE,
            "ltng": HazardRiskHazardType.STORM,
            "swnd": HazardRiskHazardType.WIND,
            "trnd": HazardRiskHazardType.WIND,
            "tsun": HazardRiskHazardType.SOURCE_SPECIFIC,
            "vlcn": HazardRiskHazardType.SOURCE_SPECIFIC,
            "wfir": HazardRiskHazardType.WILDFIRE,
            "wntw": HazardRiskHazardType.WINTER_WEATHER,
        }

    def test_null_numeric_not_applicable_rating_emits_rating_fact_only(self) -> None:
        scores = FemaNriScoresMapper()(_record(), _snapshot()).record
        avalanche_scores = [
            score
            for score in scores
            if score.source_hazard.value is not None and score.source_hazard.value.code == "avln"
        ]

        assert [score.score_id for score in avalanche_scores] == ["01001020100:avln_riskr"]
        rating = avalanche_scores[0]

        assert rating.hazard_type.value is HazardRiskHazardType.SOURCE_SPECIFIC
        assert rating.score_type.value is HazardRiskScoreType.RATING
        assert isinstance(rating.score_measure.value, CategoryScoreMeasure)
        assert rating.score_measure.value.value.code == "not-applicable"
        assert rating.score_direction.value is HazardRiskScoreDirection.NOT_APPLICABLE

    def test_unknown_nri_version_fails_methodology_mapping(self) -> None:
        raw = _row()
        raw["NRI_VER"] = "Future Release"

        with pytest.raises(MappingError, match="unrecognized FEMA NRI version"):
            FemaNriScoresMapper()(_record(raw), _snapshot())


class TestDrift:
    async def test_fixture_drift_clean(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(FEMA_NRI_TRACTS_QUERY_URL).mock(
                side_effect=[
                    httpx.Response(200, json={"count": 1}),
                    httpx.Response(200, json=_query_response()),
                ]
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(_adapter(client), FemaNriScoresMapper())
                schema_obs = SchemaObserver(spec=FEMA_NRI_TRACTS_SCHEMA)
                taxonomy_obs = TaxonomyObserver(specs=FEMA_NRI_TRACTS_TAXONOMIES)
                observed = attach_observers(pipeline_result, [schema_obs, taxonomy_obs])
                records = [record async for record in observed.records]

        assert len(records) == 1
        assert schema_obs.finalize(pipeline_result.snapshot).findings == ()
        assert taxonomy_obs.finalize(pipeline_result.snapshot).findings == ()

    async def test_unknown_rating_and_version_surface_taxonomy_drift(self) -> None:
        payload = _query_response()
        attributes = payload["features"][0]["attributes"]
        attributes["RISK_RATNG"] = "Extremely Low"
        attributes["NRI_VER"] = "Future Release"

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(FEMA_NRI_TRACTS_QUERY_URL).mock(
                side_effect=[
                    httpx.Response(200, json={"count": 1}),
                    httpx.Response(200, json=payload),
                ]
            )

            async with httpx.AsyncClient() as client:
                fetch_result = await _adapter(client).fetch()
                taxonomy_obs = TaxonomyObserver(specs=FEMA_NRI_TRACTS_TAXONOMIES)
                async for record in fetch_result.records:
                    taxonomy_obs.observe(record)

        report = taxonomy_obs.finalize(fetch_result.snapshot)

        assert any(
            finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE
            and finding.taxonomy_id == "fema-nri-risk-ratng"
            for finding in report.findings
        )

        assert any(
            finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE
            and finding.taxonomy_id == "fema-nri-version"
            for finding in report.findings
        )


def test_source_metadata_preserves_scope_and_all_hazard_prefixes() -> None:
    assert SOURCE_ID == "fema-arcgis"
    assert FEMA_NRI_TRACTS_DATASET_ID == "National_Risk_Index_Census_Tracts"
    assert "National Risk Index" in FEMA_NRI_SOURCE_SCOPE
    assert NRI_HAZARD_PREFIXES == (
        "AVLN",
        "CFLD",
        "CWAV",
        "DRGT",
        "ERQK",
        "HAIL",
        "HRCN",
        "HWAV",
        "IFLD",
        "ISTM",
        "LNDS",
        "LTNG",
        "SWND",
        "TRND",
        "TSUN",
        "VLCN",
        "WFIR",
        "WNTW",
    )
