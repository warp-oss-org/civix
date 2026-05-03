"""Tests for the FEMA NFHL source slice."""

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
from civix.core.identity.models.identifiers import DatasetId, SnapshotId
from civix.core.mapping.errors import MappingError
from civix.core.pipeline import attach_observers, run
from civix.core.ports.models.adapter import SourceAdapter
from civix.core.quality.models.fields import FieldQuality
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.spatial.models.geometry import GeometryType
from civix.domains.hazard_risk.adapters.sources.us.fema_nfhl import (
    FEMA_NFHL_FLOOD_HAZARD_ZONES_DATASET_ID,
    FEMA_NFHL_FLOOD_HAZARD_ZONES_LAYER_NAME,
    FEMA_NFHL_FLOOD_HAZARD_ZONES_ORDER,
    FEMA_NFHL_FLOOD_HAZARD_ZONES_QUERY_URL,
    FEMA_NFHL_FLOOD_HAZARD_ZONES_SCHEMA,
    FEMA_NFHL_FLOOD_HAZARD_ZONES_SERVICE_URL,
    FEMA_NFHL_FLOOD_HAZARD_ZONES_SOURCE_CRS,
    FEMA_NFHL_FLOOD_HAZARD_ZONES_TAXONOMIES,
    FEMA_NFHL_SOURCE_SCOPE,
    SOURCE_ID,
    US_JURISDICTION,
    FemaNfhlCaveat,
    FemaNfhlFloodHazardZonesAdapter,
    FemaNfhlFloodHazardZonesFetchConfig,
    FemaNfhlZoneMapper,
)
from civix.domains.hazard_risk.models import (
    HazardRiskHazardType,
    HazardRiskZoneStatus,
    build_hazard_risk_zone_key,
)

PINNED_NOW = datetime(2026, 5, 3, 12, 0, tzinfo=UTC)
FIXTURES = Path(__file__).parent / "fixtures"


def _query_response() -> dict[str, Any]:
    return json.loads((FIXTURES / "flood_hazard_zones_query_response.json").read_text())


def _row(index: int = 0) -> dict[str, Any]:
    return dict(_query_response()["features"][index]["attributes"])


def _record(raw: dict[str, Any] | None = None) -> RawRecord:
    payload = _row() if raw is None else raw

    return RawRecord(
        snapshot_id=SnapshotId("snap-fema-nfhl"),
        raw_data=payload,
        source_record_id=str(payload["FLD_AR_ID"]),
    )


def _snapshot(dataset_id: DatasetId = FEMA_NFHL_FLOOD_HAZARD_ZONES_DATASET_ID) -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId("snap-fema-nfhl"),
        source_id=SOURCE_ID,
        dataset_id=dataset_id,
        jurisdiction=US_JURISDICTION,
        fetched_at=PINNED_NOW,
        record_count=2,
        source_url=FEMA_NFHL_FLOOD_HAZARD_ZONES_QUERY_URL,
    )


def _adapter(
    client: httpx.AsyncClient,
    *,
    page_size: int = 1000,
) -> FemaNfhlFloodHazardZonesAdapter:
    return FemaNfhlFloodHazardZonesAdapter(
        fetch_config=FemaNfhlFloodHazardZonesFetchConfig(
            client=client,
            clock=lambda: PINNED_NOW,
            page_size=page_size,
            where="DFIRM_ID='01001C'",
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
    async def test_fetches_traceable_arcgis_attribute_rows_from_mapserver(self) -> None:
        requests: list[httpx.Request] = []

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(FEMA_NFHL_FLOOD_HAZARD_ZONES_QUERY_URL).mock(
                side_effect=_capture_sequence(
                    requests,
                    [
                        httpx.Response(200, json={"count": 2}),
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
        assert result.snapshot.dataset_id == FEMA_NFHL_FLOOD_HAZARD_ZONES_DATASET_ID
        assert result.snapshot.source_url == FEMA_NFHL_FLOOD_HAZARD_ZONES_QUERY_URL
        assert result.snapshot.fetch_params is not None
        assert result.snapshot.fetch_params["where"] == "DFIRM_ID='01001C'"
        assert result.snapshot.fetch_params["returnGeometry"] == "false"
        assert result.snapshot.fetch_params["orderByFields"] == FEMA_NFHL_FLOOD_HAZARD_ZONES_ORDER
        assert requests[0].url.params["returnCountOnly"] == "true"
        assert requests[1].url.params["returnGeometry"] == "false"
        assert requests[1].url.params["orderByFields"] == "OBJECTID"
        assert [record.source_record_id for record in records] == ["01001C_0001", "01001C_0002"]
        assert records[0].raw_data["FLD_ZONE"] == "AE"
        assert "features" not in records[0].raw_data


class TestZoneMapper:
    def test_maps_effective_flood_zone_with_deterministic_geometry_ref(self) -> None:
        result = FemaNfhlZoneMapper()(_record(), _snapshot())
        zone = result.record

        assert zone.zone_key == build_hazard_risk_zone_key(
            SOURCE_ID,
            FEMA_NFHL_FLOOD_HAZARD_ZONES_DATASET_ID,
            "01001C_0001",
        )
        assert zone.hazard_type.value is HazardRiskHazardType.FLOOD
        assert zone.status.value is HazardRiskZoneStatus.EFFECTIVE
        assert zone.source_status.value is not None
        assert zone.source_status.value.taxonomy_id == "fema-nfhl-layer-status"
        assert zone.source_zone.value is not None
        assert zone.source_zone.value.code == "fld-zone-ae__sfha-y__subtype-floodway"
        assert zone.plan_identifier.value == "01001C"
        assert zone.plan_name.value == "FEMA NFHL DFIRM 01001C"
        assert zone.plan_name.quality is FieldQuality.DERIVED
        assert zone.effective_period.quality is FieldQuality.UNMAPPED
        assert zone.footprint.quality is FieldQuality.UNMAPPED
        assert zone.geometry_ref.value is not None
        assert zone.geometry_ref.value.geometry_type is GeometryType.POLYGON
        assert zone.geometry_ref.value.uri == FEMA_NFHL_FLOOD_HAZARD_ZONES_SERVICE_URL
        assert zone.geometry_ref.value.layer_name == FEMA_NFHL_FLOOD_HAZARD_ZONES_LAYER_NAME
        assert zone.geometry_ref.value.geometry_id == "01001C_0001"
        assert zone.geometry_ref.value.source_crs == FEMA_NFHL_FLOOD_HAZARD_ZONES_SOURCE_CRS
        assert zone.geometry_ref.value.query_keys == (
            ("FLD_AR_ID", "01001C_0001"),
            ("DFIRM_ID", "01001C"),
        )

        identifiers = zone.source_zone_identifiers.value
        assert identifiers is not None
        assert [identifier.value for identifier in identifiers] == [
            "01001C_0001",
            "01001C",
            "nfhl-gfid-0001",
            "{11111111-1111-1111-1111-111111111111}",
        ]

    def test_non_sfha_zone_has_stable_source_zone_code_without_subtype(self) -> None:
        result = FemaNfhlZoneMapper()(_record(_row(1)), _snapshot())
        zone = result.record

        assert zone.source_zone.value is not None
        assert zone.source_zone.value.code == "fld-zone-x__sfha-n__subtype-none"

    def test_effective_status_requires_effective_nfhl_dataset(self) -> None:
        with pytest.raises(MappingError, match="effective Flood Hazard Zones dataset"):
            FemaNfhlZoneMapper()(
                _record(),
                _snapshot(DatasetId("Prelim_NFHL_Flood_Hazard_Zones")),
            )

    def test_unrecognized_sfha_flag_fails_mapping(self) -> None:
        raw = _row()
        raw["SFHA_TF"] = "U"

        with pytest.raises(MappingError, match="unrecognized FEMA NFHL SFHA_TF"):
            FemaNfhlZoneMapper()(_record(raw), _snapshot())

    def test_source_caveats_include_effective_date_scope(self) -> None:
        result = FemaNfhlZoneMapper()(_record(), _snapshot())
        caveats = result.record.source_caveats.value

        assert caveats is not None
        assert {caveat.code for caveat in caveats} == {caveat.value for caveat in FemaNfhlCaveat}
        assert any("Effective Dates" in caveat.label for caveat in caveats)

    def test_mapping_report_pins_deliberate_schema_sentinel_fields(self) -> None:
        result = FemaNfhlZoneMapper()(_record(), _snapshot())

        assert result.report.unmapped_source_fields == (
            "AR_REVERT",
            "AR_SUBTRV",
            "BFE_REVERT",
            "DEPTH",
            "DEP_REVERT",
            "DUAL_ZONE",
            "LEN_UNIT",
            "STATIC_BFE",
            "STUDY_TYP",
            "VELOCITY",
            "VEL_UNIT",
            "V_DATUM",
        )


class TestDrift:
    async def test_fixture_drift_clean(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(FEMA_NFHL_FLOOD_HAZARD_ZONES_QUERY_URL).mock(
                side_effect=[
                    httpx.Response(200, json={"count": 2}),
                    httpx.Response(200, json=_query_response()),
                ]
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(_adapter(client), FemaNfhlZoneMapper())
                schema_obs = SchemaObserver(spec=FEMA_NFHL_FLOOD_HAZARD_ZONES_SCHEMA)
                taxonomy_obs = TaxonomyObserver(specs=FEMA_NFHL_FLOOD_HAZARD_ZONES_TAXONOMIES)
                observed = attach_observers(pipeline_result, [schema_obs, taxonomy_obs])
                records = [record async for record in observed.records]

        assert len(records) == 2
        assert schema_obs.finalize(pipeline_result.snapshot).findings == ()
        assert taxonomy_obs.finalize(pipeline_result.snapshot).findings == ()

    async def test_unknown_zone_subtype_and_sfha_flag_surface_taxonomy_drift(self) -> None:
        payload = _query_response()
        attributes = payload["features"][0]["attributes"]
        attributes["FLD_ZONE"] = "Future Zone"
        attributes["ZONE_SUBTY"] = "Future Subtype"
        attributes["SFHA_TF"] = "U"

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(FEMA_NFHL_FLOOD_HAZARD_ZONES_QUERY_URL).mock(
                side_effect=[
                    httpx.Response(200, json={"count": 2}),
                    httpx.Response(200, json=payload),
                ]
            )

            async with httpx.AsyncClient() as client:
                fetch_result = await _adapter(client).fetch()
                taxonomy_obs = TaxonomyObserver(specs=FEMA_NFHL_FLOOD_HAZARD_ZONES_TAXONOMIES)
                async for record in fetch_result.records:
                    taxonomy_obs.observe(record)

        report = taxonomy_obs.finalize(fetch_result.snapshot)

        assert any(
            finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE
            and finding.taxonomy_id == "fema-nfhl-zone"
            for finding in report.findings
        )
        assert any(
            finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE
            and finding.taxonomy_id == "fema-nfhl-zone-subtype"
            for finding in report.findings
        )
        assert any(
            finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE
            and finding.taxonomy_id == "fema-nfhl-sfha-flag"
            for finding in report.findings
        )


def test_source_metadata_preserves_nfhl_layer_contract() -> None:
    assert SOURCE_ID == "fema-arcgis"
    assert FEMA_NFHL_FLOOD_HAZARD_ZONES_DATASET_ID == "NFHL_Flood_Hazard_Zones"
    assert FEMA_NFHL_FLOOD_HAZARD_ZONES_ORDER == "OBJECTID"
    assert FEMA_NFHL_FLOOD_HAZARD_ZONES_SOURCE_CRS == "EPSG:4269"
    assert "MapServer layer 28" in FEMA_NFHL_SOURCE_SCOPE
