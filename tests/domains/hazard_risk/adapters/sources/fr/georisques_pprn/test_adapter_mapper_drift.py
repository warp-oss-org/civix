"""Tests for the France Georisques GASPAR PPRN source slice."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from civix.core.drift import SchemaObserver, TaxonomyDriftKind, TaxonomyObserver
from civix.core.identity.models.identifiers import DatasetId, SnapshotId
from civix.core.mapping.errors import MappingError
from civix.core.pipeline import attach_observers, run
from civix.core.ports.errors import FetchError
from civix.core.ports.models.adapter import SourceAdapter
from civix.core.quality.models.fields import FieldQuality
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.temporal import TemporalPeriodPrecision
from civix.domains.hazard_risk.adapters.sources.fr import georisques_pprn as pprn
from civix.domains.hazard_risk.models import (
    HazardRiskAreaKind,
    HazardRiskHazardType,
    HazardRiskZoneStatus,
    build_hazard_risk_area_key,
    build_hazard_risk_zone_key,
)

PINNED_NOW = datetime(2026, 5, 3, 12, 0, tzinfo=UTC)
FIXTURES = Path(__file__).parent / "fixtures"
CSV_FIXTURE = FIXTURES / "pprn_gaspar.csv"


def _csv_bytes() -> bytes:
    return CSV_FIXTURE.read_bytes()


def _csv_text() -> str:
    return CSV_FIXTURE.read_text()


def _fixture_rows() -> list[dict[str, str]]:
    reader = csv.reader(CSV_FIXTURE.open(), delimiter=";")
    header = next(reader)
    fieldnames = _canonical_fieldnames(header)

    return [dict(zip(fieldnames, row, strict=True)) for row in reader]


def _canonical_fieldnames(fieldnames: list[str]) -> tuple[str, ...]:
    seen: dict[str, int] = {}
    canonical: list[str] = []

    for field_name in fieldnames:
        count = seen.get(field_name, 0) + 1
        seen[field_name] = count

        if field_name == "CODE RISQUE 2" and count == 2:
            canonical.append("CODE RISQUE 3")
            continue

        canonical.append(field_name)

    return tuple(canonical)


def _row(index: int = 0, **overrides: Any) -> dict[str, Any]:
    raw = dict(_fixture_rows()[index])
    raw.update(overrides)

    return raw


def _snapshot(dataset_id: DatasetId = pprn.GEORISQUES_PPRN_DATASET_ID) -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId("snap-georisques-pprn"),
        source_id=pprn.SOURCE_ID,
        dataset_id=dataset_id,
        jurisdiction=pprn.FR_JURISDICTION,
        fetched_at=PINNED_NOW,
        record_count=5,
        source_url=pprn.GEORISQUES_PPRN_CSV_URL,
    )


def _record(raw: dict[str, Any] | None = None, *, index: int = 1) -> RawRecord:
    payload = _row(index - 1) if raw is None else raw

    return RawRecord(
        snapshot_id=SnapshotId("snap-georisques-pprn"),
        raw_data=payload,
        source_record_id=_source_record_id(payload),
    )


def _source_record_id(raw: dict[str, Any]) -> str:
    record_hash = hashlib.sha256(
        json.dumps(raw, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()

    return (
        f"{raw['CODE PROECEDURE']}:{raw['CODE INSEE COMMUNE']}:{raw['CODE RISQUE 3']}:"
        f"sha256-{record_hash[:16]}"
    )


def _adapter(client: httpx.AsyncClient) -> pprn.GeorisquesPprnAdapter:
    return pprn.GeorisquesPprnAdapter(
        fetch_config=pprn.GeorisquesPprnFetchConfig(client=client, clock=lambda: PINNED_NOW)
    )


class TestAdapter:
    async def test_fetches_traceable_gaspar_csv_rows(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(pprn.GEORISQUES_PPRN_CSV_URL).mock(
                return_value=httpx.Response(200, content=_csv_bytes())
            )

            async with httpx.AsyncClient() as client:
                adapter = _adapter(client)
                result = await adapter.fetch()
                records = [record async for record in result.records]

        assert isinstance(adapter, SourceAdapter)
        assert result.snapshot.source_id == pprn.SOURCE_ID
        assert result.snapshot.dataset_id == pprn.GEORISQUES_PPRN_DATASET_ID
        assert result.snapshot.jurisdiction == pprn.FR_JURISDICTION
        assert result.snapshot.source_url == pprn.GEORISQUES_PPRN_CSV_URL
        assert result.snapshot.fetch_params == {"format": "csv", "delimiter": ";"}
        assert result.snapshot.content_hash == hashlib.sha256(_csv_bytes()).hexdigest()
        assert result.snapshot.record_count == 5
        assert [record.source_record_id for record in records] == [
            _source_record_id(row) for row in _fixture_rows()
        ]
        assert records[0].raw_data["CODE RISQUE 2"] == "11"
        assert records[0].raw_data["CODE RISQUE 3"] == "110"
        assert records[0].raw_data["LIBELLE SOUS-ETAT"] == "Approuvé"

    async def test_missing_header_fails_fetch(self) -> None:
        content = _csv_text().replace("CODE PROECEDURE", "CODE PROCEDURE", 1).encode()

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(pprn.GEORISQUES_PPRN_CSV_URL).mock(
                return_value=httpx.Response(200, content=content)
            )

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="missing required fields"):
                    await _adapter(client).fetch()

    async def test_malformed_csv_row_fails_fetch(self) -> None:
        content = (_csv_text() + "extra;columns\n").encode()

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(pprn.GEORISQUES_PPRN_CSV_URL).mock(
                return_value=httpx.Response(200, content=content)
            )

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="malformed Georisques PPRN CSV row"):
                    await _adapter(client).fetch()


class TestAreaMapper:
    def test_maps_commune_as_administrative_area_without_plan_geometry(self) -> None:
        result = pprn.GeorisquesPprnAreaMapper()(_record(), _snapshot())
        area = result.record

        assert area.area_key == build_hazard_risk_area_key(
            pprn.SOURCE_ID,
            pprn.GEORISQUES_PPRN_DATASET_ID,
            "84091",
        )
        assert area.area_kind.value is HazardRiskAreaKind.ADMINISTRATIVE_AREA
        assert area.name.value == "Piolenc"
        assert area.jurisdiction.value is not None
        assert area.jurisdiction.value.country == "FR"
        assert area.jurisdiction.value.region == "93"
        assert area.jurisdiction.value.locality == "84091"
        assert area.administrative_areas.value == (
            "PROVENCE-ALPES-COTE D'AZUR",
            "VAUCLUSE",
            "Piolenc",
        )
        assert area.footprint.quality is FieldQuality.UNMAPPED
        assert area.geometry_ref.quality is FieldQuality.UNMAPPED
        assert area.source_hazards.quality is FieldQuality.UNMAPPED

        identifiers = area.source_area_identifiers.value
        assert identifiers is not None
        assert [identifier.value for identifier in identifiers] == [
            "84091",
            "84",
            "93",
        ]


class TestZoneMapper:
    def test_maps_approved_pprn_row_to_effective_regulatory_zone_context(self) -> None:
        result = pprn.GeorisquesPprnZoneMapper()(_record(), _snapshot())
        zone = result.record

        assert zone.zone_key == build_hazard_risk_zone_key(
            pprn.SOURCE_ID,
            pprn.GEORISQUES_PPRN_DATASET_ID,
            _source_record_id(_row()),
        )
        assert zone.plan_identifier.value == "84DDT20020008"
        assert zone.plan_name.value == "PPRN-I - Rhône [ Piolenc ]"
        assert zone.hazard_type.value is HazardRiskHazardType.FLOOD
        assert zone.source_hazard.value is not None
        assert zone.source_hazard.value.label == (
            "Inondation - Inondation - Par une crue à débordement lent de cours d'eau"
        )
        assert zone.source_zone.value is not None
        assert zone.source_zone.value.code.startswith("model-pprn-i-risk-110")
        assert zone.status.value is HazardRiskZoneStatus.EFFECTIVE
        assert zone.source_status.value is not None
        assert zone.source_status.value.label == "Opposable - Approuvé"
        assert zone.effective_period.value is not None
        assert zone.effective_period.value.precision is TemporalPeriodPrecision.DATE
        assert zone.effective_period.value.date_value == date(2025, 8, 14)
        assert zone.footprint.quality is FieldQuality.UNMAPPED
        assert zone.geometry_ref.quality is FieldQuality.UNMAPPED

    def test_anticipated_status_maps_to_effective_with_anticipated_date(self) -> None:
        result = pprn.GeorisquesPprnZoneMapper()(_record(index=2), _snapshot())
        zone = result.record

        assert zone.hazard_type.value is HazardRiskHazardType.WILDFIRE
        assert zone.status.value is HazardRiskZoneStatus.EFFECTIVE
        assert zone.effective_period.value is not None
        assert zone.effective_period.value.date_value == date(2024, 6, 1)

    def test_prescribed_status_maps_to_in_progress_without_effective_period(self) -> None:
        result = pprn.GeorisquesPprnZoneMapper()(_record(index=3), _snapshot())
        zone = result.record

        assert zone.hazard_type.value is HazardRiskHazardType.LANDSLIDE
        assert zone.status.value is HazardRiskZoneStatus.IN_PROGRESS
        assert zone.effective_period.quality is FieldQuality.NOT_PROVIDED

    def test_cancelled_and_abrogated_statuses_remain_distinguishable(self) -> None:
        cancelled = pprn.GeorisquesPprnZoneMapper()(_record(index=4), _snapshot()).record
        abrogated = pprn.GeorisquesPprnZoneMapper()(_record(index=5), _snapshot()).record

        assert cancelled.status.value is HazardRiskZoneStatus.CANCELLED
        assert abrogated.status.value is HazardRiskZoneStatus.ABROGATED

    def test_caduque_without_more_specific_substate_maps_to_abrogated(self) -> None:
        raw = _row(index=4, **{"LIBELLE SOUS-ETAT": ""})
        result = pprn.GeorisquesPprnZoneMapper()(_record(raw), _snapshot())

        assert result.record.status.value is HazardRiskZoneStatus.ABROGATED

    def test_invalid_nonblank_effective_date_fails_mapping(self) -> None:
        raw = _row(APPROBATION="2025-99-14")

        with pytest.raises(MappingError, match="invalid date source field 'APPROBATION'"):
            pprn.GeorisquesPprnZoneMapper()(_record(raw), _snapshot())

    def test_effective_period_is_not_provided_when_all_effective_dates_are_blank(self) -> None:
        raw = _row(APPROBATION="", APPLIC_ANTIC="", PRESCRIPTION="")
        result = pprn.GeorisquesPprnZoneMapper()(_record(raw), _snapshot())

        assert result.record.effective_period.quality is FieldQuality.NOT_PROVIDED
        assert result.record.effective_period.source_fields == (
            "APPROBATION",
            "APPLIC_ANTIC",
        )

    def test_mapping_report_keeps_unconsumed_procedure_dates_visible(self) -> None:
        result = pprn.GeorisquesPprnZoneMapper()(_record(), _snapshot())

        assert "PROGRAMMATION_DEBUT" in result.report.unmapped_source_fields
        assert "DATE DERNIERE MISE A JOUR" in result.report.unmapped_source_fields
        assert "APPROBATION" not in result.report.unmapped_source_fields
        assert "LIBELLE ETAT" not in result.report.unmapped_source_fields

    def test_wrong_dataset_fails_mapping(self) -> None:
        with pytest.raises(MappingError, match="GASPAR PPRN dataset"):
            pprn.GeorisquesPprnZoneMapper()(
                _record(),
                _snapshot(DatasetId("pprt_gaspar")),
            )


class TestDrift:
    async def test_fixture_drift_clean(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(pprn.GEORISQUES_PPRN_CSV_URL).mock(
                return_value=httpx.Response(200, content=_csv_bytes())
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(_adapter(client), pprn.GeorisquesPprnZoneMapper())
                schema_obs = SchemaObserver(spec=pprn.GEORISQUES_PPRN_SCHEMA)
                taxonomy_obs = TaxonomyObserver(specs=pprn.GEORISQUES_PPRN_TAXONOMIES)
                observed = attach_observers(pipeline_result, [schema_obs, taxonomy_obs])
                records = [record async for record in observed.records]

        assert len(records) == 5
        assert schema_obs.finalize(pipeline_result.snapshot).findings == ()
        assert taxonomy_obs.finalize(pipeline_result.snapshot).findings == ()

    async def test_unknown_risk_and_status_surface_taxonomy_drift(self) -> None:
        rows = _fixture_rows()
        rows[0]["CODE RISQUE 3"] = "999"
        rows[0]["LIBELLE ETAT"] = "Futur"
        rows[0]["LIBELLE SOUS-ETAT"] = "Expérimental"
        content = _csv_from_rows(rows).encode()

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(pprn.GEORISQUES_PPRN_CSV_URL).mock(
                return_value=httpx.Response(200, content=content)
            )

            async with httpx.AsyncClient() as client:
                fetch_result = await _adapter(client).fetch()
                taxonomy_obs = TaxonomyObserver(specs=pprn.GEORISQUES_PPRN_TAXONOMIES)
                async for record in fetch_result.records:
                    taxonomy_obs.observe(record)

        report = taxonomy_obs.finalize(fetch_result.snapshot)

        assert any(
            finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE
            and finding.taxonomy_id == "georisques-pprn-risk"
            for finding in report.findings
        )
        assert any(
            finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE
            and finding.taxonomy_id == "georisques-pprn-state"
            for finding in report.findings
        )
        assert any(
            finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE
            and finding.taxonomy_id == "georisques-pprn-substate"
            for finding in report.findings
        )


def _csv_from_rows(rows: list[dict[str, str]]) -> str:
    lines = [";".join(pprn.GEORISQUES_PPRN_FIELDS)]
    for row in rows:
        lines.append(";".join(row[field] for field in pprn.GEORISQUES_PPRN_FIELDS))

    return "\n".join(lines)


def test_source_metadata_preserves_georisques_pprn_contract() -> None:
    assert pprn.SOURCE_ID == "georisques-gaspar"
    assert pprn.GEORISQUES_PPRN_DATASET_ID == "pprn_gaspar"
    assert pprn.FR_JURISDICTION.country == "FR"
    assert pprn.GEORISQUES_PPRN_CSV_URL == "https://files.georisques.fr/GASPAR/pprn_gaspar.csv"
    assert "commune-grained" in pprn.GEORISQUES_PPRN_SOURCE_SCOPE
