"""Tests for the France TMJA road-traffic source slice."""

from __future__ import annotations

import csv
from collections.abc import AsyncIterable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from civix.core.drift import (
    SchemaDriftKind,
    SchemaObserver,
    TaxonomyDriftKind,
    TaxonomyObserver,
)
from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, SnapshotId, SourceId
from civix.core.mapping.errors import MappingError
from civix.core.pipeline import attach_observers, run
from civix.core.ports.errors import FetchError
from civix.core.ports.models.adapter import FetchResult, SourceAdapter
from civix.core.quality.models.fields import FieldQuality
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.temporal import TemporalPeriodPrecision, TemporalTimezoneStatus
from civix.domains.mobility_observations.adapters.sources.fr import (
    tmja_road_traffic as fr_tmja,
)
from civix.domains.mobility_observations.models.common import (
    AggregationWindow,
    CountMetricType,
    CountUnit,
    MeasurementMethod,
    MobilitySiteKind,
    ObservationDirection,
    TravelMode,
)

PINNED_NOW = datetime(2026, 5, 2, 12, 0, tzinfo=UTC)
FIXTURES = Path(__file__).parent / "fixtures"
CSV_FIXTURE = FIXTURES / "tmja_rrnc_2024.csv"


def _csv_bytes() -> bytes:
    return CSV_FIXTURE.read_bytes()


def _csv_text() -> str:
    return CSV_FIXTURE.read_text()


def _fetch(client: httpx.AsyncClient) -> fr_tmja.FrTmjaRoadTrafficFetchConfig:
    return fr_tmja.FrTmjaRoadTrafficFetchConfig(client=client, clock=lambda: PINNED_NOW)


def _adapter(client: httpx.AsyncClient) -> fr_tmja.FrTmjaRoadTrafficAdapter:
    return fr_tmja.FrTmjaRoadTrafficAdapter(fetch_config=_fetch(client))


def _fixture_rows() -> list[dict[str, str]]:
    return list(csv.DictReader(CSV_FIXTURE.open(), delimiter=";"))


def _raw_fixture(index: int = 0, **overrides: Any) -> dict[str, Any]:
    raw = dict(_fixture_rows()[index])
    raw.update(overrides)

    return raw


def _snapshot() -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId("snap-fr-tmja"),
        source_id=fr_tmja.SOURCE_ID,
        dataset_id=fr_tmja.FR_TMJA_RRNC_2024_DATASET_ID,
        jurisdiction=fr_tmja.FR_TMJA_JURISDICTION,
        fetched_at=PINNED_NOW,
        record_count=1,
    )


def _record(raw: dict[str, Any]) -> RawRecord:
    return RawRecord(
        snapshot_id=SnapshotId("snap-fr-tmja"),
        raw_data=raw,
        source_record_id="A0001:18875:20374:I:2024:row-1",
    )


@dataclass(frozen=True, slots=True)
class _StaticAdapter:
    rows: tuple[dict[str, Any], ...]

    @property
    def source_id(self) -> SourceId:
        return fr_tmja.SOURCE_ID

    @property
    def dataset_id(self) -> DatasetId:
        return fr_tmja.FR_TMJA_RRNC_2024_DATASET_ID

    @property
    def jurisdiction(self) -> Jurisdiction:
        return fr_tmja.FR_TMJA_JURISDICTION

    async def fetch(self) -> FetchResult:
        snapshot = _snapshot()

        async def records() -> AsyncIterable[RawRecord]:
            for index, row in enumerate(self.rows, start=1):
                yield RawRecord(
                    snapshot_id=snapshot.snapshot_id,
                    raw_data=row,
                    source_record_id=f"row-{index}",
                )

        return FetchResult(snapshot=snapshot, records=records())


class TestAdapter:
    async def test_adapter_fetches_csv_and_preserves_source_shape(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(fr_tmja.DEFAULT_RESOURCE_URL).mock(
                return_value=httpx.Response(200, content=_csv_bytes())
            )

            async with httpx.AsyncClient() as client:
                adapter = _adapter(client)
                result = await adapter.fetch()
                records = [record async for record in result.records]

        assert isinstance(adapter, SourceAdapter)
        assert result.snapshot.source_id == fr_tmja.SOURCE_ID
        assert result.snapshot.dataset_id == fr_tmja.FR_TMJA_RRNC_2024_DATASET_ID
        assert result.snapshot.jurisdiction.country == "FR"
        assert result.snapshot.source_url == fr_tmja.DEFAULT_RESOURCE_URL
        assert result.snapshot.fetch_params == {"resource_id": fr_tmja.FR_TMJA_RESOURCE_ID}
        assert result.snapshot.content_hash is not None
        assert result.snapshot.record_count == 3
        assert records[0].source_record_id == "A0001:18875:20374:I:2024:row-1"
        assert records[0].record_hash is not None
        assert records[0].raw_data["typeComptageTrafic_lib"] == "Permanent horaire"
        assert records[0].raw_data["ratio_PL"] == "17,192"

    async def test_adapter_falls_back_to_cp1252(self) -> None:
        csv_text = _csv_text().replace("Permanent horaire", "Permanent horaire é", 1)

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(fr_tmja.DEFAULT_RESOURCE_URL).mock(
                return_value=httpx.Response(200, content=csv_text.encode("cp1252"))
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                records = [record async for record in result.records]

        assert records[0].raw_data["typeComptageTrafic_lib"] == "Permanent horaire é"

    async def test_adapter_http_failure_is_fetch_error(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(fr_tmja.DEFAULT_RESOURCE_URL).mock(return_value=httpx.Response(500))

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="failed to read TMJA CSV"):
                    await _adapter(client).fetch()

    async def test_adapter_missing_header_field_is_fetch_error(self) -> None:
        bad_csv = _csv_text().replace("ratio_PL", "ratio_pl")

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(fr_tmja.DEFAULT_RESOURCE_URL).mock(
                return_value=httpx.Response(200, content=bad_csv.encode())
            )

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="missing required fields"):
                    await _adapter(client).fetch()

    async def test_adapter_rejects_malformed_csv_rows(self) -> None:
        header, row, *_ = _csv_text().splitlines()
        bad_csv = f"{header}\n{row};unexpected\n"

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(fr_tmja.DEFAULT_RESOURCE_URL).mock(
                return_value=httpx.Response(200, content=bad_csv.encode())
            )

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="malformed TMJA CSV row"):
                    await _adapter(client).fetch()

    async def test_adapter_rejects_non_csv_200_response(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(fr_tmja.DEFAULT_RESOURCE_URL).mock(
                return_value=httpx.Response(200, content=b"<html>temporarily unavailable</html>")
            )

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="non-CSV TMJA response"):
                    await _adapter(client).fetch()

    async def test_adapter_rejects_empty_csv(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(fr_tmja.DEFAULT_RESOURCE_URL).mock(
                return_value=httpx.Response(200, content=b"")
            )

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="empty TMJA CSV"):
                    await _adapter(client).fetch()

    async def test_adapter_leaves_malformed_identity_source_record_id_empty(self) -> None:
        header, row, *_ = _csv_text().splitlines()
        columns = row.split(";")
        columns[1] = ""
        bad_identity_csv = f"{header}\n{';'.join(columns)}\n"

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(fr_tmja.DEFAULT_RESOURCE_URL).mock(
                return_value=httpx.Response(200, content=bad_identity_csv.encode())
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                records = [record async for record in result.records]

        assert records[0].source_record_id is None


class TestSiteMapper:
    def _map(self, **overrides: Any) -> Any:
        raw = _raw_fixture(**overrides)

        return fr_tmja.FrTmjaRoadSegmentSiteMapper()(_record(raw), _snapshot())

    def test_row_maps_to_road_segment_site(self) -> None:
        result = self._map()
        site = result.record

        assert site.site_id == "A0001:18875:20374:I"
        assert site.kind.value is MobilitySiteKind.ROAD_SEGMENT
        assert site.road_names.value == ("A0001",)
        assert site.address.value is not None
        assert site.address.value.country == "FR"
        assert site.direction.value is ObservationDirection.BIDIRECTIONAL
        assert site.active_period.value is not None
        assert site.active_period.value.precision is TemporalPeriodPrecision.YEAR
        assert site.active_period.value.year_value == 2024

    def test_site_id_ignores_pr_concession_and_abscissa_fields(self) -> None:
        result = self._map(
            prD="0019",
            depPrD="095",
            concessionPrD="N",
            absD="0.0",
            prF="0020",
            depPrF="095",
            concessionPrF="N",
            absF="499.0",
        )

        assert result.record.site_id == "A0001:18875:20374:I"

    def test_projected_coordinates_are_unmapped_and_visible_in_report(self) -> None:
        result = self._map()
        site = result.record

        assert site.footprint.value is None
        assert site.footprint.quality is FieldQuality.UNMAPPED
        assert site.footprint.source_fields == ()
        assert {"xD", "yD", "xF", "yF", "zD", "zF"}.issubset(result.report.unmapped_source_fields)
        assert site.source_caveats.value is not None
        assert any(
            caveat.code == fr_tmja.PROJECTED_COORDINATES_CAVEAT_CODE
            for caveat in site.source_caveats.value
        )

    def test_permanent_hourly_maps_to_automated_counter(self) -> None:
        result = self._map()

        assert result.record.measurement_method.value is MeasurementMethod.AUTOMATED_COUNTER
        assert result.record.measurement_method.quality is FieldQuality.STANDARDIZED

    def test_unknown_method_code_maps_to_other_inferred(self) -> None:
        result = self._map(typeComptageTrafic="9", typeComptageTrafic_lib="Autre")

        assert result.record.measurement_method.value is MeasurementMethod.OTHER
        assert result.record.measurement_method.quality is FieldQuality.INFERRED

    def test_missing_identity_field_fails_loudly(self) -> None:
        with pytest.raises(MappingError, match="missing required source field"):
            self._map(cumulD="")


class TestCountMapper:
    def _map(self, **overrides: Any) -> Any:
        raw = _raw_fixture(**overrides)

        return fr_tmja.FrTmjaCountMapper()(_record(raw), _snapshot())

    def test_tmja_row_maps_to_annual_average_daily_count(self) -> None:
        result = self._map()
        observation = result.record

        assert observation.observation_id == "A0001:18875:20374:I:2024:TMJA"
        assert observation.site_id == "A0001:18875:20374:I"
        assert observation.value.value == Decimal("96596")
        assert observation.value.quality is FieldQuality.DIRECT
        assert observation.travel_mode.value is TravelMode.VEHICLE
        assert observation.metric_type.value is CountMetricType.TMJA
        assert observation.unit.value is CountUnit.VEHICLES_PER_DAY
        assert observation.aggregation_window.value is AggregationWindow.ANNUAL_AVERAGE_DAILY
        assert observation.direction.value is ObservationDirection.BIDIRECTIONAL

    def test_period_matches_gb_annual_average_shape(self) -> None:
        result = self._map()
        period = result.record.period.value

        assert period is not None
        assert period.precision is TemporalPeriodPrecision.YEAR
        assert period.year_value == 2024
        assert period.timezone_status is TemporalTimezoneStatus.UNKNOWN
        assert period.start_datetime is None
        assert period.end_datetime is None

    def test_decimal_comma_tmja_value_is_accepted(self) -> None:
        result = self._map(TMJA="96596,5")

        assert result.record.value.value == Decimal("96596.5")

    def test_ratio_pl_is_left_raw_and_not_mapped(self) -> None:
        result = self._map()

        assert "ratio_PL" in result.report.unmapped_source_fields
        assert result.record.source_caveats.value is not None
        assert any(
            caveat.code == fr_tmja.RATIO_PL_RAW_CAVEAT_CODE
            for caveat in result.record.source_caveats.value
        )

    def test_observation_caveats_lock_source_semantics(self) -> None:
        result = self._map()
        caveat_codes = {caveat.code for caveat in result.record.source_caveats.value or ()}

        assert fr_tmja.ANNUAL_AVERAGE_CAVEAT_CODE in caveat_codes
        assert fr_tmja.BIDIRECTIONAL_CAVEAT_CODE in caveat_codes
        assert fr_tmja.RRNC_COVERAGE_CAVEAT_CODE in caveat_codes

    def test_invalid_year_fails_loudly(self) -> None:
        with pytest.raises(MappingError, match="invalid year"):
            self._map(anneeMesureTrafic="not-a-year")

    def test_negative_tmja_fails_loudly(self) -> None:
        with pytest.raises(MappingError, match="negative numeric"):
            self._map(TMJA="-1")

    def test_invalid_tmja_fails_loudly(self) -> None:
        with pytest.raises(MappingError, match="invalid numeric"):
            self._map(TMJA="not-a-number")

    def test_missing_identity_field_fails_loudly(self) -> None:
        with pytest.raises(MappingError, match="missing required source field"):
            self._map(route="")


class TestDrift:
    async def test_clean_fixture_has_no_drift(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(fr_tmja.DEFAULT_RESOURCE_URL).mock(
                return_value=httpx.Response(200, content=_csv_bytes())
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(
                    _adapter(client),
                    fr_tmja.FrTmjaCountMapper(),
                )
                schema_obs = SchemaObserver(spec=fr_tmja.FR_TMJA_SCHEMA)
                taxonomy_obs = TaxonomyObserver(specs=fr_tmja.FR_TMJA_TAXONOMIES)
                observed = attach_observers(pipeline_result, [schema_obs, taxonomy_obs])
                async for _ in observed.records:
                    pass

        assert schema_obs.finalize(pipeline_result.snapshot).findings == ()
        assert taxonomy_obs.finalize(pipeline_result.snapshot).findings == ()

    async def test_unknown_type_label_surfaces_as_taxonomy_drift(self) -> None:
        raw = _raw_fixture(typeComptageTrafic_lib="Comptage expérimental")
        pipeline_result = await run(
            _StaticAdapter((raw,)),
            fr_tmja.FrTmjaCountMapper(),
        )
        taxonomy_obs = TaxonomyObserver(specs=fr_tmja.FR_TMJA_TAXONOMIES)
        observed = attach_observers(pipeline_result, [taxonomy_obs])
        async for _ in observed.records:
            pass

        report = taxonomy_obs.finalize(pipeline_result.snapshot)

        assert any(
            finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE
            and finding.source_field == "typeComptageTrafic_lib"
            for finding in report.findings
        )

    async def test_unknown_concession_and_cote_surface_as_taxonomy_drift(self) -> None:
        raw = _raw_fixture(concessionPrD="X", concessionPrF="X", cote="Z")
        pipeline_result = await run(
            _StaticAdapter((raw,)),
            fr_tmja.FrTmjaCountMapper(),
        )
        taxonomy_obs = TaxonomyObserver(specs=fr_tmja.FR_TMJA_TAXONOMIES)
        observed = attach_observers(pipeline_result, [taxonomy_obs])
        async for _ in observed.records:
            pass

        report = taxonomy_obs.finalize(pipeline_result.snapshot)
        source_fields = {
            finding.source_field
            for finding in report.findings
            if finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE
        }

        assert {"concessionPrD", "concessionPrF", "cote"}.issubset(source_fields)

    async def test_missing_required_field_surfaces_as_schema_drift(self) -> None:
        raw = _raw_fixture()
        del raw["ratio_PL"]
        pipeline_result = await run(
            _StaticAdapter((raw,)),
            fr_tmja.FrTmjaCountMapper(),
        )
        schema_obs = SchemaObserver(spec=fr_tmja.FR_TMJA_SCHEMA)
        observed = attach_observers(pipeline_result, [schema_obs])
        async for _ in observed.records:
            pass

        report = schema_obs.finalize(pipeline_result.snapshot)

        assert any(
            finding.kind is SchemaDriftKind.MISSING_FIELD and finding.field_name == "ratio_PL"
            for finding in report.findings
        )


def test_source_metadata_preserves_scope_and_caveats() -> None:
    assert fr_tmja.SOURCE_ID == "fr-mte-road-traffic"
    assert fr_tmja.FR_TMJA_JURISDICTION.country == "FR"
    assert fr_tmja.FR_TMJA_RESOURCE_ID == "dbec4f42-b5fc-429f-b913-eeb758777383"
    assert "2024" in fr_tmja.FR_TMJA_SOURCE_SCOPE
    assert "follow-up" in fr_tmja.FR_TMJA_SOURCE_SCOPE
    assert any("licence ouverte" in caveat.casefold() for caveat in fr_tmja.FR_TMJA_RELEASE_CAVEATS)
    assert any("annual-average" in caveat.casefold() for caveat in fr_tmja.FR_TMJA_RELEASE_CAVEATS)
    assert any("concessioned" in caveat.casefold() for caveat in fr_tmja.FR_TMJA_RELEASE_CAVEATS)
    assert any("ratio_pl" in caveat.casefold() for caveat in fr_tmja.FR_TMJA_RELEASE_CAVEATS)
