"""Tests for the NYC LL84 source slice."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, date, datetime
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
from civix.core.identity.models.identifiers import SnapshotId
from civix.core.mapping.errors import MappingError
from civix.core.pipeline import attach_observers, run
from civix.core.ports.errors import FetchError
from civix.core.ports.models.adapter import SourceAdapter
from civix.core.quality.models.fields import FieldQuality
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.temporal import TemporalPeriodPrecision
from civix.domains.building_energy_emissions.adapters.sources.us.nyc_ll84 import (
    LL84_BASE_URL,
    LL84_DATASET_ID,
    LL84_DEFAULT_ORDER,
    LL84_OUT_FIELDS,
    LL84_RAW_SCHEMA,
    LL84_SOURCE_SCOPE,
    LL84_TAXONOMIES,
    NYC_JURISDICTION,
    SOURCE_ID,
    NycLl84Adapter,
    NycLl84FetchConfig,
    NycLl84MetricsMapper,
    NycLl84ReportMapper,
    NycLl84SubjectMapper,
)
from civix.domains.building_energy_emissions.models import (
    BuildingSubjectKind,
    EmissionsMetricType,
    EnergyMetricType,
    IdentityCertainty,
    MetricFamily,
    MetricValueSource,
    NumericMetricMeasure,
    ReportingPeriodPrecision,
    SourceValueState,
    WaterMetricType,
    build_building_energy_report_key,
    build_building_energy_subject_key,
)

PINNED_NOW = datetime(2026, 5, 3, 12, 0, tzinfo=UTC)
FIXTURES = Path(__file__).parent / "fixtures"


def _rows() -> list[dict[str, Any]]:
    return json.loads((FIXTURES / "ll84_query_response.json").read_text())


def _row(index: int = 0) -> dict[str, Any]:
    return dict(_rows()[index])


def _record(raw: dict[str, Any] | None = None) -> RawRecord:
    payload = _row() if raw is None else raw
    property_id = str(payload["property_id"])
    report_year = str(payload["report_year"])

    return RawRecord(
        snapshot_id=SnapshotId("snap-nyc-ll84"),
        raw_data=payload,
        source_record_id=f"{property_id}:{report_year}",
    )


def _snapshot() -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId("snap-nyc-ll84"),
        source_id=SOURCE_ID,
        dataset_id=LL84_DATASET_ID,
        jurisdiction=NYC_JURISDICTION,
        fetched_at=PINNED_NOW,
        record_count=len(_rows()),
        source_url=LL84_BASE_URL,
    )


def _adapter(client: httpx.AsyncClient, *, page_size: int = 1000) -> NycLl84Adapter:
    return NycLl84Adapter(
        fetch_config=NycLl84FetchConfig(
            client=client,
            clock=lambda: PINNED_NOW,
            page_size=page_size,
            where="report_year=2024",
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
    async def test_fetches_paginated_rows_with_socrata_params(self) -> None:
        rows = _rows()
        requests: list[httpx.Request] = []

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(LL84_BASE_URL).mock(
                side_effect=_capture_sequence(
                    requests,
                    [
                        httpx.Response(200, json=[{"count": str(len(rows))}]),
                        httpx.Response(200, json=rows),
                    ],
                )
            )

            async with httpx.AsyncClient() as client:
                adapter = _adapter(client)
                result = await adapter.fetch()
                records = [record async for record in result.records]

        assert isinstance(adapter, SourceAdapter)
        assert result.snapshot.source_id == SOURCE_ID
        assert result.snapshot.dataset_id == LL84_DATASET_ID
        assert result.snapshot.jurisdiction == NYC_JURISDICTION
        assert result.snapshot.source_url == LL84_BASE_URL
        assert result.snapshot.record_count == len(rows)
        assert result.snapshot.fetch_params is not None
        assert result.snapshot.fetch_params["$where"] == "report_year=2024"
        assert result.snapshot.fetch_params["$order"] == LL84_DEFAULT_ORDER
        assert "$select" in result.snapshot.fetch_params

        count_request, page_request = requests
        assert count_request.url.params["$select"] == "count(*)"
        assert count_request.url.params["$where"] == "report_year=2024"
        assert page_request.url.params["$where"] == "report_year=2024"
        assert page_request.url.params["$order"] == LL84_DEFAULT_ORDER
        assert page_request.url.params["$limit"] == "1000"
        assert page_request.url.params["$offset"] == "0"
        assert "property_id" in page_request.url.params["$select"]

        assert [record.source_record_id for record in records] == [
            "8139:2024",
            "28400:2024",
            "66135792:2024",
        ]

    async def test_strips_socrata_computed_region_transport_fields(self) -> None:
        rows = _rows()
        rows[0][":@computed_region_efsh_h5xi"] = "10118"

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(LL84_BASE_URL).mock(
                side_effect=[
                    httpx.Response(200, json=[{"count": str(len(rows))}]),
                    httpx.Response(200, json=rows),
                ]
            )

            async with httpx.AsyncClient() as client:
                fetch_result = await _adapter(client).fetch()
                records = [record async for record in fetch_result.records]

        assert all(
            not name.startswith(":@computed_region_")
            for record in records
            for name in record.raw_data
        )

    async def test_count_failure_raises_fetch_error(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(LL84_BASE_URL).mock(return_value=httpx.Response(500, text="oops"))

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError) as exc_info:
                    await _adapter(client).fetch()

        assert exc_info.value.operation == "count"
        assert exc_info.value.dataset_id == LL84_DATASET_ID


class TestSubjectMapper:
    def test_standalone_property_emits_reporting_account_subject(self) -> None:
        result = NycLl84SubjectMapper()(_record(_row(0)), _snapshot())
        subject = result.record

        assert subject.subject_key == build_building_energy_subject_key(
            SOURCE_ID, LL84_DATASET_ID, "8139"
        )
        assert subject.subject_kind.value is BuildingSubjectKind.REPORTING_ACCOUNT
        assert subject.identity_certainty.value is IdentityCertainty.STABLE_CROSS_YEAR
        assert subject.parent_subject_key.quality is FieldQuality.UNMAPPED
        assert subject.parent_subject_key.value is None

        identifiers = subject.source_subject_identifiers.value or ()
        identifier_values = [identifier.value for identifier in identifiers]
        assert identifier_values == ["8139", "1009990001", "1011223"]

        property_types = subject.property_types.value or ()
        assert [category.label for category in property_types] == ["Office"]
        assert subject.floor_area.value == Decimal("2768591")
        assert subject.year_built.value == 1931

    def test_multi_building_campus_splits_identifiers_and_links_parent(self) -> None:
        result = NycLl84SubjectMapper()(_record(_row(1)), _snapshot())
        subject = result.record

        identifiers = subject.source_subject_identifiers.value or ()
        bbl_values = [
            identifier.value
            for identifier in identifiers
            if identifier.identifier_kind is not None and identifier.identifier_kind.code == "bbl"
        ]
        bin_values = [
            identifier.value
            for identifier in identifiers
            if identifier.identifier_kind is not None and identifier.identifier_kind.code == "bin"
        ]
        assert bbl_values == ["2034560010", "2034560020", "2034560030"]
        assert bin_values == ["2050010", "2050011", "2050012"]

        parent_key = subject.parent_subject_key.value
        assert parent_key is not None
        assert parent_key != subject.subject_key
        assert parent_key == build_building_energy_subject_key(SOURCE_ID, LL84_DATASET_ID, "28401")
        assert subject.name.quality is FieldQuality.NOT_PROVIDED
        assert subject.name.value is None

        property_type_labels = [category.label for category in (subject.property_types.value or ())]
        assert property_type_labels == ["Multifamily Housing", "Retail Store", "Other"]

    def test_classified_address_marks_redacted_without_value(self) -> None:
        result = NycLl84SubjectMapper()(_record(_row(2)), _snapshot())
        subject = result.record

        assert subject.address.quality is FieldQuality.REDACTED
        assert subject.address.value is None

    def test_missing_property_id_raises_mapper_scoped_error(self) -> None:
        raw = _row(0)
        raw["property_id"] = None

        with pytest.raises(MappingError) as exc_info:
            NycLl84SubjectMapper()(_record(raw), _snapshot())

        assert exc_info.value.source_fields == ("property_id",)


class TestReportMapper:
    def test_report_key_is_deterministic_from_property_id_and_year(self) -> None:
        result = NycLl84ReportMapper()(_record(_row(0)), _snapshot())
        report = result.record

        assert report.report_key == build_building_energy_report_key(
            SOURCE_ID, LL84_DATASET_ID, "8139", "2024"
        )
        assert report.subject_key == build_building_energy_subject_key(
            SOURCE_ID, LL84_DATASET_ID, "8139"
        )
        assert report.reporting_period.value is not None
        assert report.reporting_period.value.precision is TemporalPeriodPrecision.YEAR
        assert report.reporting_period.value.year_value == 2024
        assert report.reporting_period_precision.value is ReportingPeriodPrecision.CALENDAR_YEAR
        assert report.report_submission_date.value == date(2025, 5, 1)
        assert report.report_generation_date.value == date(2025, 4, 30)

    def test_data_quality_caveats_include_possible_issue_flag(self) -> None:
        report = NycLl84ReportMapper()(_record(_row(2)), _snapshot()).record
        caveats = report.data_quality_caveats.value or ()

        labels = {category.label for category in caveats}
        assert "Data Quality Checker Flagged Possible Issue" in labels
        assert any("Electric Meter Alert" in label for label in labels)


class TestMetricsMapper:
    def test_complete_row_emits_one_metric_per_field_with_source_republished(self) -> None:
        result = NycLl84MetricsMapper()(_record(_row(0)), _snapshot())
        metrics = result.record

        assert len(metrics) == 11
        assert all(
            metric.value_source.value is MetricValueSource.SOURCE_REPUBLISHED for metric in metrics
        )
        assert all(
            metric.report_key.value is not None
            and metric.report_key.value
            == build_building_energy_report_key(SOURCE_ID, LL84_DATASET_ID, "8139", "2024")
            for metric in metrics
        )
        assert all(metric.case_key.quality is FieldQuality.UNMAPPED for metric in metrics)
        assert all(metric.value_state.value is SourceValueState.REPORTED for metric in metrics)

        site_eui = next(
            metric
            for metric in metrics
            if metric.energy_metric_type.value is EnergyMetricType.SITE_EUI
        )
        assert isinstance(site_eui.measure.value, NumericMetricMeasure)
        assert site_eui.measure.value.value == Decimal("92.4")
        assert site_eui.unit.value is not None
        assert site_eui.unit.value.code == "kbtu-per-ft2"

        total_ghg = next(
            metric
            for metric in metrics
            if metric.emissions_metric_type.value is EmissionsMetricType.LOCATION_BASED_GHG
        )
        assert total_ghg.metric_family is MetricFamily.EMISSIONS
        assert isinstance(total_ghg.measure.value, NumericMetricMeasure)
        assert total_ghg.measure.value.value == Decimal("21450.7")

        water = next(
            metric
            for metric in metrics
            if metric.water_metric_type.value is WaterMetricType.WATER_USE
        )
        assert water.metric_family is MetricFamily.WATER

    def test_sentinel_values_preserve_distinct_value_states(self) -> None:
        metrics = NycLl84MetricsMapper()(_record(_row(2)), _snapshot()).record
        by_label = {
            metric.source_metric_label.value.code: metric  # type: ignore[union-attr]
            for metric in metrics
        }

        not_available = by_label["site-eui-kbtu-ft2"]
        unable_to_check = by_label["weather-normalized-site-eui-kbtu-ft2"]
        not_applicable = by_label["natural-gas-use-kbtu"]
        blank = by_label["direct-ghg-emissions-metric-tons-co2e"]

        assert not_available.value_state.value is SourceValueState.NOT_AVAILABLE
        assert not_available.measure.value is None
        assert not_available.measure.quality is FieldQuality.NOT_PROVIDED

        assert unable_to_check.value_state.value is SourceValueState.UNABLE_TO_CHECK
        assert not_applicable.value_state.value is SourceValueState.NOT_APPLICABLE
        assert blank.value_state.value is SourceValueState.NOT_AVAILABLE
        assert blank.measure.quality is FieldQuality.NOT_PROVIDED

    def test_invalid_decimal_raises_mapping_error_with_source_field(self) -> None:
        raw = _row(0)
        raw["site_eui_kbtu_ft2"] = "92.4abc"

        with pytest.raises(MappingError) as exc_info:
            NycLl84MetricsMapper()(_record(raw), _snapshot())

        assert exc_info.value.source_fields == ("site_eui_kbtu_ft2",)


class TestPipelineDrift:
    async def test_fixture_drift_clean(self) -> None:
        rows = _rows()

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(LL84_BASE_URL).mock(
                side_effect=[
                    httpx.Response(200, json=[{"count": str(len(rows))}]),
                    httpx.Response(200, json=rows),
                ]
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(_adapter(client), NycLl84SubjectMapper())
                schema_obs = SchemaObserver(spec=LL84_RAW_SCHEMA)
                taxonomy_obs = TaxonomyObserver(specs=LL84_TAXONOMIES)
                observed = attach_observers(pipeline_result, [schema_obs, taxonomy_obs])
                consumed = [record async for record in observed.records]

        assert len(consumed) == len(rows)
        assert schema_obs.finalize(pipeline_result.snapshot).findings == ()
        assert taxonomy_obs.finalize(pipeline_result.snapshot).findings == ()

    async def test_unknown_borough_surfaces_taxonomy_drift(self) -> None:
        rows = _rows()
        rows[0]["borough"] = "Mars"

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(LL84_BASE_URL).mock(
                side_effect=[
                    httpx.Response(200, json=[{"count": str(len(rows))}]),
                    httpx.Response(200, json=rows),
                ]
            )

            async with httpx.AsyncClient() as client:
                fetch_result = await _adapter(client).fetch()
                taxonomy_obs = TaxonomyObserver(specs=LL84_TAXONOMIES)
                async for record in fetch_result.records:
                    taxonomy_obs.observe(record)

        report = taxonomy_obs.finalize(fetch_result.snapshot)
        assert any(
            finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE
            and finding.taxonomy_id == "nyc-ll84-borough"
            for finding in report.findings
        )

    async def test_missing_property_id_surfaces_schema_drift(self) -> None:
        rows = _rows()
        del rows[0]["property_id"]

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(LL84_BASE_URL).mock(
                side_effect=[
                    httpx.Response(200, json=[{"count": str(len(rows))}]),
                    httpx.Response(200, json=rows),
                ]
            )

            async with httpx.AsyncClient() as client:
                fetch_result = await _adapter(client).fetch()
                schema_obs = SchemaObserver(spec=LL84_RAW_SCHEMA)
                async for record in fetch_result.records:
                    schema_obs.observe(record)

        report = schema_obs.finalize(fetch_result.snapshot)
        assert any(
            finding.kind is SchemaDriftKind.MISSING_FIELD and finding.field_name == "property_id"
            for finding in report.findings
        )


def test_source_metadata_preserves_scope_and_field_layout() -> None:
    assert SOURCE_ID == "nyc-open-data"
    assert LL84_DATASET_ID == "5zyy-y8am"
    assert "Local Law 84" in LL84_SOURCE_SCOPE
    assert "property_id" in LL84_OUT_FIELDS
    assert "report_year" in LL84_OUT_FIELDS
