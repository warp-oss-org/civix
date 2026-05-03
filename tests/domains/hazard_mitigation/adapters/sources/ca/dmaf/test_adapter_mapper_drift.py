"""Tests for the Canada DMAF source slice."""

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

from civix.core.drift import SchemaObserver, TaxonomyDriftKind, TaxonomyObserver
from civix.core.identity.models.identifiers import SnapshotId
from civix.core.pipeline import attach_observers, run
from civix.core.ports.errors import FetchError
from civix.core.ports.models.adapter import SourceAdapter
from civix.core.quality.models.fields import FieldQuality
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.temporal import TemporalPeriodPrecision
from civix.domains.hazard_mitigation.adapters.sources.ca.dmaf import (
    CA_JURISDICTION,
    CANADA_DMAF_PROJECTS_DATASET_ID,
    CANADA_DMAF_PROJECTS_RESOURCE_FORMAT,
    CANADA_DMAF_PROJECTS_RESOURCE_NAME,
    CANADA_DMAF_PROJECTS_SCHEMA,
    CANADA_DMAF_PROJECTS_TAXONOMIES,
    CANADA_DMAF_ROW_FILTER,
    CANADA_DMAF_SOURCE_SCOPE,
    OPEN_CANADA_CKAN_BASE_URL,
    SOURCE_ID,
    CanadaDmafCaveat,
    CanadaDmafProjectMapper,
    CanadaDmafProjectsAdapter,
)
from civix.domains.hazard_mitigation.models.common import (
    MitigationFundingAmountKind,
    MitigationFundingEventType,
    MitigationFundingShareKind,
    MitigationInterventionType,
)
from civix.infra.sources.ckan import CkanFetchConfig

PINNED_NOW = datetime(2026, 5, 3, 12, 0, tzinfo=UTC)
FIXTURES = Path(__file__).parent / "fixtures"
PACKAGE_SHOW_URL = f"{OPEN_CANADA_CKAN_BASE_URL}package_show"
STATIC_RESOURCE_URL = "https://www.infrastructure.gc.ca/alt-format/opendata/project-list.json"
RESOURCE_ID = "project-list-json"


def _project_list_payload() -> dict[str, Any]:
    return json.loads((FIXTURES / "project_list.json").read_text())


def _package_payload() -> dict[str, Any]:
    return {
        "success": True,
        "result": {
            "resources": [
                {
                    "id": "project-list-csv",
                    "name": CANADA_DMAF_PROJECTS_RESOURCE_NAME,
                    "format": "CSV",
                    "language": ["en", "fr"],
                    "url": "https://www.infrastructure.gc.ca/alt-format/opendata/project-list.csv",
                },
                {
                    "id": RESOURCE_ID,
                    "name": CANADA_DMAF_PROJECTS_RESOURCE_NAME,
                    "format": CANADA_DMAF_PROJECTS_RESOURCE_FORMAT,
                    "language": ["en", "fr"],
                    "url": STATIC_RESOURCE_URL,
                },
            ]
        },
    }


def _fetch(client: httpx.AsyncClient) -> CkanFetchConfig:
    return CkanFetchConfig(client=client, clock=lambda: PINNED_NOW)


def _adapter(client: httpx.AsyncClient) -> CanadaDmafProjectsAdapter:
    return CanadaDmafProjectsAdapter(fetch_config=_fetch(client))


def _snapshot() -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId("canada-dmaf-snapshot"),
        source_id=SOURCE_ID,
        dataset_id=CANADA_DMAF_PROJECTS_DATASET_ID,
        jurisdiction=CA_JURISDICTION,
        fetched_at=PINNED_NOW,
        record_count=1,
    )


def _record(raw: dict[str, Any], source_record_id: str) -> RawRecord:
    return RawRecord(
        snapshot_id=SnapshotId("canada-dmaf-snapshot"),
        raw_data=raw,
        source_record_id=source_record_id,
    )


def _rows(payload: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    source = _project_list_payload() if payload is None else payload
    headers = source["indexTitles"]

    return [dict(zip(headers, row, strict=True)) for row in source["data"]]


def _capture_sequence(
    requests: list[httpx.Request],
    responses: list[httpx.Response],
) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)

        return responses.pop(0)

    return handler


class TestAdapter:
    async def test_fetches_static_project_list_and_drops_non_dmaf_rows(self) -> None:
        requests: list[httpx.Request] = []

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                side_effect=_capture_sequence(
                    requests,
                    [httpx.Response(200, json=_package_payload())],
                )
            )
            respx_mock.get(STATIC_RESOURCE_URL).mock(
                return_value=httpx.Response(200, json=_project_list_payload())
            )

            async with httpx.AsyncClient() as client:
                adapter = _adapter(client)
                result = await adapter.fetch()
                records = [record async for record in result.records]

        assert isinstance(adapter, SourceAdapter)
        assert requests[0].url.params["id"] == str(CANADA_DMAF_PROJECTS_DATASET_ID)
        assert result.snapshot.source_id == SOURCE_ID
        assert result.snapshot.dataset_id == CANADA_DMAF_PROJECTS_DATASET_ID
        assert result.snapshot.record_count == 2
        assert result.snapshot.source_url == STATIC_RESOURCE_URL
        assert result.snapshot.fetch_params == {
            "package_id": str(CANADA_DMAF_PROJECTS_DATASET_ID),
            "resource_id": RESOURCE_ID,
            "resource_name": CANADA_DMAF_PROJECTS_RESOURCE_NAME,
            "resource_format": CANADA_DMAF_PROJECTS_RESOURCE_FORMAT,
            "source_total_records": "3",
            "row_filter": CANADA_DMAF_ROW_FILTER,
        }
        assert [record.source_record_id for record in records] == ["DMAF-001", "DMAF-002"]
        assert all(record.raw_data["programCode_en"] == "DMAF" for record in records)

    async def test_malformed_static_project_list_raises_fetch_error(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(STATIC_RESOURCE_URL).mock(
                return_value=httpx.Response(200, json={"data": []})
            )

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="indexTitles"):
                    await _adapter(client).fetch()

    async def test_duplicate_static_headers_raise_fetch_error(self) -> None:
        payload = _project_list_payload()
        payload["indexTitles"][1] = "projectNumber"

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(STATIC_RESOURCE_URL).mock(return_value=httpx.Response(200, json=payload))

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="duplicate"):
                    await _adapter(client).fetch()

    async def test_wrong_width_static_row_raises_fetch_error(self) -> None:
        payload = _project_list_payload()
        payload["data"][0] = payload["data"][0][:-1]

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(STATIC_RESOURCE_URL).mock(return_value=httpx.Response(200, json=payload))

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="width"):
                    await _adapter(client).fetch()


class TestMapper:
    def test_maps_complete_dmaf_project(self) -> None:
        result = CanadaDmafProjectMapper()(_record(_rows()[0], "DMAF-001"), _snapshot())
        project = result.record

        assert project.project_id == "DMAF-001"
        assert project.title.value == "Flood mitigation upgrades for Riverside"
        assert project.description.quality is FieldQuality.UNMAPPED
        assert project.programme.value is not None
        assert project.programme.value.code == "dmaf"
        assert project.programme.value.label == "Disaster Mitigation and Adaptation Fund"
        assert project.organizations.value is not None
        assert project.organizations.value[0].name == "Riverside, Town of"
        assert project.hazard_types.quality is FieldQuality.UNMAPPED
        assert project.source_hazards.quality is FieldQuality.UNMAPPED
        assert project.status.value is None
        assert project.status.quality is FieldQuality.UNMAPPED
        assert project.source_status.value is None
        assert project.source_status.quality is FieldQuality.UNMAPPED
        assert project.intervention_types.value == (MitigationInterventionType.SOURCE_SPECIFIC,)
        assert project.source_interventions.value is not None
        assert project.source_interventions.value[0].label == "Disaster Mitigation"

    def test_maps_geography_dates_and_funding_summaries(self) -> None:
        project = CanadaDmafProjectMapper()(_record(_rows()[0], "DMAF-001"), _snapshot()).record

        assert project.approval_period.value is not None
        assert project.approval_period.value.date_value == date(2024, 1, 15)
        assert project.project_period.value is not None
        assert project.project_period.value.precision is TemporalPeriodPrecision.INTERVAL
        assert project.project_period.value.start_datetime == datetime(2024, 3, 1)
        assert project.project_period.value.end_datetime == datetime(2025, 9, 30)
        assert project.geography.value is not None
        assert project.geography.value[0].place_name == "Riverside"
        assert project.geography.value[0].address is not None
        assert project.geography.value[0].address.region == "NB"
        assert project.funding_summaries.value is not None
        assert [
            (component.amount_kind, component.share_kind, component.lifecycle)
            for component in project.funding_summaries.value
        ] == [
            (
                MitigationFundingAmountKind.PROJECT_AMOUNT,
                MitigationFundingShareKind.FEDERAL,
                MitigationFundingEventType.AWARD,
            ),
            (
                MitigationFundingAmountKind.TOTAL_ELIGIBLE_COST,
                MitigationFundingShareKind.TOTAL,
                None,
            ),
        ]
        assert project.funding_summaries.value[0].money.amount == Decimal("5000000")
        assert project.funding_summaries.value[0].money.currency == "CAD"

    def test_forecast_dates_are_caveats_not_actual_project_period(self) -> None:
        project = CanadaDmafProjectMapper()(_record(_rows()[1], "DMAF-002"), _snapshot()).record

        assert project.project_period.quality is FieldQuality.UNMAPPED
        assert project.source_caveats.value is not None
        assert {caveat.code for caveat in project.source_caveats.value} == {
            "forecast-construction-dates",
            "total-eligible-cost-lifecycle",
        }

    def test_unmapped_french_fields_and_project_number_reporting(self) -> None:
        report = CanadaDmafProjectMapper()(_record(_rows()[0], "DMAF-001"), _snapshot()).report

        assert "projectNumber" not in report.unmapped_source_fields
        assert "projectTitle_fr" in report.unmapped_source_fields
        assert "programCode_fr" in report.unmapped_source_fields
        assert "program_fr" in report.unmapped_source_fields
        assert "ultimateRecipient_fr" in report.unmapped_source_fields
        assert "location_fr" in report.unmapped_source_fields
        assert "category_fr" in report.unmapped_source_fields
        assert "forecastedConstructionStartDate" not in report.unmapped_source_fields
        assert "forecastedConstructionEndDate" not in report.unmapped_source_fields

class TestDrift:
    async def test_dmaf_fixture_drift_clean(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(STATIC_RESOURCE_URL).mock(
                return_value=httpx.Response(200, json=_project_list_payload())
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(_adapter(client), CanadaDmafProjectMapper())
                schema_obs = SchemaObserver(spec=CANADA_DMAF_PROJECTS_SCHEMA)
                taxonomy_obs = TaxonomyObserver(specs=CANADA_DMAF_PROJECTS_TAXONOMIES)
                observed = attach_observers(pipeline_result, [schema_obs, taxonomy_obs])
                records = [record async for record in observed.records]

        assert len(records) == 2
        assert schema_obs.finalize(pipeline_result.snapshot).findings == ()
        assert taxonomy_obs.finalize(pipeline_result.snapshot).findings == ()

    async def test_unknown_category_and_region_surface_as_taxonomy_drift(self) -> None:
        payload = _project_list_payload()
        headers = payload["indexTitles"]
        category_index = headers.index("category_en")
        region_index = headers.index("region")
        payload["data"][0][category_index] = "New Category"
        payload["data"][0][region_index] = "yt"

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(STATIC_RESOURCE_URL).mock(return_value=httpx.Response(200, json=payload))

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(_adapter(client), CanadaDmafProjectMapper())
                taxonomy_obs = TaxonomyObserver(specs=CANADA_DMAF_PROJECTS_TAXONOMIES)
                observed = attach_observers(pipeline_result, [taxonomy_obs])
                async for _ in observed.records:
                    pass

        report = taxonomy_obs.finalize(pipeline_result.snapshot)

        assert any(
            finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE
            and finding.taxonomy_id == "canada-dmaf-category"
            for finding in report.findings
        )
        assert any(
            finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE
            and finding.taxonomy_id == "canada-dmaf-region"
            for finding in report.findings
        )


def test_source_metadata_preserves_scope_and_ids() -> None:
    assert SOURCE_ID == "infrastructure-canada"
    assert CANADA_DMAF_PROJECTS_DATASET_ID == "beee0771-dab9-4be8-9b80-f8e8b3fdfd9d"
    assert "Disaster Mitigation and Adaptation Fund" in CANADA_DMAF_SOURCE_SCOPE
    assert CanadaDmafCaveat.FORECAST_CONSTRUCTION_DATES.value
