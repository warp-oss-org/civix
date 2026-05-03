"""Tests for the FEMA HMA source slice."""

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
from civix.core.identity.models.identifiers import DatasetId, SnapshotId
from civix.core.pipeline import attach_observers, run
from civix.core.ports.models.adapter import SourceAdapter
from civix.core.quality.models.fields import FieldQuality
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.temporal import TemporalPeriodPrecision
from civix.domains.hazard_mitigation.adapters.sources.us.fema_hma import (
    FEMA_HMA_PROJECTS_DATASET_ID,
    FEMA_HMA_PROJECTS_ORDER,
    FEMA_HMA_PROJECTS_SCHEMA,
    FEMA_HMA_PROJECTS_TAXONOMIES,
    FEMA_HMA_PROJECTS_VERSION,
    FEMA_HMA_SOURCE_SCOPE,
    FEMA_HMA_TRANSACTIONS_DATASET_ID,
    FEMA_HMA_TRANSACTIONS_ORDER,
    FEMA_HMA_TRANSACTIONS_SCHEMA,
    FEMA_HMA_TRANSACTIONS_TAXONOMIES,
    FEMA_HMA_TRANSACTIONS_VERSION,
    SOURCE_ID,
    US_JURISDICTION,
    FemaHmaProjectMapper,
    FemaHmaProjectsAdapter,
    FemaHmaTransactionMapper,
    FemaHmaTransactionsAdapter,
    OpenFemaHmaCaveat,
    observed_openfema_hma_metadata_caveats,
)
from civix.domains.hazard_mitigation.models.common import (
    MitigationFundingAmountKind,
    MitigationFundingShareKind,
    MitigationInterventionType,
    MitigationProjectStatus,
)
from civix.infra.sources.openfema import DEFAULT_BASE_URL, OpenFemaFetchConfig

PINNED_NOW = datetime(2026, 5, 2, 12, 0, tzinfo=UTC)
FIXTURES = Path(__file__).parent / "fixtures"
PROJECTS_URL = f"{DEFAULT_BASE_URL}{FEMA_HMA_PROJECTS_VERSION}/{FEMA_HMA_PROJECTS_DATASET_ID}"
TRANSACTIONS_URL = (
    f"{DEFAULT_BASE_URL}{FEMA_HMA_TRANSACTIONS_VERSION}/{FEMA_HMA_TRANSACTIONS_DATASET_ID}"
)
OPENFEMA_HMA_DESCRIPTION = (
    "This dataset contains information on the HMA subgrants. "
    "Sensitive information, such as Personally Identifiable Information (PII), has been "
    "removed to protect privacy. "
    "This dataset comes from the source system mentioned above and is subject to a small "
    "percentage of human error. "
    "In some cases, data was not provided by the subapplicant, applicant, and/or entered "
    "into NEMIS Mitigation and eGrants. "
    "The financial information in this dataset is not derived from FEMA's official financial "
    "systems. "
    "Due to differences in reporting periods, status of obligations, and how business rules "
    "are applied, this financial information may differ slightly from official publication "
    "on public websites such as https://www.usaspending.gov. "
    "This dataset is not intended to be used for any official federal financial reporting. "
    "FEMA's terms and conditions and citation requirements follow."
)


def _projects() -> list[dict[str, Any]]:
    return json.loads((FIXTURES / "projects_page.json").read_text())


def _transactions() -> list[dict[str, Any]]:
    return json.loads((FIXTURES / "transactions_page.json").read_text())


def _payload(entity: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {"metadata": {"count": len(rows)}, entity: rows}


def _project_adapter(client: httpx.AsyncClient, *, page_size: int = 1000) -> FemaHmaProjectsAdapter:
    return FemaHmaProjectsAdapter(
        fetch_config=OpenFemaFetchConfig(
            client=client,
            clock=lambda: PINNED_NOW,
            page_size=page_size,
            order_by=FEMA_HMA_PROJECTS_ORDER,
        )
    )


def _transaction_adapter(
    client: httpx.AsyncClient,
    *,
    page_size: int = 1000,
) -> FemaHmaTransactionsAdapter:
    return FemaHmaTransactionsAdapter(
        fetch_config=OpenFemaFetchConfig(
            client=client,
            clock=lambda: PINNED_NOW,
            page_size=page_size,
            order_by=FEMA_HMA_TRANSACTIONS_ORDER,
        )
    )


def _snapshot(dataset_id: DatasetId = FEMA_HMA_PROJECTS_DATASET_ID) -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId(f"snap-{dataset_id}"),
        source_id=SOURCE_ID,
        dataset_id=dataset_id,
        jurisdiction=US_JURISDICTION,
        fetched_at=PINNED_NOW,
        record_count=1,
    )


def _record(raw: dict[str, Any], source_record_id: str) -> RawRecord:
    return RawRecord(
        snapshot_id=SnapshotId("snap-hma"),
        raw_data=raw,
        source_record_id=source_record_id,
    )


def _capture_sequence(
    requests: list[httpx.Request],
    responses: list[httpx.Response],
) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)

        return responses.pop(0)

    return handler


class TestAdapters:
    async def test_project_adapter_fetches_traceable_rows(self) -> None:
        requests: list[httpx.Request] = []

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PROJECTS_URL).mock(
                side_effect=_capture_sequence(
                    requests,
                    [
                        httpx.Response(
                            200,
                            json=_payload(str(FEMA_HMA_PROJECTS_DATASET_ID), _projects()),
                        )
                    ],
                )
            )

            async with httpx.AsyncClient() as client:
                adapter = _project_adapter(client)
                result = await adapter.fetch()
                records = [record async for record in result.records]

        assert isinstance(adapter, SourceAdapter)
        assert result.snapshot.source_id == SOURCE_ID
        assert result.snapshot.dataset_id == FEMA_HMA_PROJECTS_DATASET_ID
        assert result.snapshot.fetch_params == {
            "$top": "1000",
            "$orderby": FEMA_HMA_PROJECTS_ORDER,
        }
        assert requests[0].url.params["$count"] == "true"
        assert [record.source_record_id for record in records] == [
            "DR-0820-0001-R",
            "FMA-PJ-10-WA-2017-006",
        ]

    async def test_transaction_adapter_fetches_composite_record_ids(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(TRANSACTIONS_URL).mock(
                return_value=httpx.Response(
                    200,
                    json=_payload(str(FEMA_HMA_TRANSACTIONS_DATASET_ID), _transactions()),
                )
            )

            async with httpx.AsyncClient() as client:
                result = await _transaction_adapter(client).fetch()
                records = [record async for record in result.records]

        assert [record.source_record_id for record in records] == [
            "DR-0820-0001-R:0",
            "DR-4332-0300-R:2",
        ]


class TestProjectMapper:
    def test_maps_fema_project_without_fabricated_title_or_transaction(self) -> None:
        result = FemaHmaProjectMapper()(
            _record(_projects()[0], "DR-0820-0001-R"),
            _snapshot(),
        )
        project = result.record

        assert project.project_id == "DR-0820-0001-R"
        assert project.title.value is None
        assert project.title.quality is FieldQuality.UNMAPPED
        assert project.description.quality is FieldQuality.UNMAPPED
        assert project.status.value is MitigationProjectStatus.CLOSED
        assert project.hazard_types.quality is FieldQuality.UNMAPPED
        assert project.intervention_types.value == (
            MitigationInterventionType.PROPERTY_ACQUISITION,
        )
        assert project.source_interventions.value is not None
        assert project.organizations.value is not None
        assert project.organizations.value[0].name == "ST. GEORGE"
        assert project.funding_summaries.value is not None
        assert len(project.funding_summaries.value) == 2
        assert project.net_benefits.value is not None
        assert project.net_benefits.value.amount == Decimal("212000")
        assert project.net_benefits.value.currency == "USD"
        assert "id" in result.report.unmapped_source_fields

    def test_maps_project_dates_to_explicit_periods(self) -> None:
        project = FemaHmaProjectMapper()(
            _record(_projects()[0], "DR-0820-0001-R"),
            _snapshot(),
        ).record

        assert project.approval_period.value is not None
        assert project.approval_period.value.date_value == date(1990, 7, 25)
        assert project.fiscal_period.value is not None
        assert project.fiscal_period.value.year_value == 1989
        assert project.project_period.value is not None
        assert project.project_period.value.precision is TemporalPeriodPrecision.INTERVAL
        assert project.publication_period.quality is FieldQuality.UNMAPPED
        assert project.source_caveats.quality is FieldQuality.STANDARDIZED
        assert project.source_caveats.value is not None
        assert any(
            caveat.code == "initial-approval-date-published"
            for caveat in project.source_caveats.value
        )

    def test_open_project_without_closed_date_leaves_project_period_unmapped(self) -> None:
        project = FemaHmaProjectMapper()(
            _record(_projects()[1], "FMA-PJ-10-WA-2017-006"),
            _snapshot(),
        ).record

        assert project.project_period.quality is FieldQuality.UNMAPPED
        assert project.source_caveats.value is not None
        assert any(
            caveat.code == "initial-obligation-date-published"
            for caveat in project.source_caveats.value
        )

    def test_project_funding_summary_components_preserve_amount_semantics(self) -> None:
        project = FemaHmaProjectMapper()(
            _record(_projects()[1], "FMA-PJ-10-WA-2017-006"),
            _snapshot(),
        ).record

        assert project.funding_summaries.value is not None
        kinds = [
            (component.amount_kind, component.share_kind)
            for component in project.funding_summaries.value
        ]
        assert (
            MitigationFundingAmountKind.PROJECT_AMOUNT,
            MitigationFundingShareKind.FEDERAL,
        ) in kinds
        assert (
            MitigationFundingAmountKind.ADMINISTRATIVE_COST,
            MitigationFundingShareKind.RECIPIENT,
        ) in kinds
        admin_component = next(
            component
            for component in project.funding_summaries.value
            if component.source_category is not None
            and component.source_category.code == "recipientadmincostamt"
        )
        assert admin_component.lifecycle is None

    def test_obligated_status_stays_source_specific(self) -> None:
        raw = _projects()[1]
        raw["status"] = "Obligated"

        project = FemaHmaProjectMapper()(
            _record(raw, "FMA-PJ-10-WA-2017-006"),
            _snapshot(),
        ).record

        assert project.status.value is MitigationProjectStatus.SOURCE_SPECIFIC

    def test_multi_county_geography_emits_one_county_record_each_without_region(self) -> None:
        raw = _projects()[0]
        raw["projectCounties"] = "WASHINGTON;IRON"
        result = FemaHmaProjectMapper()(_record(raw, "DR-0820-0001-R"), _snapshot())

        assert result.record.geography.value is not None
        assert [geography.place_name for geography in result.record.geography.value] == [
            "WASHINGTON",
            "IRON",
        ]
        assert "region" in result.report.unmapped_source_fields


class TestTransactionMapper:
    def test_maps_transaction_project_join_and_fund_code(self) -> None:
        result = FemaHmaTransactionMapper()(
            _record(_transactions()[0], "DR-0820-0001-R:0"),
            _snapshot(FEMA_HMA_TRANSACTIONS_DATASET_ID),
        )
        transaction = result.record

        assert transaction.transaction_id == "DR-0820-0001-R:0"
        assert transaction.project_id.value == "DR-0820-0001-R"
        assert transaction.transaction_period.value is not None
        assert transaction.transaction_period.value.date_value == date(1990, 7, 25)
        assert transaction.funding_programme.value is not None
        assert transaction.funding_programme.value.code == "6n"
        assert transaction.fiscal_period.quality is FieldQuality.UNMAPPED
        assert transaction.event_type.quality is FieldQuality.UNMAPPED
        assert transaction.amount_components.value is not None
        assert transaction.amount_components.value[0].money.amount == Decimal("57000")
        assert "fundCode" not in transaction.amount_components.source_fields

    def test_signed_negative_transaction_amounts_are_preserved_without_event_inference(
        self,
    ) -> None:
        transaction = FemaHmaTransactionMapper()(
            _record(_transactions()[1], "DR-4332-0300-R:2"),
            _snapshot(FEMA_HMA_TRANSACTIONS_DATASET_ID),
        ).record

        assert transaction.amount_components.value is not None
        amounts = [component.money.amount for component in transaction.amount_components.value]
        assert Decimal("-18274.93") in amounts
        assert Decimal("-1620") in amounts
        assert all(component.lifecycle is None for component in transaction.amount_components.value)
        assert transaction.event_type.value is None


class TestCaveats:
    def test_metadata_caveat_fixture_matches_fixed_category_set(self) -> None:
        caveats = observed_openfema_hma_metadata_caveats(OPENFEMA_HMA_DESCRIPTION)

        assert set(caveats) == set(OpenFemaHmaCaveat)

    def test_new_metadata_caveat_sentence_fails_loudly(self) -> None:
        description = OPENFEMA_HMA_DESCRIPTION.replace(
            "FEMA's terms and conditions",
            "New caveat sentence added by source. FEMA's terms and conditions",
        )

        with pytest.raises(ValueError, match="unrecognized OpenFEMA HMA caveat"):
            observed_openfema_hma_metadata_caveats(description)


class TestDrift:
    async def test_project_fixture_drift_clean(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PROJECTS_URL).mock(
                return_value=httpx.Response(
                    200,
                    json=_payload(str(FEMA_HMA_PROJECTS_DATASET_ID), _projects()),
                )
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(_project_adapter(client), FemaHmaProjectMapper())
                schema_obs = SchemaObserver(spec=FEMA_HMA_PROJECTS_SCHEMA)
                taxonomy_obs = TaxonomyObserver(specs=FEMA_HMA_PROJECTS_TAXONOMIES)
                observed = attach_observers(pipeline_result, [schema_obs, taxonomy_obs])
                records = [record async for record in observed.records]

        assert len(records) == 2
        assert schema_obs.finalize(pipeline_result.snapshot).findings == ()
        assert taxonomy_obs.finalize(pipeline_result.snapshot).findings == ()

    async def test_transaction_fixture_drift_clean(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(TRANSACTIONS_URL).mock(
                return_value=httpx.Response(
                    200,
                    json=_payload(str(FEMA_HMA_TRANSACTIONS_DATASET_ID), _transactions()),
                )
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(
                    _transaction_adapter(client), FemaHmaTransactionMapper()
                )
                schema_obs = SchemaObserver(spec=FEMA_HMA_TRANSACTIONS_SCHEMA)
                taxonomy_obs = TaxonomyObserver(specs=FEMA_HMA_TRANSACTIONS_TAXONOMIES)
                observed = attach_observers(pipeline_result, [schema_obs, taxonomy_obs])
                records = [record async for record in observed.records]

        assert len(records) == 2
        assert schema_obs.finalize(pipeline_result.snapshot).findings == ()
        assert taxonomy_obs.finalize(pipeline_result.snapshot).findings == ()

    async def test_unknown_project_status_and_type_surface_as_taxonomy_drift(self) -> None:
        rows = _projects()
        rows[0]["status"] = "Reopened"
        rows[0]["projectType"] = "999.9: Brand New Mitigation Activity"

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PROJECTS_URL).mock(
                return_value=httpx.Response(
                    200,
                    json=_payload(str(FEMA_HMA_PROJECTS_DATASET_ID), rows),
                )
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(_project_adapter(client), FemaHmaProjectMapper())
                taxonomy_obs = TaxonomyObserver(specs=FEMA_HMA_PROJECTS_TAXONOMIES)
                observed = attach_observers(pipeline_result, [taxonomy_obs])
                async for _ in observed.records:
                    pass

        report = taxonomy_obs.finalize(pipeline_result.snapshot)

        assert any(
            finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE
            and finding.taxonomy_id == "openfema-hma-project-status"
            for finding in report.findings
        )
        assert any(
            finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE
            and finding.taxonomy_id == "openfema-hma-project-type"
            for finding in report.findings
        )

    async def test_unknown_fund_code_surfaces_as_taxonomy_drift(self) -> None:
        rows = _transactions()
        rows[0]["fundCode"] = "NEW"

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(TRANSACTIONS_URL).mock(
                return_value=httpx.Response(
                    200,
                    json=_payload(str(FEMA_HMA_TRANSACTIONS_DATASET_ID), rows),
                )
            )

            async with httpx.AsyncClient() as client:
                pipeline_result = await run(
                    _transaction_adapter(client), FemaHmaTransactionMapper()
                )
                taxonomy_obs = TaxonomyObserver(specs=FEMA_HMA_TRANSACTIONS_TAXONOMIES)
                observed = attach_observers(pipeline_result, [taxonomy_obs])
                async for _ in observed.records:
                    pass

        report = taxonomy_obs.finalize(pipeline_result.snapshot)

        assert any(
            finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE
            and finding.taxonomy_id == "openfema-hma-fund-code"
            for finding in report.findings
        )


def test_source_metadata_preserves_scope_and_ids() -> None:
    assert SOURCE_ID == "openfema"
    assert FEMA_HMA_PROJECTS_DATASET_ID == "HazardMitigationAssistanceProjects"
    assert (
        FEMA_HMA_TRANSACTIONS_DATASET_ID
        == "HazardMitigationAssistanceProjectsFinancialTransactions"
    )
    assert "Hazard Mitigation Assistance" in FEMA_HMA_SOURCE_SCOPE
