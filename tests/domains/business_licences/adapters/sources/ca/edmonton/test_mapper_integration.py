"""Fixture-backed Edmonton business-licence pipeline tests."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import respx

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction
from civix.core.pipeline import run
from civix.domains.business_licences.adapters.sources.ca.edmonton import (
    DEFAULT_BASE_URL,
    EdmontonBusinessLicencesAdapter,
    EdmontonBusinessLicencesMapper,
)
from civix.domains.business_licences.models.licence import BusinessLicence

PINNED_NOW = datetime(2026, 4, 30, 12, 0, tzinfo=UTC)
DATASET = DatasetId("qhi4-bdpu")
JURISDICTION = Jurisdiction(country="CA", region="AB", locality="Edmonton")
RESOURCE_URL = f"{DEFAULT_BASE_URL}{DATASET}.json"

FIXTURES = Path(__file__).parent / "fixtures"


async def _run_pipeline() -> list[BusinessLicence]:
    count = json.loads((FIXTURES / "count_response.json").read_text())
    page = json.loads((FIXTURES / "records_page.json").read_text())

    async with respx.mock(assert_all_called=True) as respx_mock:
        respx_mock.get(RESOURCE_URL).mock(
            side_effect=[
                httpx.Response(200, json=count),
                httpx.Response(200, json=page),
            ]
        )

        async with httpx.AsyncClient() as client:
            adapter = EdmontonBusinessLicencesAdapter(
                dataset_id=DATASET,
                jurisdiction=JURISDICTION,
                client=client,
                clock=lambda: PINNED_NOW,
            )
            mapper = EdmontonBusinessLicencesMapper()
            result = await run(adapter, mapper)
            licences = [pr.mapped.record async for pr in result.records]

    return licences


class TestEdmontonMapperIntegration:
    async def test_three_records_in_three_licences_out(self) -> None:
        licences = await _run_pipeline()

        assert len(licences) == 3

    async def test_first_record_maps_core_fields(self) -> None:
        licences = await _run_pipeline()
        first = licences[0]

        assert first.business_name.value == "PRAIRIE CAFE"
        assert first.licence_number.value == "100031017-001"
        assert first.status.value is None
        assert first.category.value is not None
        assert first.category.value.label == "Restaurant or Food Service"
        assert first.issued_at.value == date(2026, 1, 15)
        assert first.expires_at.value == date(2027, 1, 15)
        assert first.address.value is not None
        assert first.address.value.locality == "Edmonton"
        assert first.neighbourhood.value == "Downtown"

    async def test_blank_business_and_address_are_preserved_as_not_provided(self) -> None:
        licences = await _run_pipeline()
        home_based = licences[1]

        assert home_based.business_name.value is None
        assert home_based.address.value is not None
        assert home_based.address.value.street is None
        assert home_based.coordinate.value is None

    async def test_provenance_threads_snapshot_metadata(self) -> None:
        licences = await _run_pipeline()

        for licence in licences:
            assert licence.provenance.source_id == "edmonton-open-data"
            assert licence.provenance.dataset_id == DATASET
            assert licence.provenance.jurisdiction == JURISDICTION
            assert licence.provenance.fetched_at == PINNED_NOW
            assert licence.provenance.mapper.mapper_id == "edmonton-business-licences"

    async def test_source_record_ids_are_stable_licence_numbers(self) -> None:
        licences = await _run_pipeline()

        assert [licence.provenance.source_record_id for licence in licences] == [
            "100031017-001",
            "100031018-001",
            "100031019-001",
        ]
