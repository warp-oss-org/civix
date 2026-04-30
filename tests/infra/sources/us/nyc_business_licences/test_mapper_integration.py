"""Fixture-backed NYC DCWP premises business-licence pipeline tests."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import respx

from civix.core.identity import DatasetId, Jurisdiction
from civix.core.pipeline import run
from civix.domains.business_licences import BusinessLicence, LicenceStatus
from civix.infra.sources.us.nyc_business_licences import (
    DEFAULT_BASE_URL,
    NycBusinessLicencesAdapter,
    NycBusinessLicencesMapper,
)

PINNED_NOW = datetime(2026, 4, 30, 12, 0, tzinfo=UTC)
DATASET = DatasetId("w7w3-xahh")
JURISDICTION = Jurisdiction(country="US", region="NY", locality="New York")
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
            adapter = NycBusinessLicencesAdapter(
                dataset_id=DATASET,
                jurisdiction=JURISDICTION,
                client=client,
                clock=lambda: PINNED_NOW,
            )
            mapper = NycBusinessLicencesMapper()
            result = await run(adapter, mapper)
            licences = [pr.mapped.record async for pr in result.records]

    return licences


class TestNycMapperIntegration:
    async def test_three_records_in_three_licences_out(self) -> None:
        licences = await _run_pipeline()

        assert len(licences) == 3

    async def test_first_record_maps_core_fields(self) -> None:
        licences = await _run_pipeline()
        first = licences[0]

        assert first.business_name.value == "GEM PAWNBROKERS"
        assert first.licence_number.value == "0002902-DCA"
        assert first.status.value is LicenceStatus.RENEWAL_DUE
        assert first.category.value is not None
        assert first.category.value.label == "Pawnbroker"
        assert first.issued_at.value == date(2007, 4, 18)
        assert first.expires_at.value == date(2026, 4, 30)
        assert first.address.value is not None
        assert first.address.value.locality == "NEW YORK"
        assert first.neighbourhood.quality.value == "unmapped"

    async def test_legal_name_fallback_and_numeric_coordinates(self) -> None:
        licences = await _run_pipeline()
        second = licences[1]

        assert second.business_name.value == "JAMES ROBINSON, INC."
        assert second.status.value is LicenceStatus.ACTIVE
        assert second.coordinate.value is not None
        assert second.coordinate.value.latitude == 40.76248502732357

    async def test_provenance_threads_snapshot_metadata(self) -> None:
        licences = await _run_pipeline()

        for licence in licences:
            assert licence.provenance.source_id == "nyc-open-data"
            assert licence.provenance.dataset_id == DATASET
            assert licence.provenance.jurisdiction == JURISDICTION
            assert licence.provenance.fetched_at == PINNED_NOW
            assert licence.provenance.mapper.mapper_id == "nyc-business-licences"

    async def test_source_record_ids_are_stable_license_numbers(self) -> None:
        licences = await _run_pipeline()

        assert [licence.provenance.source_record_id for licence in licences] == [
            "0002902-DCA",
            "0016371-DCA",
            "0157941-DCA",
        ]
