"""Pipeline end-to-end against fixture-backed responses.

Exercises `civix.core.pipeline.run` with the real Toronto adapter
and mapper.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import respx

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction
from civix.core.pipeline import run
from civix.core.quality.models.fields import FieldQuality
from civix.core.spatial.models.location import Address
from civix.domains.business_licences.adapters.sources.ca.toronto import (
    DEFAULT_BASE_URL,
    TorontoBusinessLicencesAdapter,
    TorontoBusinessLicencesMapper,
)
from civix.domains.business_licences.models.licence import BusinessLicence, LicenceStatus

PINNED_NOW = datetime(2026, 4, 28, 12, 0, tzinfo=UTC)
DATASET = DatasetId("municipal-licensing-and-standards-business-licences-and-permits")
JURISDICTION = Jurisdiction(country="CA", region="ON", locality="Toronto")
PACKAGE_SHOW_URL = f"{DEFAULT_BASE_URL}package_show"
DATASTORE_SEARCH_URL = f"{DEFAULT_BASE_URL}datastore_search"

FIXTURES = Path(__file__).parent / "fixtures"


async def _run_pipeline() -> list[BusinessLicence]:
    catalog = json.loads((FIXTURES / "catalog_response.json").read_text())
    page = json.loads((FIXTURES / "records_page.json").read_text())

    async with respx.mock(assert_all_called=True) as respx_mock:
        respx_mock.get(PACKAGE_SHOW_URL).mock(return_value=httpx.Response(200, json=catalog))
        respx_mock.get(DATASTORE_SEARCH_URL).mock(return_value=httpx.Response(200, json=page))

        async with httpx.AsyncClient() as client:
            adapter = TorontoBusinessLicencesAdapter(
                dataset_id=DATASET,
                jurisdiction=JURISDICTION,
                client=client,
                clock=lambda: PINNED_NOW,
            )
            mapper = TorontoBusinessLicencesMapper()
            result = await run(adapter, mapper)
            licences: list[BusinessLicence] = [pr.mapped.record async for pr in result.records]

    return licences


class TestPipelineEndToEnd:
    async def test_three_records_in_three_licences_out(self) -> None:
        licences = await _run_pipeline()

        assert len(licences) == 3

    async def test_first_record_full_normalization(self) -> None:
        licences = await _run_pipeline()
        first = licences[0]

        assert first.business_name.value == "TAXIFY"
        assert first.business_name.quality is FieldQuality.DIRECT
        assert first.licence_number.value == "B02-4741962"
        assert first.status.value is LicenceStatus.CANCELLED
        assert first.status.quality is FieldQuality.DERIVED
        assert first.category.value is not None
        assert first.category.value.code == "private-transportation-company"
        assert first.category.value.label == "PRIVATE TRANSPORTATION COMPANY"
        assert first.issued_at.value == date(2018, 1, 18)
        assert first.address.value == Address(
            country="CA",
            region="ON",
            locality="TORONTO",
            street="35 OAK ST, #304",
            postal_code="M9N 1A1",
        )

    async def test_active_record_status_not_provided(self) -> None:
        licences = await _run_pipeline()
        active = licences[1]

        assert active.status.value is None
        assert active.status.quality is FieldQuality.NOT_PROVIDED
        assert active.business_name.value == "ZEN STUDIO"
        assert active.address.value is not None
        assert active.address.value.locality == "TORONTO"

    async def test_record_with_null_operating_name_falls_back_to_not_provided(self) -> None:
        licences = await _run_pipeline()
        third = licences[2]

        assert third.business_name.value is None
        assert third.business_name.quality is FieldQuality.NOT_PROVIDED
        assert third.status.value is LicenceStatus.CANCELLED
        assert third.address.value is not None
        assert third.address.value.locality == "MISSISSAUGA"

    async def test_unmapped_domain_fields_consistent_across_records(self) -> None:
        licences = await _run_pipeline()

        for licence in licences:
            assert licence.expires_at.quality is FieldQuality.UNMAPPED
            assert licence.coordinate.quality is FieldQuality.UNMAPPED
            assert licence.neighbourhood.quality is FieldQuality.UNMAPPED

    async def test_provenance_carries_snapshot_metadata(self) -> None:
        licences = await _run_pipeline()

        for licence in licences:
            assert licence.provenance.source_id == "toronto-open-data"
            assert licence.provenance.dataset_id == DATASET
            assert licence.provenance.jurisdiction == JURISDICTION
            assert licence.provenance.fetched_at == PINNED_NOW
            assert licence.provenance.mapper.mapper_id == "toronto-business-licences"

    async def test_source_record_id_threaded_through(self) -> None:
        licences = await _run_pipeline()

        assert [licence.provenance.source_record_id for licence in licences] == [
            "B02-4741962",
            "B66-1234567",
            "B45-9876543",
        ]
