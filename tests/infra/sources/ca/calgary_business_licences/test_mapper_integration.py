"""Pipeline end-to-end against Calgary fixture-backed responses."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import respx

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction
from civix.core.pipeline import run
from civix.core.quality.models.fields import FieldQuality
from civix.core.spatial.models.location import Address, Coordinate
from civix.domains.business_licences.models.licence import BusinessLicence, LicenceStatus
from civix.infra.sources.ca.calgary_business_licences import (
    DEFAULT_BASE_URL,
    CalgaryBusinessLicencesAdapter,
    CalgaryBusinessLicencesMapper,
)

PINNED_NOW = datetime(2026, 4, 29, 12, 0, tzinfo=UTC)
DATASET = DatasetId("vdjc-pybd")
JURISDICTION = Jurisdiction(country="CA", region="AB", locality="Calgary")
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
            adapter = CalgaryBusinessLicencesAdapter(
                dataset_id=DATASET,
                jurisdiction=JURISDICTION,
                client=client,
                clock=lambda: PINNED_NOW,
            )
            mapper = CalgaryBusinessLicencesMapper()
            result = await run(adapter, mapper)
            licences: list[BusinessLicence] = [pr.mapped.record async for pr in result.records]

    return licences


class TestPipelineEndToEnd:
    async def test_four_records_in_four_licences_out(self) -> None:
        licences = await _run_pipeline()

        assert len(licences) == 4

    async def test_first_record_full_normalization(self) -> None:
        licences = await _run_pipeline()
        first = licences[0]

        assert first.business_name.value == "PRAIRIE CAFE"
        assert first.licence_number.value == "100001"
        assert first.status.value is LicenceStatus.ACTIVE
        assert first.status.quality is FieldQuality.STANDARDIZED
        assert first.category.value is not None
        assert first.category.value.code == "food-service-premises"
        assert first.issued_at.value == date(2020, 1, 15)
        assert first.expires_at.value == date(2026, 1, 14)
        assert first.address.value == Address(
            country="CA",
            region="AB",
            locality="Calgary",
            street="100 MAIN ST SE",
        )
        assert first.coordinate.value == Coordinate(latitude=51.0447, longitude=-114.0719)
        assert first.neighbourhood.value == "DOWNTOWN COMMERCIAL CORE"

    async def test_multi_category_record_uses_first_category(self) -> None:
        licences = await _run_pipeline()
        second = licences[1]

        assert second.status.value is LicenceStatus.RENEWAL_DUE
        assert second.category.value is not None
        assert second.category.value.label == "RETAIL DEALER - PREMISES"

    async def test_workflow_status_maps_to_inferred_unknown(self) -> None:
        licences = await _run_pipeline()
        third = licences[2]

        assert third.status.value is LicenceStatus.UNKNOWN
        assert third.status.quality is FieldQuality.INFERRED
        assert third.issued_at.value is None
        assert third.expires_at.value is None
        assert third.coordinate.value is None

    async def test_provenance_carries_snapshot_metadata(self) -> None:
        licences = await _run_pipeline()

        for licence in licences:
            assert licence.provenance.source_id == "calgary-open-data"
            assert licence.provenance.dataset_id == DATASET
            assert licence.provenance.jurisdiction == JURISDICTION
            assert licence.provenance.fetched_at == PINNED_NOW
            assert licence.provenance.mapper.mapper_id == "calgary-business-licences"

    async def test_source_record_id_threaded_through(self) -> None:
        licences = await _run_pipeline()

        assert [licence.provenance.source_record_id for licence in licences] == [
            "100001",
            "100002",
            "100003",
            "100004",
        ]
