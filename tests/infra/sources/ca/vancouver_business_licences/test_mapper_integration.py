"""Pipeline end-to-end against fixture-backed responses.

Exercises `civix.core.pipeline.run` with the real Vancouver adapter
and mapper. Replaces the previous hand-rolled fetch/map loop.
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
from civix.core.spatial.models.location import Address, Coordinate
from civix.domains.business_licences.models.licence import BusinessLicence, LicenceStatus
from civix.infra.sources.ca.vancouver_business_licences import (
    DEFAULT_BASE_URL,
    VancouverBusinessLicencesAdapter,
    VancouverBusinessLicencesMapper,
)

PINNED_NOW = datetime(2026, 4, 25, 12, 0, tzinfo=UTC)
DATASET = DatasetId("business-licences")
JURISDICTION = Jurisdiction(country="CA", region="BC", locality="Vancouver")
RECORDS_URL = f"{DEFAULT_BASE_URL}catalog/datasets/{DATASET}/records"
EXPORTS_URL = f"{DEFAULT_BASE_URL}catalog/datasets/{DATASET}/exports/jsonl"

FIXTURES = Path(__file__).parent / "fixtures"


async def _run_pipeline() -> list[BusinessLicence]:
    count_payload = json.loads((FIXTURES / "count_response.json").read_text())
    records_body = (FIXTURES / "records.jsonl").read_text()

    async with respx.mock(assert_all_called=True) as respx_mock:
        respx_mock.get(RECORDS_URL).mock(return_value=httpx.Response(200, json=count_payload))
        respx_mock.get(EXPORTS_URL).mock(return_value=httpx.Response(200, text=records_body))

        async with httpx.AsyncClient() as client:
            adapter = VancouverBusinessLicencesAdapter(
                dataset_id=DATASET,
                jurisdiction=JURISDICTION,
                client=client,
                clock=lambda: PINNED_NOW,
            )
            mapper = VancouverBusinessLicencesMapper()
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

        assert first.business_name.value == "Joe's Cafe"
        assert first.business_name.quality is FieldQuality.DIRECT
        assert first.licence_number.value == "24-123456"
        assert first.status.value is LicenceStatus.ACTIVE
        assert first.status.quality is FieldQuality.STANDARDIZED
        assert first.category.value is not None
        assert first.category.value.code == "restaurant-class-1"
        assert first.category.value.label == "Restaurant - Class 1"
        assert first.coordinate.value == Coordinate(latitude=49.2827, longitude=-123.1207)
        assert first.address.value == Address(
            country="CA",
            region="BC",
            locality="Vancouver",
            street="123 W Pender St",
            postal_code="V6B 1A1",
        )
        assert first.neighbourhood.value == "Downtown"

    async def test_redacted_record_partial_address_survives(self) -> None:
        licences = await _run_pipeline()
        redacted = licences[1]

        assert redacted.business_name.value is None
        assert redacted.business_name.quality is FieldQuality.REDACTED
        assert redacted.address.value == Address(
            country="CA",
            region="BC",
            locality="Vancouver",
        )
        assert redacted.address.quality is FieldQuality.DERIVED
        assert redacted.coordinate.value is None
        assert redacted.coordinate.quality is FieldQuality.NOT_PROVIDED
        assert redacted.neighbourhood.value == "West End"

    async def test_cancelled_record_status_maps(self) -> None:
        licences = await _run_pipeline()
        cancelled = licences[2]

        assert cancelled.status.value is LicenceStatus.CANCELLED
        assert cancelled.status.quality is FieldQuality.STANDARDIZED
        assert cancelled.address.value is not None
        assert cancelled.address.value.street == "800 W Hastings St Suite 500"

    async def test_provenance_carries_snapshot_metadata(self) -> None:
        licences = await _run_pipeline()

        for licence in licences:
            assert licence.provenance.source_id == "vancouver-open-data"
            assert licence.provenance.dataset_id == DATASET
            assert licence.provenance.jurisdiction == JURISDICTION
            assert licence.provenance.fetched_at == PINNED_NOW
            assert licence.provenance.mapper.mapper_id == "vancouver-business-licences"

    async def test_source_record_id_threaded_through(self) -> None:
        licences = await _run_pipeline()

        assert [licence.provenance.source_record_id for licence in licences] == [
            "1234567",
            "1234568",
            "1234569",
        ]

    async def test_issued_at_uses_vancouver_local_calendar_day(self) -> None:
        # 2024-05-06T00:00:00+00:00 == 2024-05-05 17:00 PDT.
        licences = await _run_pipeline()

        assert licences[0].issued_at.value == date(2024, 5, 5)
