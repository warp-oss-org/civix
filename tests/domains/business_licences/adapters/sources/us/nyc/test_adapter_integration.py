"""Integration tests against NYC Socrata-shaped fixtures."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import respx

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction
from civix.domains.business_licences.adapters.sources.us.nyc import (
    DEFAULT_BASE_URL,
    PREMISES_FILTER,
    NycBusinessLicencesAdapter,
)
from civix.infra.sources.socrata import SOCRATA_DEFAULT_ORDER

PINNED_NOW = datetime(2026, 4, 30, 12, 0, tzinfo=UTC)
DATASET = DatasetId("w7w3-xahh")
JURISDICTION = Jurisdiction(country="US", region="NY", locality="New York")
RESOURCE_URL = f"{DEFAULT_BASE_URL}{DATASET}.json"

FIXTURES = Path(__file__).parent / "fixtures"


def _adapter(client: httpx.AsyncClient) -> NycBusinessLicencesAdapter:
    return NycBusinessLicencesAdapter(
        dataset_id=DATASET,
        jurisdiction=JURISDICTION,
        client=client,
        clock=lambda: PINNED_NOW,
    )


class TestNycShapedFixtures:
    async def test_full_fetch_round_trip(self) -> None:
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
                result = await _adapter(client).fetch()
                records = [r async for r in result.records]

        assert result.snapshot.record_count == 3
        assert result.snapshot.fetch_params == {
            "$where": PREMISES_FILTER,
            "$order": SOCRATA_DEFAULT_ORDER,
        }
        assert len(records) == 3

    async def test_record_shape_preserved_after_transport_strip(self) -> None:
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
                result = await _adapter(client).fetch()
                records = [r async for r in result.records]

        first = records[0]

        assert first.raw_data["business_name"] == "GEM FINANCIAL SERVICES, INC."
        assert first.raw_data["address_borough"] == "Manhattan"
        assert first.source_record_id == "0002902-DCA"
        assert all(not key.startswith(":@computed_region_") for key in first.raw_data)

    async def test_snapshot_back_reference_holds(self) -> None:
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
                result = await _adapter(client).fetch()
                records = [r async for r in result.records]

        for record in records:
            assert record.snapshot_id == result.snapshot.snapshot_id
