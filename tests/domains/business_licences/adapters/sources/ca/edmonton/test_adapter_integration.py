"""Integration tests against Edmonton Socrata-shaped fixtures."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import respx

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction
from civix.domains.business_licences.adapters.sources.ca.edmonton import (
    DEFAULT_BASE_URL,
    EdmontonBusinessLicencesAdapter,
)

PINNED_NOW = datetime(2026, 4, 30, 12, 0, tzinfo=UTC)
DATASET = DatasetId("qhi4-bdpu")
JURISDICTION = Jurisdiction(country="CA", region="AB", locality="Edmonton")
RESOURCE_URL = f"{DEFAULT_BASE_URL}{DATASET}.json"

FIXTURES = Path(__file__).parent / "fixtures"


def _adapter(client: httpx.AsyncClient) -> EdmontonBusinessLicencesAdapter:
    return EdmontonBusinessLicencesAdapter(
        dataset_id=DATASET,
        jurisdiction=JURISDICTION,
        client=client,
        clock=lambda: PINNED_NOW,
    )


class TestEdmontonShapedFixtures:
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

        assert first.raw_data["business_name"] == "PRAIRIE CAFE"
        assert first.raw_data["geometry_point"] == "POINT (-113.49464046380713 53.5426116941546)"
        assert first.source_record_id == "100031017-001"
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
