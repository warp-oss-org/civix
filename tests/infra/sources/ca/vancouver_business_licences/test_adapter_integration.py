"""Integration tests against fixture-backed responses with realistic shapes."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import respx

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction
from civix.infra.sources.ca.vancouver_business_licences import (
    DEFAULT_BASE_URL,
    VancouverBusinessLicencesAdapter,
)

PINNED_NOW = datetime(2026, 4, 25, 12, 0, tzinfo=UTC)
DATASET = DatasetId("business-licences")
JURISDICTION = Jurisdiction(country="CA", region="BC", locality="Vancouver")
RECORDS_URL = f"{DEFAULT_BASE_URL}catalog/datasets/{DATASET}/records"
EXPORTS_URL = f"{DEFAULT_BASE_URL}catalog/datasets/{DATASET}/exports/jsonl"

FIXTURES = Path(__file__).parent / "fixtures"


def _adapter(client: httpx.AsyncClient) -> VancouverBusinessLicencesAdapter:
    return VancouverBusinessLicencesAdapter(
        dataset_id=DATASET,
        jurisdiction=JURISDICTION,
        client=client,
        clock=lambda: PINNED_NOW,
    )


class TestVancouverShapedFixtures:
    async def test_full_fetch_round_trip(self) -> None:
        count_payload = json.loads((FIXTURES / "count_response.json").read_text())
        records_body = (FIXTURES / "records.jsonl").read_text()

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RECORDS_URL).mock(return_value=httpx.Response(200, json=count_payload))
            respx_mock.get(EXPORTS_URL).mock(return_value=httpx.Response(200, text=records_body))

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                records = [r async for r in result.records]

        assert result.snapshot.record_count == 3
        assert len(records) == 3

    async def test_record_shape_preserved(self) -> None:
        count_payload = json.loads((FIXTURES / "count_response.json").read_text())
        records_body = (FIXTURES / "records.jsonl").read_text()

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RECORDS_URL).mock(return_value=httpx.Response(200, json=count_payload))
            respx_mock.get(EXPORTS_URL).mock(return_value=httpx.Response(200, text=records_body))

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                records = [r async for r in result.records]

        first = records[0]

        assert first.raw_data["businessname"] == "Joe's Cafe"
        assert first.raw_data["geo_point_2d"] == {"lon": -123.1207, "lat": 49.2827}
        assert first.raw_data["postalcode"] == "V6B 1A1"
        assert first.source_record_id == "1234567"

    async def test_redacted_record_preserves_sentinel(self) -> None:
        count_payload = json.loads((FIXTURES / "count_response.json").read_text())
        records_body = (FIXTURES / "records.jsonl").read_text()

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RECORDS_URL).mock(return_value=httpx.Response(200, json=count_payload))
            respx_mock.get(EXPORTS_URL).mock(return_value=httpx.Response(200, text=records_body))

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                records = [r async for r in result.records]

        redacted = records[1]

        assert redacted.raw_data["businessname"] == "REDACTED"
        assert redacted.raw_data["geo_point_2d"] is None
        assert redacted.raw_data["postalcode"] is None

    async def test_extract_date_round_trips_to_source_updated_at(self) -> None:
        count_payload = json.loads((FIXTURES / "count_response.json").read_text())
        records_body = (FIXTURES / "records.jsonl").read_text()

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RECORDS_URL).mock(return_value=httpx.Response(200, json=count_payload))
            respx_mock.get(EXPORTS_URL).mock(return_value=httpx.Response(200, text=records_body))

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                records = [r async for r in result.records]

        for r in records:
            assert r.source_updated_at == datetime(2026, 4, 25, 0, 0, tzinfo=UTC)

    async def test_snapshot_back_reference_holds(self) -> None:
        count_payload = json.loads((FIXTURES / "count_response.json").read_text())
        records_body = (FIXTURES / "records.jsonl").read_text()

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RECORDS_URL).mock(return_value=httpx.Response(200, json=count_payload))
            respx_mock.get(EXPORTS_URL).mock(return_value=httpx.Response(200, text=records_body))

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                records = [r async for r in result.records]

        for r in records:
            assert r.snapshot_id == result.snapshot.snapshot_id
