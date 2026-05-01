"""Integration tests against fixture-backed responses with realistic shapes."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import respx

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction
from civix.infra.sources.ca.toronto_business_licences import (
    DEFAULT_BASE_URL,
    TorontoBusinessLicencesAdapter,
)

PINNED_NOW = datetime(2026, 4, 28, 12, 0, tzinfo=UTC)
DATASET = DatasetId("municipal-licensing-and-standards-business-licences-and-permits")
JURISDICTION = Jurisdiction(country="CA", region="ON", locality="Toronto")
RESOURCE_ID = "169e90ba-3ae0-43dd-8b2f-919e87002f50"
PACKAGE_SHOW_URL = f"{DEFAULT_BASE_URL}package_show"
DATASTORE_SEARCH_URL = f"{DEFAULT_BASE_URL}datastore_search"

FIXTURES = Path(__file__).parent / "fixtures"


def _adapter(client: httpx.AsyncClient) -> TorontoBusinessLicencesAdapter:
    return TorontoBusinessLicencesAdapter(
        dataset_id=DATASET,
        jurisdiction=JURISDICTION,
        client=client,
        clock=lambda: PINNED_NOW,
    )


class TestTorontoShapedFixtures:
    async def test_full_fetch_round_trip(self) -> None:
        catalog = json.loads((FIXTURES / "catalog_response.json").read_text())
        page = json.loads((FIXTURES / "records_page.json").read_text())

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(return_value=httpx.Response(200, json=catalog))
            respx_mock.get(DATASTORE_SEARCH_URL).mock(return_value=httpx.Response(200, json=page))

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                records = [r async for r in result.records]

        assert result.snapshot.record_count == 3
        assert len(records) == 3

    async def test_record_shape_preserved(self) -> None:
        catalog = json.loads((FIXTURES / "catalog_response.json").read_text())
        page = json.loads((FIXTURES / "records_page.json").read_text())

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(return_value=httpx.Response(200, json=catalog))
            respx_mock.get(DATASTORE_SEARCH_URL).mock(return_value=httpx.Response(200, json=page))

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                records = [r async for r in result.records]

        first = records[0]

        assert first.raw_data["Operating Name"] == "TAXIFY"
        assert first.raw_data["Licence Address Line 2"] == "TORONTO, ON"
        assert first.raw_data["Cancel Date"] == "2018-12-07"
        assert first.source_record_id == "B02-4741962"

    async def test_active_record_has_null_cancel_date(self) -> None:
        catalog = json.loads((FIXTURES / "catalog_response.json").read_text())
        page = json.loads((FIXTURES / "records_page.json").read_text())

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(return_value=httpx.Response(200, json=catalog))
            respx_mock.get(DATASTORE_SEARCH_URL).mock(return_value=httpx.Response(200, json=page))

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                records = [r async for r in result.records]

        active = records[1]

        assert active.raw_data["Operating Name"] == "ZEN STUDIO"
        assert active.raw_data["Cancel Date"] is None

    async def test_record_with_null_operating_name_round_trips(self) -> None:
        catalog = json.loads((FIXTURES / "catalog_response.json").read_text())
        page = json.loads((FIXTURES / "records_page.json").read_text())

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(return_value=httpx.Response(200, json=catalog))
            respx_mock.get(DATASTORE_SEARCH_URL).mock(return_value=httpx.Response(200, json=page))

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                records = [r async for r in result.records]

        third = records[2]

        assert third.raw_data["Operating Name"] is None
        assert third.raw_data["Client Name"] == "ACME RENTALS LTD"

    async def test_ckan_internal_id_is_preserved(self) -> None:
        catalog = json.loads((FIXTURES / "catalog_response.json").read_text())
        page = json.loads((FIXTURES / "records_page.json").read_text())

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(return_value=httpx.Response(200, json=catalog))
            respx_mock.get(DATASTORE_SEARCH_URL).mock(return_value=httpx.Response(200, json=page))

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                records = [r async for r in result.records]

        assert [r.raw_data["_id"] for r in records] == [1, 2, 3]

    async def test_snapshot_back_reference_holds(self) -> None:
        catalog = json.loads((FIXTURES / "catalog_response.json").read_text())
        page = json.loads((FIXTURES / "records_page.json").read_text())

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(return_value=httpx.Response(200, json=catalog))
            respx_mock.get(DATASTORE_SEARCH_URL).mock(return_value=httpx.Response(200, json=page))

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                records = [r async for r in result.records]

        for r in records:
            assert r.snapshot_id == result.snapshot.snapshot_id

    async def test_snapshot_url_carries_active_resource_id(self) -> None:
        catalog = json.loads((FIXTURES / "catalog_response.json").read_text())
        page = json.loads((FIXTURES / "records_page.json").read_text())

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(return_value=httpx.Response(200, json=catalog))
            respx_mock.get(DATASTORE_SEARCH_URL).mock(return_value=httpx.Response(200, json=page))

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                _ = [r async for r in result.records]

        assert RESOURCE_ID in (result.snapshot.source_url or "")
