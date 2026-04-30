"""Unit tests for NycBusinessLicencesAdapter against Socrata-shaped responses."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
import respx

from civix.core.adapters import FetchError
from civix.core.identity import DatasetId, Jurisdiction
from civix.infra.sources.us.nyc_business_licences import (
    DEFAULT_BASE_URL,
    DEFAULT_PAGE_SIZE,
    PREMISES_FILTER,
    SOURCE_ID,
    NycBusinessLicencesAdapter,
)

PINNED_NOW = datetime(2026, 4, 30, 12, 0, tzinfo=UTC)
DATASET = DatasetId("w7w3-xahh")
JURISDICTION = Jurisdiction(country="US", region="NY", locality="New York")
RESOURCE_URL = f"{DEFAULT_BASE_URL}{DATASET}.json"


def _adapter(
    client: httpx.AsyncClient,
    *,
    page_size: int = 1000,
    app_token: str | None = None,
) -> NycBusinessLicencesAdapter:
    return NycBusinessLicencesAdapter(
        dataset_id=DATASET,
        jurisdiction=JURISDICTION,
        client=client,
        clock=lambda: PINNED_NOW,
        page_size=page_size,
        app_token=app_token,
    )


def _record(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "license_nbr": "0002902-DCA",
        "business_name": "GEM FINANCIAL SERVICES, INC.",
        "business_category": "Pawnbroker",
        "license_type": "Premises",
        "license_status": "Active",
        ":@computed_region_test": "10",
    }
    row.update(overrides)

    return row


def _capture_sequence(
    requests: list[httpx.Request],
    responses: list[httpx.Response],
) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)

        return responses.pop(0)

    return handler


class TestProperties:
    async def test_source_id_is_constant(self) -> None:
        async with httpx.AsyncClient() as client:
            adapter = _adapter(client)

            assert adapter.source_id == SOURCE_ID

    async def test_default_page_size_is_socrata_unauthenticated_max(self) -> None:
        async with httpx.AsyncClient() as client:
            adapter = NycBusinessLicencesAdapter(
                dataset_id=DATASET,
                jurisdiction=JURISDICTION,
                client=client,
            )

            assert adapter.page_size == DEFAULT_PAGE_SIZE

    async def test_page_size_must_be_positive(self) -> None:
        async with httpx.AsyncClient() as client:
            with pytest.raises(ValueError, match="page_size"):
                _adapter(client, page_size=0)


class TestFetchHappyPath:
    async def test_builds_snapshot_with_premises_count_probe(self) -> None:
        requests: list[httpx.Request] = []

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=_capture_sequence(
                    requests,
                    [
                        httpx.Response(200, json=[{"count": "53318"}]),
                        httpx.Response(200, json=[]),
                    ],
                )
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                yielded = [r async for r in result.records]

            assert result.snapshot.record_count == 53318
            assert result.snapshot.fetched_at == PINNED_NOW
            assert result.snapshot.source_url == RESOURCE_URL
            assert result.snapshot.fetch_params == {"$where": PREMISES_FILTER}
            assert yielded == []
            assert requests[0].url.params["$select"] == "count(*)"
            assert requests[0].url.params["$where"] == PREMISES_FILTER

    async def test_snapshot_id_is_deterministic_given_clock(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[
                    httpx.Response(200, json=[{"count": 0}]),
                    httpx.Response(200, json=[]),
                ]
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()

            assert result.snapshot.snapshot_id == (
                f"{SOURCE_ID}:{DATASET}:{PINNED_NOW.isoformat()}"
            )

    async def test_streams_records_from_first_page(self) -> None:
        records = [_record(license_nbr="0002902-DCA"), _record(license_nbr="0016371-DCA")]
        requests: list[httpx.Request] = []

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=_capture_sequence(
                    requests,
                    [
                        httpx.Response(200, json=[{"count": "2"}]),
                        httpx.Response(200, json=records),
                    ],
                )
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                yielded = [r async for r in result.records]

            assert [r.source_record_id for r in yielded] == ["0002902-DCA", "0016371-DCA"]
            assert [r.raw_data["business_name"] for r in yielded] == [
                "GEM FINANCIAL SERVICES, INC.",
                "GEM FINANCIAL SERVICES, INC.",
            ]
            assert requests[1].url.params["$where"] == PREMISES_FILTER

    async def test_source_updated_at_is_none(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[
                    httpx.Response(200, json=[{"count": "1"}]),
                    httpx.Response(200, json=[_record()]),
                ]
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                yielded = [r async for r in result.records]

            assert yielded[0].source_updated_at is None

    async def test_computed_region_fields_are_stripped(self) -> None:
        raw = _record(address_borough="Manhattan", **{":@computed_region_extra": "20"})

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[
                    httpx.Response(200, json=[{"count": "1"}]),
                    httpx.Response(200, json=[raw]),
                ]
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                yielded = [r async for r in result.records]

            assert "address_borough" in yielded[0].raw_data
            assert all(not key.startswith(":@computed_region_") for key in yielded[0].raw_data)

    async def test_missing_license_number_leaves_source_record_id_none(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[
                    httpx.Response(200, json=[{"count": "1"}]),
                    httpx.Response(200, json=[_record(license_nbr=None)]),
                ]
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                yielded = [r async for r in result.records]

            assert yielded[0].source_record_id is None


class TestPagination:
    async def test_walks_pages_until_short_page_terminates(self) -> None:
        page_one = [_record(license_nbr="0002902-DCA"), _record(license_nbr="0016371-DCA")]
        page_two = [_record(license_nbr="0157941-DCA")]
        requests: list[httpx.Request] = []

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=_capture_sequence(
                    requests,
                    [
                        httpx.Response(200, json=[{"count": "3"}]),
                        httpx.Response(200, json=page_one),
                        httpx.Response(200, json=page_two),
                    ],
                )
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client, page_size=2).fetch()
                yielded = [r async for r in result.records]

            assert [r.source_record_id for r in yielded] == [
                "0002902-DCA",
                "0016371-DCA",
                "0157941-DCA",
            ]
            assert requests[1].url.params["$offset"] == "0"
            assert requests[2].url.params["$offset"] == "2"
            assert requests[2].url.params["$where"] == PREMISES_FILTER


class TestFailures:
    async def test_non_json_count_raises_fetch_error(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(httpx.Response(200, text="nope"))

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="non-JSON response"):
                    await _adapter(client).fetch()

    async def test_invalid_count_shape_raises_fetch_error(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(httpx.Response(200, json=[{"count": "nan"}]))

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="missing or invalid count"):
                    await _adapter(client).fetch()

    async def test_non_list_records_raises_fetch_error(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[
                    httpx.Response(200, json=[{"count": "1"}]),
                    httpx.Response(200, json={"records": []}),
                ]
            )

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="non-list JSON body"):
                    await _adapter(client).fetch()

    async def test_non_object_record_raises_fetch_error_when_streamed(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[
                    httpx.Response(200, json=[{"count": "1"}]),
                    httpx.Response(200, json=[["not", "object"]]),
                ]
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()

                with pytest.raises(FetchError, match="non-object record"):
                    _ = [r async for r in result.records]
