"""Unit tests for EdmontonBusinessLicencesAdapter against Socrata-shaped responses."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
import respx

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction
from civix.core.ports.errors import FetchError
from civix.domains.business_licences.adapters.sources.ca.edmonton import (
    DEFAULT_BASE_URL,
    DEFAULT_PAGE_SIZE,
    SOURCE_ID,
    EdmontonBusinessLicencesAdapter,
)

PINNED_NOW = datetime(2026, 4, 30, 12, 0, tzinfo=UTC)
DATASET = DatasetId("qhi4-bdpu")
JURISDICTION = Jurisdiction(country="CA", region="AB", locality="Edmonton")
RESOURCE_URL = f"{DEFAULT_BASE_URL}{DATASET}.json"


def _adapter(
    client: httpx.AsyncClient,
    *,
    page_size: int = 1000,
    app_token: str | None = None,
) -> EdmontonBusinessLicencesAdapter:
    return EdmontonBusinessLicencesAdapter(
        dataset_id=DATASET,
        jurisdiction=JURISDICTION,
        client=client,
        clock=lambda: PINNED_NOW,
        page_size=page_size,
        app_token=app_token,
    )


def _record(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "business_licence_category": "Restaurant or Food Service",
        "business_name": "PRAIRIE CAFE",
        "externalid": "100031017-001",
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
            adapter = EdmontonBusinessLicencesAdapter(
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
    async def test_builds_snapshot_with_count_probe(self) -> None:
        requests: list[httpx.Request] = []

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=_capture_sequence(
                    requests,
                    [
                        httpx.Response(200, json=[{"count": "42"}]),
                        httpx.Response(200, json=[]),
                    ],
                )
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                yielded = [r async for r in result.records]

            assert result.snapshot.record_count == 42
            assert result.snapshot.fetched_at == PINNED_NOW
            assert result.snapshot.source_url == RESOURCE_URL
            assert yielded == []
            assert requests[0].url.params["$select"] == "count(*)"

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
        records = [_record(externalid="100031017-001"), _record(externalid="100031018-001")]

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[
                    httpx.Response(200, json=[{"count": "2"}]),
                    httpx.Response(200, json=records),
                ]
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                yielded = [r async for r in result.records]

            assert [r.source_record_id for r in yielded] == [
                "100031017-001",
                "100031018-001",
            ]
            assert [r.raw_data["business_name"] for r in yielded] == [
                "PRAIRIE CAFE",
                "PRAIRIE CAFE",
            ]

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
        raw = _record(ward="O-day'min", **{":@computed_region_extra": "20"})

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

            assert "ward" in yielded[0].raw_data
            assert all(not key.startswith(":@computed_region_") for key in yielded[0].raw_data)

    async def test_missing_externalid_leaves_source_record_id_none(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[
                    httpx.Response(200, json=[{"count": "1"}]),
                    httpx.Response(200, json=[_record(externalid=None)]),
                ]
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                yielded = [r async for r in result.records]

            assert yielded[0].source_record_id is None


class TestPagination:
    async def test_walks_pages_until_short_page_terminates(self) -> None:
        page_one = [_record(externalid="100031017-001"), _record(externalid="100031018-001")]
        page_two = [_record(externalid="100031019-001")]
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
                "100031017-001",
                "100031018-001",
                "100031019-001",
            ]
            assert requests[1].url.params["$offset"] == "0"
            assert requests[2].url.params["$offset"] == "2"


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
