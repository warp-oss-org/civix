"""Unit tests for CalgaryBusinessLicencesAdapter against Socrata-shaped responses."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
import respx

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction
from civix.core.ports.errors import FetchError
from civix.domains.business_licences.adapters.sources.ca.calgary import (
    DEFAULT_BASE_URL,
    DEFAULT_PAGE_SIZE,
    SOURCE_ID,
    CalgaryBusinessLicencesAdapter,
)

PINNED_NOW = datetime(2026, 4, 29, 12, 0, tzinfo=UTC)
DATASET = DatasetId("vdjc-pybd")
JURISDICTION = Jurisdiction(country="CA", region="AB", locality="Calgary")
RESOURCE_URL = f"{DEFAULT_BASE_URL}{DATASET}.json"


def _adapter(
    client: httpx.AsyncClient,
    *,
    page_size: int = 1000,
    app_token: str | None = None,
) -> CalgaryBusinessLicencesAdapter:
    return CalgaryBusinessLicencesAdapter(
        dataset_id=DATASET,
        jurisdiction=JURISDICTION,
        client=client,
        clock=lambda: PINNED_NOW,
        page_size=page_size,
        app_token=app_token,
    )


def _record(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "getbusid": "100001",
        "tradename": "PRAIRIE CAFE",
        "jobstatusdesc": "Licensed",
        ":@computed_region_4a3i_ccfj": "10",
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
            adapter = CalgaryBusinessLicencesAdapter(
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
        records = [_record(getbusid="100001"), _record(getbusid="100002")]

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

            assert [r.source_record_id for r in yielded] == ["100001", "100002"]
            assert [r.raw_data["tradename"] for r in yielded] == ["PRAIRIE CAFE", "PRAIRIE CAFE"]

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
        raw = _record(
            globalid="11111111-1111-1111-1111-111111111111",
            **{":@computed_region_p8tp_5dkv": "20"},
        )

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

            assert "globalid" in yielded[0].raw_data
            assert all(not key.startswith(":@computed_region_") for key in yielded[0].raw_data)

    async def test_missing_getbusid_leaves_source_record_id_none(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[
                    httpx.Response(200, json=[{"count": "1"}]),
                    httpx.Response(200, json=[_record(getbusid=None)]),
                ]
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                yielded = [r async for r in result.records]

            assert yielded[0].source_record_id is None


class TestPagination:
    async def test_walks_pages_until_short_page_terminates(self) -> None:
        page_one = [_record(getbusid="100001"), _record(getbusid="100002")]
        page_two = [_record(getbusid="100003")]
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

            assert [r.source_record_id for r in yielded] == ["100001", "100002", "100003"]
            assert requests[1].url.params["$limit"] == "2"
            assert requests[1].url.params["$offset"] == "0"
            assert requests[2].url.params["$offset"] == "2"

    async def test_stops_after_empty_first_page(self) -> None:
        requests: list[httpx.Request] = []

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=_capture_sequence(
                    requests,
                    [
                        httpx.Response(200, json=[{"count": "0"}]),
                        httpx.Response(200, json=[]),
                    ],
                )
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client, page_size=2).fetch()
                yielded = [r async for r in result.records]

            assert yielded == []
            assert len(requests) == 2


class TestAppToken:
    async def test_app_token_reaches_count_and_records_requests(self) -> None:
        requests: list[httpx.Request] = []

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=_capture_sequence(
                    requests,
                    [
                        httpx.Response(200, json=[{"count": "1"}]),
                        httpx.Response(200, json=[_record()]),
                    ],
                )
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client, app_token="test-token").fetch()
                _ = [r async for r in result.records]

            assert requests[0].headers["X-App-Token"] == "test-token"
            assert requests[1].headers["X-App-Token"] == "test-token"

    async def test_app_token_omitted_by_default(self) -> None:
        requests: list[httpx.Request] = []

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=_capture_sequence(
                    requests,
                    [
                        httpx.Response(200, json=[{"count": "0"}]),
                        httpx.Response(200, json=[]),
                    ],
                )
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                _ = [r async for r in result.records]

            assert "X-App-Token" not in requests[0].headers
            assert "X-App-Token" not in requests[1].headers


class TestFetchErrors:
    async def test_count_http_error_raises_count_operation(self) -> None:
        async with respx.mock() as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(return_value=httpx.Response(500))

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError) as excinfo:
                    await _adapter(client).fetch()

            assert excinfo.value.operation == "count"

    async def test_count_non_json_raises(self) -> None:
        async with respx.mock() as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(return_value=httpx.Response(200, text="not json"))

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="non-JSON"):
                    await _adapter(client).fetch()

    @pytest.mark.parametrize(
        "payload",
        [
            {},
            [],
            [{"total": "1"}],
            [{"count": "-1"}],
            [{"count": "abc"}],
        ],
    )
    async def test_malformed_count_response_raises(self, payload: object) -> None:
        async with respx.mock() as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(return_value=httpx.Response(200, json=payload))

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="count"):
                    await _adapter(client).fetch()

    async def test_records_non_json_raises(self) -> None:
        async with respx.mock() as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[
                    httpx.Response(200, json=[{"count": "1"}]),
                    httpx.Response(200, text="not json"),
                ]
            )

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="non-JSON"):
                    await _adapter(client).fetch()

    async def test_records_non_list_raises(self) -> None:
        async with respx.mock() as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[
                    httpx.Response(200, json=[{"count": "1"}]),
                    httpx.Response(200, json={"records": []}),
                ]
            )

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="non-list"):
                    await _adapter(client).fetch()

    async def test_second_page_http_error_raises_during_streaming(self) -> None:
        async with respx.mock() as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[
                    httpx.Response(200, json=[{"count": "3"}]),
                    httpx.Response(200, json=[_record(getbusid="1"), _record(getbusid="2")]),
                    httpx.Response(500),
                ]
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client, page_size=2).fetch()

                with pytest.raises(FetchError) as excinfo:
                    _ = [r async for r in result.records]

            assert excinfo.value.operation == "fetch-page"

    async def test_record_that_is_not_object_raises(self) -> None:
        async with respx.mock() as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[
                    httpx.Response(200, json=[{"count": "1"}]),
                    httpx.Response(200, json=[[1, 2, 3]]),
                ]
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()

                with pytest.raises(FetchError, match="non-object record"):
                    _ = [r async for r in result.records]


class TestBaseUrlOverride:
    async def test_custom_base_url_used(self) -> None:
        custom_base = "https://example.test/resource/"
        custom_url = f"{custom_base}{DATASET}.json"

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(custom_url).mock(
                side_effect=[
                    httpx.Response(200, json=[{"count": "0"}]),
                    httpx.Response(200, json=[]),
                ]
            )

            async with httpx.AsyncClient() as client:
                adapter = CalgaryBusinessLicencesAdapter(
                    dataset_id=DATASET,
                    jurisdiction=JURISDICTION,
                    client=client,
                    clock=lambda: PINNED_NOW,
                    base_url=custom_base,
                )
                result = await adapter.fetch()
                _ = [r async for r in result.records]

            assert result.snapshot.source_url == custom_url
