"""Tests for the shared Socrata fetch helper."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

import httpx
import pytest
import respx

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, SourceId
from civix.core.ports.errors import FetchError
from civix.core.ports.models.adapter import SourceAdapter
from civix.infra.sources.socrata import (
    SOCRATA_DEFAULT_ORDER,
    SocrataDatasetConfig,
    SocrataFetchConfig,
    SocrataSourceAdapter,
    fetch_socrata_dataset,
)

PINNED_NOW = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
BASE_URL = "https://example.test/resource/"
DATASET_ID = DatasetId("abcd-1234")
RESOURCE_URL = f"{BASE_URL}{DATASET_ID}.json"
SOURCE_ID = SourceId("example-socrata")
JURISDICTION = Jurisdiction(country="US", region="IL", locality="Chicago")


def _dataset(source_record_id_field: str = "record_id") -> SocrataDatasetConfig:
    return SocrataDatasetConfig(
        source_id=SOURCE_ID,
        dataset_id=DATASET_ID,
        jurisdiction=JURISDICTION,
        base_url=BASE_URL,
        source_record_id_field=source_record_id_field,
    )


def _fetch(
    client: httpx.AsyncClient,
    *,
    page_size: int = 2,
    app_token: str | None = None,
    where: str | None = None,
) -> SocrataFetchConfig:
    return SocrataFetchConfig(
        client=client,
        clock=lambda: PINNED_NOW,
        page_size=page_size,
        app_token=app_token,
        where=where,
    )


def _capture_sequence(
    requests: list[httpx.Request],
    responses: list[httpx.Response],
) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)

        return responses.pop(0)

    return handler


class TestSocrataFetch:
    async def test_source_adapter_exposes_protocol_metadata_and_fetches(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[
                    httpx.Response(200, json=[{"count": "1"}]),
                    httpx.Response(200, json=[{"record_id": "row-1"}]),
                ]
            )

            async with httpx.AsyncClient() as client:
                adapter = SocrataSourceAdapter(dataset=_dataset(), fetch_config=_fetch(client))
                result = await adapter.fetch()
                records = [r async for r in result.records]

        assert isinstance(adapter, SourceAdapter)
        assert adapter.source_id == SOURCE_ID
        assert adapter.dataset_id == DATASET_ID
        assert adapter.jurisdiction == JURISDICTION
        assert result.snapshot.dataset_id == DATASET_ID
        assert [r.source_record_id for r in records] == ["row-1"]

    async def test_count_page_params_headers_and_record_shape(self) -> None:
        requests: list[httpx.Request] = []

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=_capture_sequence(
                    requests,
                    [
                        httpx.Response(200, json=[{"count": "1"}]),
                        httpx.Response(
                            200,
                            json=[
                                {
                                    "record_id": "row-1",
                                    "name": "Kept",
                                    ":@computed_region_x": "dropped",
                                }
                            ],
                        ),
                    ],
                )
            )

            async with httpx.AsyncClient() as client:
                result = await fetch_socrata_dataset(
                    dataset=_dataset(),
                    fetch=_fetch(
                        client,
                        app_token="token",
                        where="status = 'Active'",
                    ),
                )
                records = [r async for r in result.records]

        assert result.snapshot.record_count == 1
        assert result.snapshot.fetch_params == {
            "$order": SOCRATA_DEFAULT_ORDER,
            "$where": "status = 'Active'",
        }
        assert requests[0].url.params["$select"] == "count(*)"
        assert requests[0].url.params["$where"] == "status = 'Active'"
        assert "$order" not in requests[0].url.params
        assert requests[1].url.params["$order"] == SOCRATA_DEFAULT_ORDER
        assert requests[1].url.params["$where"] == "status = 'Active'"
        assert requests[1].headers["X-App-Token"] == "token"
        assert records[0].source_record_id == "row-1"
        assert records[0].raw_data == {"record_id": "row-1", "name": "Kept"}

    async def test_empty_first_page_terminates(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[
                    httpx.Response(200, json=[{"count": "0"}]),
                    httpx.Response(200, json=[]),
                ]
            )

            async with httpx.AsyncClient() as client:
                result = await fetch_socrata_dataset(dataset=_dataset(), fetch=_fetch(client))
                records = [r async for r in result.records]

        assert records == []

    async def test_partial_last_page_terminates_without_extra_fetch(self) -> None:
        requests: list[httpx.Request] = []

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=_capture_sequence(
                    requests,
                    [
                        httpx.Response(200, json=[{"count": "1"}]),
                        httpx.Response(200, json=[{"record_id": "row-1"}]),
                    ],
                )
            )

            async with httpx.AsyncClient() as client:
                result = await fetch_socrata_dataset(dataset=_dataset(), fetch=_fetch(client))
                records = [r async for r in result.records]

        assert [r.source_record_id for r in records] == ["row-1"]
        assert len(requests) == 2

    async def test_full_page_fetches_next_page_until_empty(self) -> None:
        requests: list[httpx.Request] = []

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=_capture_sequence(
                    requests,
                    [
                        httpx.Response(200, json=[{"count": "2"}]),
                        httpx.Response(200, json=[{"record_id": "row-1"}, {"record_id": "row-2"}]),
                        httpx.Response(200, json=[]),
                    ],
                )
            )

            async with httpx.AsyncClient() as client:
                result = await fetch_socrata_dataset(dataset=_dataset(), fetch=_fetch(client))
                records = [r async for r in result.records]

        assert [r.source_record_id for r in records] == ["row-1", "row-2"]
        assert requests[1].url.params["$offset"] == "0"
        assert requests[2].url.params["$offset"] == "2"

    async def test_invalid_page_size_rejected(self) -> None:
        async with httpx.AsyncClient() as client:
            with pytest.raises(ValueError, match="page_size"):
                _fetch(client, page_size=0)

    async def test_non_json_count_raises_fetch_error(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(httpx.Response(200, text="not json"))

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="non-JSON response"):
                    await fetch_socrata_dataset(dataset=_dataset(), fetch=_fetch(client))

    async def test_invalid_count_shape_raises_fetch_error(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(httpx.Response(200, json=[{"count": "nan"}]))

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="missing or invalid count"):
                    await fetch_socrata_dataset(dataset=_dataset(), fetch=_fetch(client))

    async def test_non_list_page_raises_fetch_error(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[
                    httpx.Response(200, json=[{"count": "1"}]),
                    httpx.Response(200, json={"records": []}),
                ]
            )

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="non-list JSON body"):
                    await fetch_socrata_dataset(dataset=_dataset(), fetch=_fetch(client))

    async def test_non_object_record_raises_when_streamed(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[
                    httpx.Response(200, json=[{"count": "1"}]),
                    httpx.Response(200, json=[["not", "object"]]),
                ]
            )

            async with httpx.AsyncClient() as client:
                result = await fetch_socrata_dataset(dataset=_dataset(), fetch=_fetch(client))

                with pytest.raises(FetchError, match="non-object record"):
                    _ = [r async for r in result.records]
