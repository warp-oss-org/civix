"""Tests for the shared OpenFEMA fetch helper."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

import httpx
import pytest
import respx

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, SourceId
from civix.core.ports.errors import FetchError
from civix.core.ports.models.adapter import SourceAdapter
from civix.infra.sources.openfema import (
    DEFAULT_BASE_URL,
    MAX_PAGE_SIZE,
    OpenFemaDatasetConfig,
    OpenFemaFetchConfig,
    OpenFemaSourceAdapter,
    fetch_openfema_dataset,
)

PINNED_NOW = datetime(2026, 5, 2, 12, 0, tzinfo=UTC)
DATASET_ID = DatasetId("ExampleEntity")
SOURCE_ID = SourceId("openfema-test")
JURISDICTION = Jurisdiction(country="US")
RESOURCE_URL = f"{DEFAULT_BASE_URL}v1/{DATASET_ID}"


def _dataset(source_record_id_fields: tuple[str, ...] = ("record_id",)) -> OpenFemaDatasetConfig:
    return OpenFemaDatasetConfig(
        source_id=SOURCE_ID,
        dataset_id=DATASET_ID,
        jurisdiction=JURISDICTION,
        version="v1",
        entity=str(DATASET_ID),
        source_record_id_fields=source_record_id_fields,
    )


def _fetch(
    client: httpx.AsyncClient,
    *,
    page_size: int = 2,
    order_by: str | None = "record_id",
) -> OpenFemaFetchConfig:
    return OpenFemaFetchConfig(
        client=client,
        clock=lambda: PINNED_NOW,
        page_size=page_size,
        order_by=order_by,
    )


def _payload(*, count: int, rows: list[object]) -> dict[str, object]:
    return {"metadata": {"count": count}, str(DATASET_ID): rows}


def _capture_sequence(
    requests: list[httpx.Request],
    responses: list[httpx.Response],
) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)

        return responses.pop(0)

    return handler


class TestOpenFemaFetch:
    async def test_source_adapter_exposes_protocol_metadata_and_fetches(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                return_value=httpx.Response(
                    200,
                    json=_payload(count=1, rows=[{"record_id": "row-1"}]),
                )
            )

            async with httpx.AsyncClient() as client:
                adapter = OpenFemaSourceAdapter(dataset=_dataset(), fetch_config=_fetch(client))
                result = await adapter.fetch()
                records = [record async for record in result.records]

        assert isinstance(adapter, SourceAdapter)
        assert adapter.source_id == SOURCE_ID
        assert adapter.dataset_id == DATASET_ID
        assert adapter.jurisdiction == JURISDICTION
        assert result.snapshot.dataset_id == DATASET_ID
        assert [record.source_record_id for record in records] == ["row-1"]

    async def test_first_page_count_params_and_raw_record_shape(self) -> None:
        requests: list[httpx.Request] = []

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=_capture_sequence(
                    requests,
                    [
                        httpx.Response(
                            200,
                            json=_payload(
                                count=1,
                                rows=[{"record_id": "row-1", "name": "Kept"}],
                            ),
                        ),
                    ],
                )
            )

            async with httpx.AsyncClient() as client:
                result = await fetch_openfema_dataset(
                    dataset=_dataset(),
                    fetch=OpenFemaFetchConfig(
                        client=client,
                        clock=lambda: PINNED_NOW,
                        page_size=2,
                        order_by="record_id",
                        filter_expr="state eq 'Utah'",
                        select="record_id,name",
                    ),
                )
                records = [record async for record in result.records]

        assert result.snapshot.record_count == 1
        assert result.snapshot.fetch_params == {
            "$top": "2",
            "$orderby": "record_id",
            "$filter": "state eq 'Utah'",
            "$select": "record_id,name",
        }
        assert requests[0].url.params["$count"] == "true"
        assert requests[0].url.params["$skip"] == "0"
        assert requests[0].url.params["$top"] == "2"
        assert requests[0].url.params["$orderby"] == "record_id"
        assert records[0].raw_data == {"record_id": "row-1", "name": "Kept"}

    async def test_pages_until_reported_count_is_reached(self) -> None:
        requests: list[httpx.Request] = []

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=_capture_sequence(
                    requests,
                    [
                        httpx.Response(
                            200,
                            json=_payload(
                                count=3,
                                rows=[{"record_id": "row-1"}, {"record_id": "row-2"}],
                            ),
                        ),
                        httpx.Response(
                            200,
                            json=_payload(count=0, rows=[{"record_id": "row-3"}]),
                        ),
                    ],
                )
            )

            async with httpx.AsyncClient() as client:
                result = await fetch_openfema_dataset(dataset=_dataset(), fetch=_fetch(client))
                records = [record async for record in result.records]

        assert [record.source_record_id for record in records] == ["row-1", "row-2", "row-3"]
        assert requests[0].url.params["$skip"] == "0"
        assert requests[1].url.params["$skip"] == "2"
        assert "$count" not in requests[1].url.params

    async def test_optional_source_record_id_fields_leave_record_id_unset(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                return_value=httpx.Response(
                    200,
                    json=_payload(count=1, rows=[{"record_id": "row-1"}]),
                )
            )

            async with httpx.AsyncClient() as client:
                result = await fetch_openfema_dataset(
                    dataset=_dataset(source_record_id_fields=()),
                    fetch=_fetch(client),
                )
                records = [record async for record in result.records]

        assert records[0].source_record_id is None

    async def test_composite_source_record_id_fields(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                return_value=httpx.Response(
                    200,
                    json=_payload(
                        count=1,
                        rows=[{"project_id": "P-1", "transaction_id": 2}],
                    ),
                )
            )

            async with httpx.AsyncClient() as client:
                result = await fetch_openfema_dataset(
                    dataset=_dataset(source_record_id_fields=("project_id", "transaction_id")),
                    fetch=_fetch(client),
                )
                records = [record async for record in result.records]

        assert records[0].source_record_id == "P-1:2"

    async def test_page_size_bounds_are_enforced(self) -> None:
        async with httpx.AsyncClient() as client:
            with pytest.raises(ValueError, match="page_size"):
                _fetch(client, page_size=0)

            with pytest.raises(ValueError, match=str(MAX_PAGE_SIZE)):
                _fetch(client, page_size=MAX_PAGE_SIZE + 1)

    async def test_non_json_page_raises_fetch_error(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(httpx.Response(200, text="not json"))

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="non-JSON response"):
                    await fetch_openfema_dataset(dataset=_dataset(), fetch=_fetch(client))

    async def test_invalid_count_raises_fetch_error(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                httpx.Response(200, json={"metadata": {"count": "nan"}, str(DATASET_ID): []})
            )

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="missing or invalid count"):
                    await fetch_openfema_dataset(dataset=_dataset(), fetch=_fetch(client))

    async def test_non_list_entity_payload_raises_fetch_error(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                httpx.Response(
                    200,
                    json={"metadata": {"count": 1}, str(DATASET_ID): {"bad": "shape"}},
                )
            )

            async with httpx.AsyncClient() as client:
                result = await fetch_openfema_dataset(dataset=_dataset(), fetch=_fetch(client))

                with pytest.raises(FetchError, match="records list"):
                    _ = [record async for record in result.records]

    async def test_non_object_record_raises_when_streamed(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                httpx.Response(200, json=_payload(count=1, rows=[["bad"]]))
            )

            async with httpx.AsyncClient() as client:
                result = await fetch_openfema_dataset(dataset=_dataset(), fetch=_fetch(client))

                with pytest.raises(FetchError, match="non-object record"):
                    _ = [record async for record in result.records]
