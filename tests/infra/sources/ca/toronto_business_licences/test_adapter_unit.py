"""Unit tests for TorontoBusinessLicencesAdapter against handcrafted respx routes."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
import respx

from civix.core.adapters import FetchError
from civix.core.identity import DatasetId, Jurisdiction
from civix.infra.sources.ca.toronto_business_licences import (
    DEFAULT_BASE_URL,
    SOURCE_ID,
    TorontoBusinessLicencesAdapter,
)

PINNED_NOW = datetime(2026, 4, 28, 12, 0, tzinfo=UTC)
DATASET = DatasetId("municipal-licensing-and-standards-business-licences-and-permits")
JURISDICTION = Jurisdiction(country="CA", region="ON", locality="Toronto")
RESOURCE_ID = "169e90ba-3ae0-43dd-8b2f-919e87002f50"
PACKAGE_SHOW_URL = f"{DEFAULT_BASE_URL}package_show"
DATASTORE_SEARCH_URL = f"{DEFAULT_BASE_URL}datastore_search"


def _adapter(client: httpx.AsyncClient, *, page_size: int = 1000) -> TorontoBusinessLicencesAdapter:
    return TorontoBusinessLicencesAdapter(
        dataset_id=DATASET,
        jurisdiction=JURISDICTION,
        client=client,
        clock=lambda: PINNED_NOW,
        page_size=page_size,
    )


def _package_payload(
    *, resource_id: str = RESOURCE_ID, datastore_active: bool = True
) -> dict[str, Any]:
    return {
        "success": True,
        "result": {
            "id": "57b2285f-4f80-45fb-ae3e-41a02c3a137f",
            "name": str(DATASET),
            "resources": [
                {
                    "id": "non-active-1",
                    "format": "JSON",
                    "datastore_active": False,
                },
                {
                    "id": resource_id,
                    "format": "CSV",
                    "datastore_active": datastore_active,
                },
            ],
        },
    }


def _datastore_payload(*, total: int, records: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "success": True,
        "result": {
            "resource_id": RESOURCE_ID,
            "total": total,
            "records": records,
        },
    }


class TestProperties:
    async def test_source_id_is_constant(self) -> None:
        async with httpx.AsyncClient() as client:
            adapter = _adapter(client)

            assert adapter.source_id == SOURCE_ID

    async def test_dataset_id_and_jurisdiction_round_trip(self) -> None:
        async with httpx.AsyncClient() as client:
            adapter = _adapter(client)

            assert adapter.dataset_id == DATASET
            assert adapter.jurisdiction == JURISDICTION

    async def test_page_size_must_be_positive(self) -> None:
        async with httpx.AsyncClient() as client:
            with pytest.raises(ValueError, match="page_size"):
                _adapter(client, page_size=0)


class TestFetchHappyPath:
    async def test_builds_snapshot_with_total_from_first_page(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(DATASTORE_SEARCH_URL).mock(
                return_value=httpx.Response(
                    200,
                    json=_datastore_payload(total=42, records=[]),
                )
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                _ = [r async for r in result.records]

            assert result.snapshot.record_count == 42
            assert result.snapshot.fetched_at == PINNED_NOW
            assert RESOURCE_ID in (result.snapshot.source_url or "")

    async def test_snapshot_id_is_deterministic_given_clock(self) -> None:
        async with respx.mock(assert_all_called=False) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(DATASTORE_SEARCH_URL).mock(
                return_value=httpx.Response(
                    200,
                    json=_datastore_payload(total=0, records=[]),
                )
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()

            assert result.snapshot.snapshot_id == (
                f"{SOURCE_ID}:{DATASET}:{PINNED_NOW.isoformat()}"
            )

    async def test_streams_records_from_first_page(self) -> None:
        records = [
            {"_id": 1, "Licence No.": "B-1", "Operating Name": "A"},
            {"_id": 2, "Licence No.": "B-2", "Operating Name": "B"},
            {"_id": 3, "Licence No.": "B-3", "Operating Name": "C"},
        ]

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(DATASTORE_SEARCH_URL).mock(
                return_value=httpx.Response(
                    200,
                    json=_datastore_payload(total=3, records=records),
                )
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                yielded = [r async for r in result.records]

            assert [r.raw_data["Operating Name"] for r in yielded] == ["A", "B", "C"]

    async def test_preserves_ckan_internal_id_in_raw_data(self) -> None:
        records = [{"_id": 7, "Licence No.": "B-1", "Operating Name": "A"}]

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(DATASTORE_SEARCH_URL).mock(
                return_value=httpx.Response(
                    200,
                    json=_datastore_payload(total=1, records=records),
                )
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                yielded = [r async for r in result.records]

            assert yielded[0].raw_data["_id"] == 7

    async def test_source_record_id_extracted_from_licence_no(self) -> None:
        records = [{"_id": 1, "Licence No.": "B02-4741962", "Operating Name": "TAXIFY"}]

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(DATASTORE_SEARCH_URL).mock(
                return_value=httpx.Response(
                    200,
                    json=_datastore_payload(total=1, records=records),
                )
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                yielded = [r async for r in result.records]

            assert yielded[0].source_record_id == "B02-4741962"

    async def test_source_record_id_is_none_when_licence_no_missing(self) -> None:
        records = [{"_id": 1, "Operating Name": "X"}]

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(DATASTORE_SEARCH_URL).mock(
                return_value=httpx.Response(
                    200,
                    json=_datastore_payload(total=1, records=records),
                )
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                yielded = [r async for r in result.records]

            assert yielded[0].source_record_id is None

    async def test_source_updated_at_is_none_for_date_only_field(self) -> None:
        # Toronto's Last Record Update is date-only with no time/zone, so the
        # adapter declines to fabricate a UTC datetime.
        records = [
            {
                "_id": 1,
                "Licence No.": "B-1",
                "Last Record Update": "2018-12-07",
            }
        ]

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(DATASTORE_SEARCH_URL).mock(
                return_value=httpx.Response(
                    200,
                    json=_datastore_payload(total=1, records=records),
                )
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                yielded = [r async for r in result.records]

            assert yielded[0].source_updated_at is None

    async def test_empty_dataset_yields_no_records(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(DATASTORE_SEARCH_URL).mock(
                return_value=httpx.Response(
                    200,
                    json=_datastore_payload(total=0, records=[]),
                )
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                yielded = [r async for r in result.records]

            assert result.snapshot.record_count == 0
            assert yielded == []


class TestPagination:
    async def test_walks_pages_until_short_page_terminates(self) -> None:
        page_one = [{"_id": i, "Licence No.": f"B-{i}"} for i in range(1, 3)]
        page_two = [{"_id": i, "Licence No.": f"B-{i}"} for i in range(3, 5)]
        page_three = [{"_id": 5, "Licence No.": "B-5"}]

        responses = [
            httpx.Response(200, json=_datastore_payload(total=5, records=page_one)),
            httpx.Response(200, json=_datastore_payload(total=5, records=page_two)),
            httpx.Response(200, json=_datastore_payload(total=5, records=page_three)),
        ]

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(DATASTORE_SEARCH_URL).mock(side_effect=responses)

            async with httpx.AsyncClient() as client:
                result = await _adapter(client, page_size=2).fetch()
                yielded = [r async for r in result.records]

            assert [r.raw_data["Licence No."] for r in yielded] == [
                "B-1",
                "B-2",
                "B-3",
                "B-4",
                "B-5",
            ]

    async def test_stops_immediately_when_first_page_returns_empty_records(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(DATASTORE_SEARCH_URL).mock(
                return_value=httpx.Response(
                    200,
                    json=_datastore_payload(total=0, records=[]),
                )
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client, page_size=2).fetch()
                yielded = [r async for r in result.records]

            assert yielded == []

    async def test_offset_param_advances_between_pages(self) -> None:
        page_one = [{"_id": i, "Licence No.": f"B-{i}"} for i in range(1, 3)]
        page_two: list[dict[str, Any]] = []  # short page → terminate

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            search_route = respx_mock.get(DATASTORE_SEARCH_URL).mock(
                side_effect=[
                    httpx.Response(200, json=_datastore_payload(total=3, records=page_one)),
                    httpx.Response(200, json=_datastore_payload(total=3, records=page_two)),
                ]
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client, page_size=2).fetch()
                _ = [r async for r in result.records]

            assert len(search_route.calls) == 2
            last_request = search_route.calls.last.request

            assert last_request.url.params["offset"] == "2"

    async def test_stops_at_total_without_extra_empty_page(self) -> None:
        page_one = [{"_id": i, "Licence No.": f"B-{i}"} for i in range(1, 3)]
        page_two = [{"_id": i, "Licence No.": f"B-{i}"} for i in range(3, 5)]

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            search_route = respx_mock.get(DATASTORE_SEARCH_URL).mock(
                side_effect=[
                    httpx.Response(200, json=_datastore_payload(total=4, records=page_one)),
                    httpx.Response(200, json=_datastore_payload(total=4, records=page_two)),
                ]
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client, page_size=2).fetch()
                yielded = [r async for r in result.records]

            assert [r.raw_data["Licence No."] for r in yielded] == [
                "B-1",
                "B-2",
                "B-3",
                "B-4",
            ]
            assert len(search_route.calls) == 2


class TestFetchErrors:
    async def test_package_show_404_raises_resolve_resource(self) -> None:
        async with respx.mock() as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(return_value=httpx.Response(404))

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError) as excinfo:
                    await _adapter(client).fetch()

            assert excinfo.value.operation == "resolve-resource"
            assert excinfo.value.source_id == SOURCE_ID
            assert excinfo.value.dataset_id == DATASET
            assert isinstance(excinfo.value.__cause__, httpx.HTTPStatusError)

    async def test_package_show_success_false_raises(self) -> None:
        async with respx.mock() as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json={"success": False, "error": {}})
            )

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="success=False"):
                    await _adapter(client).fetch()

    async def test_package_show_non_json_raises(self) -> None:
        async with respx.mock() as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(return_value=httpx.Response(200, text="not json"))

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="non-JSON"):
                    await _adapter(client).fetch()

    async def test_no_datastore_active_resource_raises(self) -> None:
        async with respx.mock() as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload(datastore_active=False))
            )

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="no datastore-active resource"):
                    await _adapter(client).fetch()

    async def test_datastore_search_500_on_first_page_raises(self) -> None:
        async with respx.mock() as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(DATASTORE_SEARCH_URL).mock(return_value=httpx.Response(500))

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError) as excinfo:
                    await _adapter(client).fetch()

            assert excinfo.value.operation == "fetch-page"

    async def test_missing_total_in_response_raises(self) -> None:
        async with respx.mock() as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(DATASTORE_SEARCH_URL).mock(
                return_value=httpx.Response(
                    200,
                    json={"success": True, "result": {"records": []}},
                )
            )

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="missing or invalid total"):
                    await _adapter(client).fetch()

    async def test_datastore_search_500_on_second_page_raises_during_streaming(self) -> None:
        first_page = [{"_id": i, "Licence No.": f"B-{i}"} for i in range(1, 3)]

        async with respx.mock() as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(DATASTORE_SEARCH_URL).mock(
                side_effect=[
                    httpx.Response(200, json=_datastore_payload(total=10, records=first_page)),
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
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(DATASTORE_SEARCH_URL).mock(
                return_value=httpx.Response(
                    200,
                    json={
                        "success": True,
                        "result": {"resource_id": RESOURCE_ID, "total": 1, "records": [[1, 2, 3]]},
                    },
                )
            )

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()

                with pytest.raises(FetchError, match="non-object record"):
                    _ = [r async for r in result.records]


class TestBaseUrlOverride:
    async def test_custom_base_url_used_for_both_endpoints(self) -> None:
        custom_base = "https://example.test/api/3/action/"
        custom_package = f"{custom_base}package_show"
        custom_search = f"{custom_base}datastore_search"

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(custom_package).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(custom_search).mock(
                return_value=httpx.Response(
                    200,
                    json=_datastore_payload(total=0, records=[]),
                )
            )

            async with httpx.AsyncClient() as client:
                adapter = TorontoBusinessLicencesAdapter(
                    dataset_id=DATASET,
                    jurisdiction=JURISDICTION,
                    client=client,
                    clock=lambda: PINNED_NOW,
                    base_url=custom_base,
                )
                result = await adapter.fetch()
                _ = [r async for r in result.records]

            assert custom_base in (result.snapshot.source_url or "")


class TestUserAgentOnWire:
    async def test_user_agent_reaches_wire(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            package_route = respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(DATASTORE_SEARCH_URL).mock(
                return_value=httpx.Response(
                    200,
                    json=_datastore_payload(total=0, records=[]),
                )
            )

            async with httpx.AsyncClient(headers={"User-Agent": "civix-test/0.0.0"}) as client:
                result = await _adapter(client).fetch()
                _ = [r async for r in result.records]

            sent = package_route.calls.last.request

            assert sent.headers["User-Agent"] == "civix-test/0.0.0"
