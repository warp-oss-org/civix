"""Tests for the shared CKAN datastore fetch helper."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
import respx

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, SourceId
from civix.core.ports.errors import FetchError
from civix.core.ports.models.adapter import SourceAdapter
from civix.infra.sources.ckan import (
    DEFAULT_BASE_URL,
    CkanDatasetConfig,
    CkanFetchConfig,
    CkanSourceAdapter,
    fetch_ckan_dataset,
    fetch_ckan_static_json_resource,
)

PINNED_NOW = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
DATASET_ID = DatasetId("example-package")
SOURCE_ID = SourceId("example-ckan")
JURISDICTION = Jurisdiction(country="CA", region="ON", locality="Toronto")
RESOURCE_ID = "resource-1"
PACKAGE_SHOW_URL = f"{DEFAULT_BASE_URL}package_show"
DATASTORE_SEARCH_URL = f"{DEFAULT_BASE_URL}datastore_search"
STATIC_RESOURCE_URL = "https://example.test/static/project-list.json"


def _dataset(
    source_record_id_fields: tuple[str, ...] = ("collision_id", "per_no"),
    resource_name: str | None = None,
) -> CkanDatasetConfig:
    return CkanDatasetConfig(
        source_id=SOURCE_ID,
        dataset_id=DATASET_ID,
        jurisdiction=JURISDICTION,
        source_record_id_fields=source_record_id_fields,
        resource_name=resource_name,
    )


def _fetch(client: httpx.AsyncClient, *, page_size: int = 2) -> CkanFetchConfig:
    return CkanFetchConfig(client=client, clock=lambda: PINNED_NOW, page_size=page_size)


def _package_payload(*, datastore_active: bool = True) -> dict[str, Any]:
    return {
        "success": True,
        "result": {
            "resources": [
                {"id": "inactive", "name": "inactive-resource", "datastore_active": False},
                {
                    "id": RESOURCE_ID,
                    "name": "target-resource",
                    "datastore_active": datastore_active,
                },
            ]
        },
    }


def _static_package_payload(*, language: list[str] | None = None) -> dict[str, Any]:
    return {
        "success": True,
        "result": {
            "resources": [
                {
                    "id": "csv-resource",
                    "name": "Project List",
                    "format": "CSV",
                    "language": ["en", "fr"],
                    "url": "https://example.test/static/project-list.csv",
                },
                {
                    "id": RESOURCE_ID,
                    "name": "Project List",
                    "format": "JSON",
                    "language": language or ["en", "fr"],
                    "url": STATIC_RESOURCE_URL,
                },
            ]
        },
    }


def _datastore_payload(*, total: int, records: list[dict[str, Any]]) -> dict[str, Any]:
    return {"success": True, "result": {"total": total, "records": records}}


def _capture_sequence(
    requests: list[httpx.Request],
    responses: list[httpx.Response],
) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)

        return responses.pop(0)

    return handler


class TestCkanFetch:
    async def test_source_adapter_exposes_protocol_metadata_and_fetches(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(DATASTORE_SEARCH_URL).mock(
                return_value=httpx.Response(
                    200,
                    json=_datastore_payload(
                        total=1,
                        records=[{"collision_id": "100", "per_no": "1"}],
                    ),
                )
            )

            async with httpx.AsyncClient() as client:
                adapter = CkanSourceAdapter(dataset=_dataset(), fetch_config=_fetch(client))
                result = await adapter.fetch()
                records = [r async for r in result.records]

        assert isinstance(adapter, SourceAdapter)
        assert adapter.source_id == SOURCE_ID
        assert adapter.dataset_id == DATASET_ID
        assert adapter.jurisdiction == JURISDICTION
        assert result.snapshot.fetch_params == {"resource_id": RESOURCE_ID}
        assert [r.source_record_id for r in records] == ["100:1"]

    async def test_resolves_resource_and_pages_records(self) -> None:
        requests: list[httpx.Request] = []

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(DATASTORE_SEARCH_URL).mock(
                side_effect=_capture_sequence(
                    requests,
                    [
                        httpx.Response(
                            200,
                            json=_datastore_payload(
                                total=3,
                                records=[
                                    {"collision_id": "100", "per_no": "1"},
                                    {"collision_id": "100", "per_no": "2"},
                                ],
                            ),
                        ),
                        httpx.Response(
                            200,
                            json=_datastore_payload(
                                total=3,
                                records=[{"collision_id": "101", "per_no": "1"}],
                            ),
                        ),
                    ],
                )
            )

            async with httpx.AsyncClient() as client:
                result = await fetch_ckan_dataset(dataset=_dataset(), fetch=_fetch(client))
                records = [r async for r in result.records]

        assert result.snapshot.record_count == 3
        assert result.snapshot.source_url is not None
        assert RESOURCE_ID in result.snapshot.source_url
        assert requests[0].url.params["offset"] == "0"
        assert requests[1].url.params["offset"] == "2"
        assert [r.source_record_id for r in records] == ["100:1", "100:2", "101:1"]

    async def test_resolves_named_resource(self) -> None:
        requests: list[httpx.Request] = []

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(DATASTORE_SEARCH_URL).mock(
                side_effect=_capture_sequence(
                    requests,
                    [
                        httpx.Response(
                            200,
                            json=_datastore_payload(
                                total=1,
                                records=[{"collision_id": "100", "per_no": "1"}],
                            ),
                        ),
                    ],
                )
            )

            async with httpx.AsyncClient() as client:
                result = await fetch_ckan_dataset(
                    dataset=_dataset(resource_name="target-resource"),
                    fetch=_fetch(client),
                )
                records = [r async for r in result.records]

        assert requests[0].url.params["resource_id"] == RESOURCE_ID
        assert result.snapshot.fetch_params == {"resource_id": RESOURCE_ID}
        assert [r.source_record_id for r in records] == ["100:1"]

    async def test_page_size_must_be_positive(self) -> None:
        async with httpx.AsyncClient() as client:
            with pytest.raises(ValueError, match="page_size"):
                _fetch(client, page_size=0)

    async def test_fetches_named_static_json_resource(self) -> None:
        payload = {"indexTitles": ["projectNumber"], "data": [["1"]]}

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_static_package_payload())
            )
            respx_mock.get(STATIC_RESOURCE_URL).mock(return_value=httpx.Response(200, json=payload))

            async with httpx.AsyncClient() as client:
                resource = await fetch_ckan_static_json_resource(
                    dataset=_dataset(),
                    fetch=_fetch(client),
                    resource_name="Project List",
                    resource_format="JSON",
                    languages=("en", "fr"),
                )

        assert resource.resource_id == RESOURCE_ID
        assert resource.resource_name == "Project List"
        assert resource.resource_format == "JSON"
        assert resource.resource_url == STATIC_RESOURCE_URL
        assert resource.payload == payload

    async def test_static_json_resource_requires_language_match(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(
                    200,
                    json=_static_package_payload(language=["en"]),
                )
            )

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="static resource"):
                    await fetch_ckan_static_json_resource(
                        dataset=_dataset(),
                        fetch=_fetch(client),
                        resource_name="Project List",
                        resource_format="JSON",
                        languages=("en", "fr"),
                    )

    async def test_static_json_resource_non_json_raises_fetch_error(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_static_package_payload())
            )
            respx_mock.get(STATIC_RESOURCE_URL).mock(httpx.Response(200, text="not json"))

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="non-JSON response"):
                    await fetch_ckan_static_json_resource(
                        dataset=_dataset(),
                        fetch=_fetch(client),
                        resource_name="Project List",
                        resource_format="JSON",
                        languages=("en", "fr"),
                    )

    async def test_no_datastore_resource_raises_fetch_error(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload(datastore_active=False))
            )

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="no datastore-active resource"):
                    await fetch_ckan_dataset(dataset=_dataset(), fetch=_fetch(client))

    async def test_missing_named_resource_raises_clear_fetch_error(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="missing-resource"):
                    await fetch_ckan_dataset(
                        dataset=_dataset(resource_name="missing-resource"),
                        fetch=_fetch(client),
                    )

    async def test_inactive_named_resource_raises_clear_fetch_error(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="not active"):
                    await fetch_ckan_dataset(
                        dataset=_dataset(resource_name="inactive-resource"),
                        fetch=_fetch(client),
                    )

    async def test_non_object_record_raises_when_streamed(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(
                return_value=httpx.Response(200, json=_package_payload())
            )
            respx_mock.get(DATASTORE_SEARCH_URL).mock(
                return_value=httpx.Response(
                    200,
                    json={"success": True, "result": {"total": 1, "records": [["bad"]]}},
                )
            )

            async with httpx.AsyncClient() as client:
                result = await fetch_ckan_dataset(dataset=_dataset(), fetch=_fetch(client))

                with pytest.raises(FetchError, match="non-object record"):
                    _ = [r async for r in result.records]
