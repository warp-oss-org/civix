"""Tests for the shared CSV byte-acquisition helper."""

from __future__ import annotations

import httpx
import pytest
import respx

from civix.core.identity.models.identifiers import DatasetId, SourceId
from civix.core.ports.errors import FetchError
from civix.infra.sources.csv import fetch_csv_bytes

SOURCE_ID = SourceId("example-portal")
DATASET_ID = DatasetId("example-table-2024")
CSV_URL = "https://example.test/data/table.csv"
REDIRECT_URL = "https://storage.example.test/signed/table.csv"
CSV_BODY = b"id,name\n1,alpha\n2,beta\n"


class TestFetchCsvBytes:
    async def test_returns_response_body_on_success(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(CSV_URL).mock(
                return_value=httpx.Response(200, content=CSV_BODY),
            )

            async with httpx.AsyncClient() as client:
                content = await fetch_csv_bytes(
                    client,
                    CSV_URL,
                    source_id=SOURCE_ID,
                    dataset_id=DATASET_ID,
                )

        assert content == CSV_BODY

    async def test_http_error_raises_fetch_error_with_context(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(CSV_URL).mock(return_value=httpx.Response(500))

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError) as excinfo:
                    await fetch_csv_bytes(
                        client,
                        CSV_URL,
                        source_id=SOURCE_ID,
                        dataset_id=DATASET_ID,
                        error_message=f"failed to read EXAMPLE CSV from {CSV_URL}",
                    )

        assert "failed to read EXAMPLE CSV" in str(excinfo.value)
        assert excinfo.value.source_id == SOURCE_ID
        assert excinfo.value.dataset_id == DATASET_ID
        assert excinfo.value.operation == "fetch-csv"
        assert isinstance(excinfo.value.__cause__, httpx.HTTPError)

    async def test_default_error_message_when_none_provided(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(CSV_URL).mock(return_value=httpx.Response(503))

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError) as excinfo:
                    await fetch_csv_bytes(
                        client,
                        CSV_URL,
                        source_id=SOURCE_ID,
                        dataset_id=DATASET_ID,
                    )

        assert f"failed to read CSV from {CSV_URL}" in str(excinfo.value)

    async def test_operation_override_propagates_to_fetch_error(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(CSV_URL).mock(return_value=httpx.Response(404))

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError) as excinfo:
                    await fetch_csv_bytes(
                        client,
                        CSV_URL,
                        source_id=SOURCE_ID,
                        dataset_id=DATASET_ID,
                        operation="download-csv",
                    )

        assert excinfo.value.operation == "download-csv"

    async def test_follow_redirects_disabled_by_default(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(CSV_URL).mock(
                return_value=httpx.Response(302, headers={"location": REDIRECT_URL})
            )

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError):
                    await fetch_csv_bytes(
                        client,
                        CSV_URL,
                        source_id=SOURCE_ID,
                        dataset_id=DATASET_ID,
                    )

    async def test_follow_redirects_true_follows_to_final_body(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            initial_route = respx_mock.get(CSV_URL).mock(
                return_value=httpx.Response(302, headers={"location": REDIRECT_URL})
            )
            redirect_route = respx_mock.get(REDIRECT_URL).mock(
                return_value=httpx.Response(200, content=CSV_BODY),
            )

            async with httpx.AsyncClient() as client:
                content = await fetch_csv_bytes(
                    client,
                    CSV_URL,
                    source_id=SOURCE_ID,
                    dataset_id=DATASET_ID,
                    follow_redirects=True,
                )

        assert content == CSV_BODY
        assert initial_route.called
        assert redirect_route.called
