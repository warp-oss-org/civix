"""Unit tests for VancouverBusinessLicencesAdapter against handcrafted respx routes."""

from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest
import respx

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction
from civix.core.ports.errors import FetchError
from civix.domains.business_licences.adapters.sources.ca.vancouver import (
    DEFAULT_BASE_URL,
    SOURCE_ID,
    VancouverBusinessLicencesAdapter,
)

PINNED_NOW = datetime(2026, 4, 25, 12, 0, tzinfo=UTC)
DATASET = DatasetId("business-licences")
JURISDICTION = Jurisdiction(country="CA", region="BC", locality="Vancouver")
RECORDS_URL = f"{DEFAULT_BASE_URL}catalog/datasets/{DATASET}/records"
EXPORTS_URL = f"{DEFAULT_BASE_URL}catalog/datasets/{DATASET}/exports/jsonl"


def _adapter(client: httpx.AsyncClient) -> VancouverBusinessLicencesAdapter:
    return VancouverBusinessLicencesAdapter(
        dataset_id=DATASET,
        jurisdiction=JURISDICTION,
        client=client,
        clock=lambda: PINNED_NOW,
    )


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


class TestFetchHappyPath:
    async def test_builds_snapshot_with_total_count(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RECORDS_URL).mock(
                return_value=httpx.Response(200, json={"total_count": 7, "results": []})
            )
            respx_mock.get(EXPORTS_URL).mock(return_value=httpx.Response(200, text=""))

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                # Drain the records iterator to satisfy assert_all_called.
                _ = [r async for r in result.records]

            assert result.snapshot.record_count == 7
            assert result.snapshot.fetched_at == PINNED_NOW
            assert result.snapshot.source_url == EXPORTS_URL

    async def test_snapshot_id_is_deterministic_given_clock(self) -> None:
        async with respx.mock(assert_all_called=False) as respx_mock:
            respx_mock.get(RECORDS_URL).mock(
                return_value=httpx.Response(200, json={"total_count": 0, "results": []})
            )
            respx_mock.get(EXPORTS_URL).mock(return_value=httpx.Response(200, text=""))

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()

            assert result.snapshot.snapshot_id == (
                f"{SOURCE_ID}:{DATASET}:{PINNED_NOW.isoformat()}"
            )

    async def test_streams_records_lazily(self) -> None:
        body = "\n".join(
            [
                '{"licencersn":"1","businessname":"A"}',
                '{"licencersn":"2","businessname":"B"}',
                '{"licencersn":"3","businessname":"C"}',
            ]
        )

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RECORDS_URL).mock(
                return_value=httpx.Response(200, json={"total_count": 3, "results": []})
            )
            respx_mock.get(EXPORTS_URL).mock(return_value=httpx.Response(200, text=body))

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                records = [r async for r in result.records]

            assert [r.raw_data["businessname"] for r in records] == ["A", "B", "C"]

    async def test_source_record_id_extracted_from_licencersn(self) -> None:
        body = '{"licencersn":"abc-123","businessname":"X"}'

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RECORDS_URL).mock(
                return_value=httpx.Response(200, json={"total_count": 1, "results": []})
            )
            respx_mock.get(EXPORTS_URL).mock(return_value=httpx.Response(200, text=body))

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                records = [r async for r in result.records]

            assert records[0].source_record_id == "abc-123"

    async def test_source_record_id_is_none_when_licencersn_missing(self) -> None:
        body = '{"businessname":"X"}'

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RECORDS_URL).mock(
                return_value=httpx.Response(200, json={"total_count": 1, "results": []})
            )
            respx_mock.get(EXPORTS_URL).mock(return_value=httpx.Response(200, text=body))

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                records = [r async for r in result.records]

            assert records[0].source_record_id is None

    async def test_extract_date_parsed_to_utc_source_updated_at(self) -> None:
        body = '{"licencersn":"1","extractdate":"2026-04-25T00:00:00+00:00"}'

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RECORDS_URL).mock(
                return_value=httpx.Response(200, json={"total_count": 1, "results": []})
            )
            respx_mock.get(EXPORTS_URL).mock(return_value=httpx.Response(200, text=body))

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                records = [r async for r in result.records]

            assert records[0].source_updated_at == datetime(2026, 4, 25, 0, 0, tzinfo=UTC)

    async def test_unparseable_extract_date_becomes_none(self) -> None:
        body = '{"licencersn":"1","extractdate":"not-a-date"}'

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RECORDS_URL).mock(
                return_value=httpx.Response(200, json={"total_count": 1, "results": []})
            )
            respx_mock.get(EXPORTS_URL).mock(return_value=httpx.Response(200, text=body))

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                records = [r async for r in result.records]

            assert records[0].source_updated_at is None

    async def test_blank_lines_in_export_stream_are_skipped(self) -> None:
        body = '\n{"licencersn":"1","businessname":"A"}\n\n{"licencersn":"2","businessname":"B"}\n'

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RECORDS_URL).mock(
                return_value=httpx.Response(200, json={"total_count": 2, "results": []})
            )
            respx_mock.get(EXPORTS_URL).mock(return_value=httpx.Response(200, text=body))

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                records = [r async for r in result.records]

            assert len(records) == 2

    async def test_empty_dataset(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RECORDS_URL).mock(
                return_value=httpx.Response(200, json={"total_count": 0, "results": []})
            )
            respx_mock.get(EXPORTS_URL).mock(return_value=httpx.Response(200, text=""))

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                records = [r async for r in result.records]

            assert result.snapshot.record_count == 0
            assert records == []


class TestFetchErrors:
    async def test_count_request_404_raises_fetch_error(self) -> None:
        async with respx.mock() as respx_mock:
            respx_mock.get(RECORDS_URL).mock(return_value=httpx.Response(404))

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError) as excinfo:
                    await _adapter(client).fetch()

            assert excinfo.value.operation == "count"
            assert excinfo.value.source_id == SOURCE_ID
            assert excinfo.value.dataset_id == DATASET
            assert isinstance(excinfo.value.__cause__, httpx.HTTPStatusError)

    async def test_count_request_returns_invalid_payload(self) -> None:
        async with respx.mock() as respx_mock:
            respx_mock.get(RECORDS_URL).mock(return_value=httpx.Response(200, json={"results": []}))

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="missing or invalid total_count"):
                    await _adapter(client).fetch()

    async def test_count_request_returns_non_json(self) -> None:
        async with respx.mock() as respx_mock:
            respx_mock.get(RECORDS_URL).mock(return_value=httpx.Response(200, text="not json"))

            async with httpx.AsyncClient() as client:
                with pytest.raises(FetchError, match="non-JSON"):
                    await _adapter(client).fetch()

    async def test_export_stream_500_raises_fetch_error(self) -> None:
        async with respx.mock() as respx_mock:
            respx_mock.get(RECORDS_URL).mock(
                return_value=httpx.Response(200, json={"total_count": 1, "results": []})
            )
            respx_mock.get(EXPORTS_URL).mock(return_value=httpx.Response(500))

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                with pytest.raises(FetchError) as excinfo:
                    _ = [r async for r in result.records]

            assert excinfo.value.operation == "stream-records"
            assert isinstance(excinfo.value.__cause__, httpx.HTTPStatusError)

    async def test_malformed_jsonl_line_raises_fetch_error(self) -> None:
        body = '{"licencersn":"1"}\nthis-is-not-json\n'

        async with respx.mock() as respx_mock:
            respx_mock.get(RECORDS_URL).mock(
                return_value=httpx.Response(200, json={"total_count": 2, "results": []})
            )
            respx_mock.get(EXPORTS_URL).mock(return_value=httpx.Response(200, text=body))

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                with pytest.raises(FetchError, match="failed to parse JSONL"):
                    _ = [r async for r in result.records]

    async def test_jsonl_line_that_is_not_an_object_raises(self) -> None:
        body = '{"licencersn":"1"}\n[1,2,3]\n'

        async with respx.mock() as respx_mock:
            respx_mock.get(RECORDS_URL).mock(
                return_value=httpx.Response(200, json={"total_count": 2, "results": []})
            )
            respx_mock.get(EXPORTS_URL).mock(return_value=httpx.Response(200, text=body))

            async with httpx.AsyncClient() as client:
                result = await _adapter(client).fetch()
                with pytest.raises(FetchError, match="not a JSON object"):
                    _ = [r async for r in result.records]


class TestBaseUrlOverride:
    async def test_custom_base_url_used_for_both_endpoints(self) -> None:
        custom_base = "https://example.test/api/v2.1/"
        custom_records = f"{custom_base}catalog/datasets/{DATASET}/records"
        custom_exports = f"{custom_base}catalog/datasets/{DATASET}/exports/jsonl"

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(custom_records).mock(
                return_value=httpx.Response(200, json={"total_count": 0, "results": []})
            )
            respx_mock.get(custom_exports).mock(return_value=httpx.Response(200, text=""))

            async with httpx.AsyncClient() as client:
                adapter = VancouverBusinessLicencesAdapter(
                    dataset_id=DATASET,
                    jurisdiction=JURISDICTION,
                    client=client,
                    clock=lambda: PINNED_NOW,
                    base_url=custom_base,
                )
                result = await adapter.fetch()
                _ = [r async for r in result.records]

            assert result.snapshot.source_url == custom_exports


class TestUserAgentOnWire:
    async def test_user_agent_reaches_wire(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            count_route = respx_mock.get(RECORDS_URL).mock(
                return_value=httpx.Response(200, json={"total_count": 0, "results": []})
            )
            respx_mock.get(EXPORTS_URL).mock(return_value=httpx.Response(200, text=""))

            async with httpx.AsyncClient(headers={"User-Agent": "civix-test/0.0.0"}) as client:
                result = await _adapter(client).fetch()
                _ = [r async for r in result.records]

            sent = count_route.calls.last.request

            assert sent.headers["User-Agent"] == "civix-test/0.0.0"
