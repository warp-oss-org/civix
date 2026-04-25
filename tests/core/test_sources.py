from collections.abc import AsyncIterable
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import httpx
import pytest
import respx

from civix import __version__
from civix.core.identity import (
    DatasetId,
    Jurisdiction,
    SnapshotId,
    SourceId,
)
from civix.core.observations import RawRecord, SourceSnapshot
from civix.core.sources import (
    DEFAULT_RETRIES,
    DEFAULT_TIMEOUT_SECONDS,
    FetchResult,
    SourceAdapter,
    default_http_client,
    default_user_agent,
)


def _snapshot() -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId("snap-1"),
        source_id=SourceId("vancouver-open-data"),
        dataset_id=DatasetId("business-licences"),
        jurisdiction=Jurisdiction(country="CA", region="BC", locality="Vancouver"),
        fetched_at=datetime(2026, 4, 25, 12, 0, tzinfo=UTC),
        record_count=1,
    )


class _FakeAdapter:
    def __init__(
        self,
        *,
        source_id: SourceId,
        dataset_id: DatasetId,
        jurisdiction: Jurisdiction,
    ) -> None:
        self._source_id = source_id
        self._dataset_id = dataset_id
        self._jurisdiction = jurisdiction

    @property
    def source_id(self) -> SourceId:
        return self._source_id

    @property
    def dataset_id(self) -> DatasetId:
        return self._dataset_id

    @property
    def jurisdiction(self) -> Jurisdiction:
        return self._jurisdiction

    async def fetch(self) -> FetchResult:
        async def gen() -> AsyncIterable[RawRecord]:
            yield RawRecord(
                snapshot_id=SnapshotId("snap-1"),
                raw_data={"name": "Joe's Cafe"},
            )

        return FetchResult(snapshot=_snapshot(), records=gen())


class TestFetchResult:
    def test_holds_snapshot_and_records(self) -> None:
        async def gen() -> AsyncIterable[RawRecord]:
            yield RawRecord(snapshot_id=SnapshotId("snap-1"), raw_data={})

        result = FetchResult(snapshot=_snapshot(), records=gen())
        assert result.snapshot.record_count == 1

    def test_frozen(self) -> None:
        async def gen() -> AsyncIterable[RawRecord]:
            yield RawRecord(snapshot_id=SnapshotId("snap-1"), raw_data={})

        result = FetchResult(snapshot=_snapshot(), records=gen())
        with pytest.raises(FrozenInstanceError):
            result.snapshot = _snapshot()  # type: ignore[misc]


class TestSourceAdapterProtocol:
    def test_fake_adapter_satisfies_protocol_at_runtime(self) -> None:
        adapter = _FakeAdapter(
            source_id=SourceId("s"),
            dataset_id=DatasetId("d"),
            jurisdiction=Jurisdiction(country="CA"),
        )
        assert isinstance(adapter, SourceAdapter)

    def test_object_missing_methods_does_not_satisfy_protocol(self) -> None:
        class _NotAnAdapter:
            pass

        assert not isinstance(_NotAnAdapter(), SourceAdapter)

    async def test_fetch_returns_fetch_result(self) -> None:
        adapter = _FakeAdapter(
            source_id=SourceId("vancouver-open-data"),
            dataset_id=DatasetId("business-licences"),
            jurisdiction=Jurisdiction(country="CA", region="BC", locality="Vancouver"),
        )
        result = await adapter.fetch()
        assert isinstance(result, FetchResult)
        records = [r async for r in result.records]
        assert len(records) == 1
        assert records[0].raw_data["name"] == "Joe's Cafe"


class TestDefaultHttpClient:
    def test_returns_async_client(self) -> None:
        client = default_http_client()
        try:
            assert isinstance(client, httpx.AsyncClient)
        finally:
            # client is not entered; just confirm shape, no I/O happened
            pass

    def test_timeout_default(self) -> None:
        client = default_http_client()
        assert client.timeout == httpx.Timeout(DEFAULT_TIMEOUT_SECONDS)

    def test_timeout_override(self) -> None:
        client = default_http_client(timeout=5.0)
        assert client.timeout == httpx.Timeout(5.0)

    def test_user_agent_default(self) -> None:
        client = default_http_client()
        assert client.headers["User-Agent"] == f"civix/{__version__}"

    def test_user_agent_override(self) -> None:
        client = default_http_client(user_agent="civix-test/0.0.0")
        assert client.headers["User-Agent"] == "civix-test/0.0.0"

    def test_default_user_agent_helper(self) -> None:
        assert default_user_agent() == f"civix/{__version__}"

    def test_default_retries_constant(self) -> None:
        assert DEFAULT_RETRIES == 3

    async def test_client_makes_requests_through_respx_stub(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            route = respx_mock.get("https://opendata.vancouver.ca/ping").mock(
                return_value=httpx.Response(200, json={"ok": True})
            )
            async with default_http_client() as client:
                response = await client.get("https://opendata.vancouver.ca/ping")
            assert route.called
            assert response.status_code == 200
            assert response.json() == {"ok": True}

    async def test_user_agent_sent_on_wire(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            route = respx_mock.get("https://opendata.vancouver.ca/ping").mock(
                return_value=httpx.Response(200, json={})
            )
            async with default_http_client() as client:
                await client.get("https://opendata.vancouver.ca/ping")
            sent_request = route.calls.last.request
            assert sent_request.headers["User-Agent"] == f"civix/{__version__}"
