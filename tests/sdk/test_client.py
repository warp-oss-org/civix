from __future__ import annotations

from collections.abc import AsyncIterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final

import httpx

from civix.core.identity.models.identifiers import (
    DatasetId,
    Jurisdiction,
    MapperId,
    SnapshotId,
    SourceId,
)
from civix.core.mapping.models.mapper import Mapper, MappingReport, MapResult
from civix.core.ports.models.adapter import FetchResult
from civix.core.provenance.models.provenance import MapperVersion
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.sdk import Civix, CivixRuntime, DatasetProduct

PINNED_NOW: Final[datetime] = datetime(2026, 1, 1, tzinfo=UTC)


def _snapshot() -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId("test-source:test-dataset:2026-01-01T00:00:00+00:00"),
        source_id=SourceId("test-source"),
        dataset_id=DatasetId("test-dataset"),
        jurisdiction=Jurisdiction(country="US"),
        fetched_at=PINNED_NOW,
        record_count=1,
    )


def _raw(snapshot: SourceSnapshot) -> RawRecord:
    return RawRecord(
        snapshot_id=snapshot.snapshot_id,
        source_record_id="row-1",
        raw_data={"value": "raw"},
    )


@dataclass(frozen=True, slots=True)
class _FakeAdapter:
    calls: list[str]

    @property
    def source_id(self) -> SourceId:
        return SourceId("test-source")

    @property
    def dataset_id(self) -> DatasetId:
        return DatasetId("test-dataset")

    @property
    def jurisdiction(self) -> Jurisdiction:
        return Jurisdiction(country="US")

    async def fetch(self) -> FetchResult:
        self.calls.append("fetch")
        snapshot = _snapshot()

        async def records() -> AsyncIterable[RawRecord]:
            yield _raw(snapshot)

        return FetchResult(snapshot=snapshot, records=records())


@dataclass(frozen=True, slots=True)
class _FakeMapper:
    calls: list[str]

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=MapperId("test-mapper"), version="1")

    def __call__(self, record: RawRecord, snapshot: SourceSnapshot) -> MapResult[str]:
        self.calls.append(f"map:{record.source_record_id}:{snapshot.dataset_id}")

        return MapResult[str](record="mapped", report=MappingReport())


def _product(
    *,
    calls: list[str],
    captured_clients: list[httpx.AsyncClient] | None = None,
) -> DatasetProduct[str]:
    def adapter_factory(runtime: CivixRuntime) -> _FakeAdapter:
        calls.append("adapter")

        if captured_clients is not None:
            captured_clients.append(runtime.http_client)

        return _FakeAdapter(calls)

    def mapper_factory() -> Mapper[str]:
        calls.append("mapper")

        return _FakeMapper(calls)

    return DatasetProduct[str](
        country="us",
        domain="test_domain",
        model="test_model",
        slug="test_product",
        adapter_factory=adapter_factory,
        mapper_factory=mapper_factory,
    )


async def test_owned_http_client_is_closed_by_aclose() -> None:
    calls: list[str] = []
    captured_clients: list[httpx.AsyncClient] = []
    client = Civix(clock=lambda: PINNED_NOW)

    await client.fetch(_product(calls=calls, captured_clients=captured_clients))
    await client.aclose()

    assert captured_clients[0].is_closed is True


async def test_caller_owned_http_client_is_not_closed_by_aclose() -> None:
    async with httpx.AsyncClient() as http_client:
        client = Civix(http_client=http_client, clock=lambda: PINNED_NOW)

        await client.aclose()

        assert http_client.is_closed is False


async def test_fetch_builds_configured_pair_and_returns_pipeline_result() -> None:
    calls: list[str] = []

    async with Civix(clock=lambda: PINNED_NOW) as client:
        result = await client.fetch(_product(calls=calls))
        records = [record async for record in result.records]

    assert result.snapshot.dataset_id == DatasetId("test-dataset")
    assert records[0].raw.source_record_id == "row-1"
    assert records[0].mapped.record == "mapped"
    assert calls == ["adapter", "mapper", "fetch", "map:row-1:test-dataset"]


async def test_fetch_rejects_closed_client() -> None:
    calls: list[str] = []
    client = Civix(clock=lambda: PINNED_NOW)

    await client.aclose()

    try:
        await client.fetch(_product(calls=calls))
    except RuntimeError as exc:
        assert str(exc) == "Civix client is closed"
    else:
        raise AssertionError("expected closed Civix client to reject fetch")
