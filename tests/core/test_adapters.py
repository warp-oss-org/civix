"""Tests for the source-adapter contract: FetchResult and SourceAdapter Protocol."""

from __future__ import annotations

from collections.abc import AsyncIterable
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import pytest

from civix.core.adapters import FetchResult, SourceAdapter
from civix.core.identity import (
    DatasetId,
    Jurisdiction,
    SnapshotId,
    SourceId,
)
from civix.core.snapshots import RawRecord, SourceSnapshot


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
        records = [r async for r in result.records]

        assert isinstance(result, FetchResult)
        assert len(records) == 1
        assert records[0].raw_data["name"] == "Joe's Cafe"
