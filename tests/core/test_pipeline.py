"""Unit tests for the pipeline glue."""

from __future__ import annotations

from collections.abc import AsyncIterable
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import pytest
from pydantic import BaseModel, ConfigDict

from civix.core.adapters import FetchResult, SourceAdapter
from civix.core.identity import (
    DatasetId,
    Jurisdiction,
    MapperId,
    SnapshotId,
    SourceId,
)
from civix.core.mapping import MappingReport, MapResult
from civix.core.pipeline import PipelineRecord, PipelineResult, run
from civix.core.provenance import MapperVersion, ProvenanceRef
from civix.core.quality import FieldQuality, MappedField
from civix.core.snapshots import RawRecord, SourceSnapshot

PINNED_NOW = datetime(2026, 4, 25, 12, 0, tzinfo=UTC)


class _FakeNormalized(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    provenance: ProvenanceRef
    name: MappedField[str]


def _snapshot(*, record_count: int = 1) -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId("snap-1"),
        source_id=SourceId("vancouver-open-data"),
        dataset_id=DatasetId("business-licences"),
        jurisdiction=Jurisdiction(country="CA", region="BC", locality="Vancouver"),
        fetched_at=PINNED_NOW,
        record_count=record_count,
    )


class _FakeAdapter:
    def __init__(self, snapshot: SourceSnapshot, raw_records: list[RawRecord]) -> None:
        self._snapshot = snapshot
        self._raw_records = raw_records
        self.fetch_calls = 0

    @property
    def source_id(self) -> SourceId:
        return self._snapshot.source_id

    @property
    def dataset_id(self) -> DatasetId:
        return self._snapshot.dataset_id

    @property
    def jurisdiction(self) -> Jurisdiction:
        return self._snapshot.jurisdiction

    async def fetch(self) -> FetchResult:
        self.fetch_calls += 1

        async def gen() -> AsyncIterable[RawRecord]:
            for r in self._raw_records:
                yield r

        return FetchResult(snapshot=self._snapshot, records=gen())


class _FakeMapper:
    def __init__(self) -> None:
        self.calls: list[tuple[RawRecord, SourceSnapshot]] = []

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=MapperId("fake-mapper"), version="0.0.0")

    def __call__(self, record: RawRecord, snapshot: SourceSnapshot) -> MapResult[_FakeNormalized]:
        self.calls.append((record, snapshot))
        provenance = ProvenanceRef(
            snapshot_id=snapshot.snapshot_id,
            source_id=snapshot.source_id,
            dataset_id=snapshot.dataset_id,
            jurisdiction=snapshot.jurisdiction,
            fetched_at=snapshot.fetched_at,
            mapper=self.version,
            source_record_id=record.source_record_id,
        )
        normalized = _FakeNormalized(
            provenance=provenance,
            name=MappedField[str](
                value=str(record.raw_data["name"]),
                quality=FieldQuality.DIRECT,
                source_fields=("name",),
            ),
        )

        return MapResult[_FakeNormalized](record=normalized, report=MappingReport())


def _raw(name: str, source_record_id: str) -> RawRecord:
    return RawRecord(
        snapshot_id=SnapshotId("snap-1"),
        raw_data={"name": name},
        source_record_id=source_record_id,
    )


class TestRun:
    async def test_returns_pipeline_result(self) -> None:
        adapter = _FakeAdapter(_snapshot(), [_raw("A", "1")])
        mapper = _FakeMapper()

        result = await run(adapter, mapper)

        assert isinstance(result, PipelineResult)

    async def test_snapshot_threaded_through(self) -> None:
        snap = _snapshot(record_count=2)
        adapter = _FakeAdapter(snap, [_raw("A", "1"), _raw("B", "2")])
        mapper = _FakeMapper()

        result = await run(adapter, mapper)

        assert result.snapshot is snap

    async def test_each_raw_record_yields_one_paired_record(self) -> None:
        adapter = _FakeAdapter(
            _snapshot(record_count=3),
            [_raw("A", "1"), _raw("B", "2"), _raw("C", "3")],
        )
        mapper = _FakeMapper()

        result = await run(adapter, mapper)
        paired = [pr async for pr in result.records]

        assert [pr.mapped.record.name.value for pr in paired] == ["A", "B", "C"]

    async def test_pipeline_record_carries_raw_alongside_mapped(self) -> None:
        adapter = _FakeAdapter(_snapshot(), [_raw("A", "src-1")])
        mapper = _FakeMapper()

        result = await run(adapter, mapper)
        paired = [pr async for pr in result.records]

        assert isinstance(paired[0], PipelineRecord)
        assert paired[0].raw.raw_data == {"name": "A"}
        assert paired[0].mapped.record.name.value == "A"

    async def test_mapper_receives_snapshot(self) -> None:
        snap = _snapshot()
        adapter = _FakeAdapter(snap, [_raw("X", "1")])
        mapper = _FakeMapper()

        result = await run(adapter, mapper)
        _ = [pr async for pr in result.records]

        assert mapper.calls[0][1] is snap

    async def test_records_are_lazy(self) -> None:
        adapter = _FakeAdapter(
            _snapshot(record_count=3),
            [_raw("A", "1"), _raw("B", "2"), _raw("C", "3")],
        )
        mapper = _FakeMapper()

        await run(adapter, mapper)

        assert mapper.calls == []

    async def test_fetch_is_eager(self) -> None:
        adapter = _FakeAdapter(_snapshot(), [_raw("A", "1")])
        mapper = _FakeMapper()

        await run(adapter, mapper)

        assert adapter.fetch_calls == 1

    async def test_mapping_report_is_yielded_per_record(self) -> None:
        adapter = _FakeAdapter(_snapshot(record_count=2), [_raw("A", "1"), _raw("B", "2")])
        mapper = _FakeMapper()

        result = await run(adapter, mapper)
        paired = [pr async for pr in result.records]

        assert all(isinstance(pr.mapped.report, MappingReport) for pr in paired)

    async def test_empty_dataset(self) -> None:
        adapter = _FakeAdapter(_snapshot(record_count=0), [])
        mapper = _FakeMapper()

        result = await run(adapter, mapper)
        paired = [pr async for pr in result.records]

        assert paired == []
        assert result.snapshot.record_count == 0

    async def test_provenance_threads_source_record_id(self) -> None:
        adapter = _FakeAdapter(_snapshot(), [_raw("A", "src-42")])
        mapper = _FakeMapper()

        result = await run(adapter, mapper)
        paired = [pr async for pr in result.records]

        assert paired[0].mapped.record.provenance.source_record_id == "src-42"

    async def test_adapter_satisfies_protocol(self) -> None:
        adapter = _FakeAdapter(_snapshot(), [])

        assert isinstance(adapter, SourceAdapter)


class TestPipelineResult:
    async def test_frozen(self) -> None:
        adapter = _FakeAdapter(_snapshot(), [])
        mapper = _FakeMapper()

        result = await run(adapter, mapper)

        with pytest.raises(FrozenInstanceError):
            result.snapshot = _snapshot()  # type: ignore[misc]
