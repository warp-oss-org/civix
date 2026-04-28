"""Tests for `attach_observers`, the pipeline-side drift wiring."""

from __future__ import annotations

from collections.abc import AsyncIterable
from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict

from civix.core.identity import (
    DatasetId,
    Jurisdiction,
    MapperId,
    SnapshotId,
    SourceId,
)
from civix.core.mapping import MappingReport, MapResult
from civix.core.pipeline import (
    PipelineRecord,
    PipelineResult,
    attach_observers,
)
from civix.core.provenance import MapperVersion, ProvenanceRef
from civix.core.quality import FieldQuality, MappedField
from civix.core.snapshots import RawRecord, SourceSnapshot

PINNED_NOW = datetime(2026, 4, 25, 12, 0, tzinfo=UTC)
SNAP = SnapshotId("snap-1")


class _MinRecord(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    provenance: ProvenanceRef
    name: MappedField[str]


def _snapshot() -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SNAP,
        source_id=SourceId("vancouver-open-data"),
        dataset_id=DatasetId("business-licences"),
        jurisdiction=Jurisdiction(country="CA", region="BC", locality="Vancouver"),
        fetched_at=PINNED_NOW,
        record_count=2,
    )


def _provenance() -> ProvenanceRef:
    return ProvenanceRef(
        snapshot_id=SNAP,
        source_id=SourceId("vancouver-open-data"),
        dataset_id=DatasetId("business-licences"),
        jurisdiction=Jurisdiction(country="CA", region="BC", locality="Vancouver"),
        fetched_at=PINNED_NOW,
        mapper=MapperVersion(mapper_id=MapperId("m"), version="0"),
    )


def _paired(source_record_id: str, **raw_data: object) -> PipelineRecord[_MinRecord]:
    raw = RawRecord(snapshot_id=SNAP, raw_data=raw_data, source_record_id=source_record_id)
    rec = _MinRecord(
        provenance=_provenance(),
        name=MappedField[str](value="x", quality=FieldQuality.DIRECT, source_fields=("name",)),
    )
    return PipelineRecord(raw=raw, mapped=MapResult[_MinRecord](record=rec, report=MappingReport()))


def _result(records: list[PipelineRecord[_MinRecord]]) -> PipelineResult[_MinRecord]:
    async def gen() -> AsyncIterable[PipelineRecord[_MinRecord]]:
        for r in records:
            yield r

    return PipelineResult[_MinRecord](snapshot=_snapshot(), records=gen())


class _RecordingObserver:
    """Minimal observer for testing the wiring."""

    def __init__(self) -> None:
        self.seen: list[str | None] = []
        self.finalize_called_with: SourceSnapshot | None = None

    def observe(self, record: RawRecord) -> None:
        self.seen.append(record.source_record_id)

    def finalize(self, snapshot: SourceSnapshot) -> object:
        self.finalize_called_with = snapshot
        return {"seen": list(self.seen)}


class TestAttachObservers:
    async def test_observer_sees_each_raw_record_once(self) -> None:
        result = _result(
            [
                _paired("r1", status="issued"),
                _paired("r2", status="cancelled"),
            ]
        )
        obs = _RecordingObserver()

        wrapped = attach_observers(result, [obs])
        consumed = [pair async for pair in wrapped.records]

        assert obs.seen == ["r1", "r2"]
        assert [p.raw.source_record_id for p in consumed] == ["r1", "r2"]

    async def test_multiple_observers_all_get_fed(self) -> None:
        result = _result([_paired("r1", status="issued")])
        a = _RecordingObserver()
        b = _RecordingObserver()

        wrapped = attach_observers(result, [a, b])
        _ = [pair async for pair in wrapped.records]

        assert a.seen == ["r1"]
        assert b.seen == ["r1"]

    async def test_partial_iteration_yields_partial_observation(self) -> None:
        result = _result(
            [
                _paired("r1", status="issued"),
                _paired("r2", status="cancelled"),
                _paired("r3", status="pending"),
            ]
        )
        obs = _RecordingObserver()

        wrapped = attach_observers(result, [obs])

        async for pair in wrapped.records:
            if pair.raw.source_record_id == "r2":
                break

        assert obs.seen == ["r1", "r2"]

    async def test_snapshot_is_passed_through_unchanged(self) -> None:
        result = _result([])

        wrapped = attach_observers(result, [_RecordingObserver()])

        assert wrapped.snapshot is result.snapshot
