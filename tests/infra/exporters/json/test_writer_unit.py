"""Unit tests for the JSON snapshot writer.

The pipeline is faked so the writer is exercised in isolation: a small
generic record type, a hand-rolled async iterator of `PipelineRecord`,
and `tmp_path` for output. Round-trip tests load the files back from
disk and reparse them through their own pydantic models.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterable, Iterable
from datetime import UTC, datetime
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from civix.core.export import ExportManifest
from civix.core.identity import (
    DatasetId,
    Jurisdiction,
    MapperId,
    SnapshotId,
    SourceId,
)
from civix.core.mapping import FieldConflict, MappingReport, MapResult
from civix.core.pipeline import PipelineRecord, PipelineResult
from civix.core.provenance import MapperVersion, ProvenanceRef
from civix.core.quality import FieldQuality, MappedField
from civix.core.snapshots import RawRecord, SourceSnapshot
from civix.infra.exporters.json import write_snapshot

PINNED_NOW = datetime(2026, 4, 25, 12, 0, tzinfo=UTC)
SNAP = SnapshotId("snap-1")
SOURCE = SourceId("vancouver-open-data")
DATASET = DatasetId("business-licences")
JURISDICTION = Jurisdiction(country="CA", region="BC", locality="Vancouver")
MAPPER = MapperVersion(mapper_id=MapperId("vancouver-business-licences"), version="0.1.0")


class _FakeRecord(BaseModel):
    """A minimal normalized record for writer tests."""

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    provenance: ProvenanceRef
    name: MappedField[str]
    score: MappedField[int]


def _provenance(*, source_record_id: str | None) -> ProvenanceRef:
    return ProvenanceRef(
        snapshot_id=SNAP,
        source_id=SOURCE,
        dataset_id=DATASET,
        jurisdiction=JURISDICTION,
        fetched_at=PINNED_NOW,
        mapper=MAPPER,
        source_record_id=source_record_id,
    )


def _snapshot(*, record_count: int) -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SNAP,
        source_id=SOURCE,
        dataset_id=DATASET,
        jurisdiction=JURISDICTION,
        fetched_at=PINNED_NOW,
        record_count=record_count,
    )


def _record(
    *,
    source_record_id: str,
    name: str,
    score: int,
    name_quality: FieldQuality = FieldQuality.DIRECT,
) -> PipelineRecord[_FakeRecord]:
    raw = RawRecord(snapshot_id=SNAP, raw_data={"n": name}, source_record_id=source_record_id)
    name_value: str | None = None if name_quality is FieldQuality.NOT_PROVIDED else name
    name_sources: tuple[str, ...] = ("n",)
    normalized = _FakeRecord(
        provenance=_provenance(source_record_id=source_record_id),
        name=MappedField[str](value=name_value, quality=name_quality, source_fields=name_sources),
        score=MappedField[int](
            value=score, quality=FieldQuality.STANDARDIZED, source_fields=("s",)
        ),
    )
    mapped = MapResult[_FakeRecord](record=normalized, report=MappingReport())

    return PipelineRecord(raw=raw, mapped=mapped)


def _result(records: Iterable[PipelineRecord[_FakeRecord]]) -> PipelineResult[_FakeRecord]:
    materialized = list(records)

    async def gen() -> AsyncIterable[PipelineRecord[_FakeRecord]]:
        for r in materialized:
            yield r

    return PipelineResult[_FakeRecord](
        snapshot=_snapshot(record_count=len(materialized)),
        records=gen(),
    )


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text().splitlines() if line]


class TestDirectoryLayout:
    async def test_writes_four_files_named_for_the_contract(self, tmp_path: Path) -> None:
        result = _result([_record(source_record_id="r1", name="A", score=1)])

        await write_snapshot(result, output_dir=tmp_path, record_type=_FakeRecord)

        snap_dir = tmp_path / SNAP

        assert {p.name for p in snap_dir.iterdir()} == {
            "records.jsonl",
            "reports.jsonl",
            "schema.json",
            "manifest.json",
        }

    async def test_no_tmp_files_remain_after_success(self, tmp_path: Path) -> None:
        result = _result([_record(source_record_id="r1", name="A", score=1)])

        await write_snapshot(result, output_dir=tmp_path, record_type=_FakeRecord)

        snap_dir = tmp_path / SNAP

        assert not list(snap_dir.glob("*.tmp"))

    async def test_creates_parent_directories(self, tmp_path: Path) -> None:
        nested = tmp_path / "a" / "b"
        result = _result([_record(source_record_id="r1", name="A", score=1)])

        await write_snapshot(result, output_dir=nested, record_type=_FakeRecord)

        assert (nested / SNAP / "manifest.json").exists()


class TestRecordsFile:
    async def test_one_line_per_record_in_pipeline_order(self, tmp_path: Path) -> None:
        result = _result(
            [
                _record(source_record_id="r1", name="A", score=1),
                _record(source_record_id="r2", name="B", score=2),
                _record(source_record_id="r3", name="C", score=3),
            ]
        )

        await write_snapshot(result, output_dir=tmp_path, record_type=_FakeRecord)

        lines = _read_jsonl(tmp_path / SNAP / "records.jsonl")
        names = [line["name"]["value"] for line in lines]  # type: ignore[index]

        assert names == ["A", "B", "C"]

    async def test_records_round_trip_through_record_type(self, tmp_path: Path) -> None:
        original = [
            _record(source_record_id="r1", name="A", score=1),
            _record(source_record_id="r2", name="B", score=2),
        ]
        result = _result(original)

        await write_snapshot(result, output_dir=tmp_path, record_type=_FakeRecord)

        lines = (tmp_path / SNAP / "records.jsonl").read_text().splitlines()
        loaded = [_FakeRecord.model_validate_json(line) for line in lines]

        assert loaded == [pair.mapped.record for pair in original]


class TestReportsFile:
    async def test_one_wrapped_report_per_record_keyed_by_source_id(self, tmp_path: Path) -> None:
        records = [
            _record(source_record_id="r1", name="A", score=1),
            _record(source_record_id="r2", name="B", score=2),
        ]
        result = _result(records)

        await write_snapshot(result, output_dir=tmp_path, record_type=_FakeRecord)

        lines = _read_jsonl(tmp_path / SNAP / "reports.jsonl")

        assert [line["source_record_id"] for line in lines] == ["r1", "r2"]
        assert all(set(line) == {"source_record_id", "report"} for line in lines)

    async def test_unmapped_and_conflict_payload_round_trips(self, tmp_path: Path) -> None:
        report = MappingReport(
            unmapped_source_fields=("extra1", "extra2"),
            conflicts=(
                FieldConflict(
                    field_name="name",
                    candidates=("A", "B"),
                    source_fields=("n1", "n2"),
                ),
            ),
        )
        raw = RawRecord(snapshot_id=SNAP, raw_data={}, source_record_id="r1")
        normalized = _FakeRecord(
            provenance=_provenance(source_record_id="r1"),
            name=MappedField[str](
                value=None,
                quality=FieldQuality.CONFLICTED,
                source_fields=("n1", "n2"),
            ),
            score=MappedField[int](value=1, quality=FieldQuality.DIRECT, source_fields=("s",)),
        )
        result = _result(
            [
                PipelineRecord(
                    raw=raw,
                    mapped=MapResult[_FakeRecord](record=normalized, report=report),
                )
            ]
        )

        await write_snapshot(result, output_dir=tmp_path, record_type=_FakeRecord)

        line = _read_jsonl(tmp_path / SNAP / "reports.jsonl")[0]
        round_tripped = MappingReport.model_validate_json(json.dumps(line["report"]))

        assert round_tripped == report


class TestSchemaFile:
    async def test_schema_matches_record_type_model_json_schema(self, tmp_path: Path) -> None:
        result = _result([_record(source_record_id="r1", name="A", score=1)])

        await write_snapshot(result, output_dir=tmp_path, record_type=_FakeRecord)

        on_disk = json.loads((tmp_path / SNAP / "schema.json").read_text())

        assert on_disk == _FakeRecord.model_json_schema()


class TestManifest:
    async def test_manifest_returned_matches_manifest_on_disk(self, tmp_path: Path) -> None:
        result = _result(
            [
                _record(source_record_id="r1", name="A", score=1),
                _record(source_record_id="r2", name="B", score=2),
            ]
        )

        returned = await write_snapshot(result, output_dir=tmp_path, record_type=_FakeRecord)

        on_disk = ExportManifest.model_validate_json(
            (tmp_path / SNAP / "manifest.json").read_text()
        )

        assert returned == on_disk

    async def test_manifest_carries_snapshot_metadata_and_mapper(self, tmp_path: Path) -> None:
        result = _result([_record(source_record_id="r1", name="A", score=1)])

        manifest = await write_snapshot(result, output_dir=tmp_path, record_type=_FakeRecord)

        assert manifest.snapshot_id == SNAP
        assert manifest.source_id == SOURCE
        assert manifest.dataset_id == DATASET
        assert manifest.jurisdiction == JURISDICTION
        assert manifest.fetched_at == PINNED_NOW
        assert manifest.record_count == 1
        assert manifest.mapper == MAPPER

    async def test_file_index_includes_three_data_files_with_correct_hashes(
        self, tmp_path: Path
    ) -> None:
        result = _result([_record(source_record_id="r1", name="A", score=1)])

        manifest = await write_snapshot(result, output_dir=tmp_path, record_type=_FakeRecord)

        snap_dir = tmp_path / SNAP
        index = {f.filename: f for f in manifest.files}

        assert set(index) == {"records.jsonl", "reports.jsonl", "schema.json"}
        for filename, entry in index.items():
            on_disk = (snap_dir / filename).read_bytes()
            assert entry.sha256 == hashlib.sha256(on_disk).hexdigest()
            assert entry.byte_count == len(on_disk)

    async def test_quality_counts_aggregate_across_records(self, tmp_path: Path) -> None:
        result = _result(
            [
                _record(source_record_id="r1", name="A", score=1),
                _record(
                    source_record_id="r2",
                    name="B",
                    score=2,
                    name_quality=FieldQuality.NOT_PROVIDED,
                ),
            ]
        )

        manifest = await write_snapshot(result, output_dir=tmp_path, record_type=_FakeRecord)

        # Two records × two MappedFields each = four field qualities total.
        assert manifest.mapping_summary.quality_counts == {
            FieldQuality.DIRECT: 1,
            FieldQuality.NOT_PROVIDED: 1,
            FieldQuality.STANDARDIZED: 2,
        }

    async def test_unmapped_and_conflict_totals_aggregate(self, tmp_path: Path) -> None:
        report = MappingReport(
            unmapped_source_fields=("a", "b", "c"),
            conflicts=(
                FieldConflict(
                    field_name="name",
                    candidates=("A", "B"),
                    source_fields=("n1", "n2"),
                ),
            ),
        )
        normalized = _FakeRecord(
            provenance=_provenance(source_record_id="r1"),
            name=MappedField[str](
                value=None, quality=FieldQuality.CONFLICTED, source_fields=("n1", "n2")
            ),
            score=MappedField[int](value=1, quality=FieldQuality.DIRECT, source_fields=("s",)),
        )
        result = _result(
            [
                PipelineRecord(
                    raw=RawRecord(snapshot_id=SNAP, raw_data={}, source_record_id="r1"),
                    mapped=MapResult[_FakeRecord](record=normalized, report=report),
                )
            ]
        )

        manifest = await write_snapshot(result, output_dir=tmp_path, record_type=_FakeRecord)

        assert manifest.mapping_summary.unmapped_source_fields_total == 3
        assert manifest.mapping_summary.conflicts_total == 1


class TestEmptyDataset:
    async def test_empty_pipeline_writes_files_with_no_records(self, tmp_path: Path) -> None:
        result = _result([])

        manifest = await write_snapshot(result, output_dir=tmp_path, record_type=_FakeRecord)

        snap_dir = tmp_path / SNAP

        assert (snap_dir / "records.jsonl").read_text() == ""
        assert (snap_dir / "reports.jsonl").read_text() == ""
        assert manifest.record_count == 0

    async def test_empty_pipeline_has_no_observed_mapper(self, tmp_path: Path) -> None:
        result = _result([])

        manifest = await write_snapshot(result, output_dir=tmp_path, record_type=_FakeRecord)

        assert manifest.mapper is None

    async def test_empty_pipeline_still_writes_schema(self, tmp_path: Path) -> None:
        result = _result([])

        await write_snapshot(result, output_dir=tmp_path, record_type=_FakeRecord)

        on_disk = json.loads((tmp_path / SNAP / "schema.json").read_text())

        assert on_disk == _FakeRecord.model_json_schema()
