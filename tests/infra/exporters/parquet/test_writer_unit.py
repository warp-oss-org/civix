"""Unit tests for the Parquet snapshot writer."""

from __future__ import annotations

import hashlib
import importlib
import json
from collections.abc import AsyncIterable, Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel, ConfigDict

import civix.infra.exporters.parquet.writer as writer
from civix.core.export.models.manifest import ExportManifest
from civix.core.identity.models.identifiers import (
    DatasetId,
    Jurisdiction,
    MapperId,
    SnapshotId,
    SourceId,
)
from civix.core.mapping.models.mapper import FieldConflict, MappingReport, MapResult
from civix.core.pipeline import PipelineRecord, PipelineResult
from civix.core.provenance.models.provenance import MapperVersion, ProvenanceRef
from civix.core.quality.models.fields import FieldQuality, MappedField
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.infra.exporters.parquet import write_snapshot

PINNED_NOW = datetime(2026, 4, 25, 12, 0, tzinfo=UTC)
SNAP = SnapshotId("snap-1")
SOURCE = SourceId("vancouver-open-data")
DATASET = DatasetId("business-licences")
JURISDICTION = Jurisdiction(country="CA", region="BC", locality="Vancouver")
MAPPER = MapperVersion(mapper_id=MapperId("vancouver-business-licences"), version="0.1.0")
PQ: Any = importlib.import_module("pyarrow.parquet")


class _FakeRecord(BaseModel):
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
    normalized = _FakeRecord(
        provenance=_provenance(source_record_id=source_record_id),
        name=MappedField[str](
            value=name_value,
            quality=name_quality,
            source_fields=("n",),
        ),
        score=MappedField[int](
            value=score,
            quality=FieldQuality.STANDARDIZED,
            source_fields=("s",),
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


def _read_parquet(path: Path) -> list[dict[str, object]]:
    return PQ.read_table(path).to_pylist()


class TestDirectoryLayout:
    async def test_writes_four_files_named_for_the_contract(self, tmp_path: Path) -> None:
        result = _result([_record(source_record_id="r1", name="A", score=1)])

        await write_snapshot(result, output_dir=tmp_path, record_type=_FakeRecord)

        snap_dir = tmp_path / SNAP

        assert {p.name for p in snap_dir.iterdir()} == {
            "records.parquet",
            "reports.jsonl",
            "schema.json",
            "manifest.json",
        }

    async def test_no_tmp_files_remain_after_success(self, tmp_path: Path) -> None:
        result = _result([_record(source_record_id="r1", name="A", score=1)])

        await write_snapshot(result, output_dir=tmp_path, record_type=_FakeRecord)

        assert not list((tmp_path / SNAP).glob("*.tmp"))


class TestRecordsFile:
    async def test_records_round_trip_as_nested_rows(self, tmp_path: Path) -> None:
        original = [
            _record(source_record_id="r1", name="A", score=1),
            _record(source_record_id="r2", name="B", score=2),
        ]
        result = _result(original)

        await write_snapshot(result, output_dir=tmp_path, record_type=_FakeRecord)

        rows = _read_parquet(tmp_path / SNAP / "records.parquet")

        assert rows == [pair.mapped.record.model_dump(mode="json") for pair in original]

    async def test_one_row_per_record_in_pipeline_order(self, tmp_path: Path) -> None:
        result = _result(
            [
                _record(source_record_id="r1", name="A", score=1),
                _record(source_record_id="r2", name="B", score=2),
                _record(source_record_id="r3", name="C", score=3),
            ]
        )

        await write_snapshot(result, output_dir=tmp_path, record_type=_FakeRecord)

        rows = _read_parquet(tmp_path / SNAP / "records.parquet")

        assert [row["name"]["value"] for row in rows] == ["A", "B", "C"]  # type: ignore[index]

    async def test_records_are_written_in_row_groups(self, tmp_path: Path) -> None:
        result = _result(
            [
                _record(source_record_id="r1", name="A", score=1),
                _record(source_record_id="r2", name="B", score=2),
                _record(source_record_id="r3", name="C", score=3),
                _record(source_record_id="r4", name="D", score=4),
                _record(source_record_id="r5", name="E", score=5),
            ]
        )

        await write_snapshot(
            result,
            output_dir=tmp_path,
            record_type=_FakeRecord,
            _row_group_size=2,
        )

        parquet_file = PQ.ParquetFile(tmp_path / SNAP / "records.parquet")
        rows = parquet_file.read().to_pylist()

        assert parquet_file.metadata.num_row_groups == 3
        assert [row["name"]["value"] for row in rows] == ["A", "B", "C", "D", "E"]  # type: ignore[index]

    async def test_row_group_size_must_be_positive(self, tmp_path: Path) -> None:
        result = _result([])

        with pytest.raises(ValueError, match="greater than zero"):
            await write_snapshot(
                result,
                output_dir=tmp_path,
                record_type=_FakeRecord,
                _row_group_size=0,
            )


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
                    raw=RawRecord(snapshot_id=SNAP, raw_data={}, source_record_id="r1"),
                    mapped=MapResult[_FakeRecord](record=normalized, report=report),
                )
            ]
        )

        await write_snapshot(result, output_dir=tmp_path, record_type=_FakeRecord)

        line = _read_jsonl(tmp_path / SNAP / "reports.jsonl")[0]
        round_tripped = MappingReport.model_validate_json(json.dumps(line["report"]))

        assert round_tripped == report


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

    async def test_file_index_includes_three_data_files_with_correct_hashes(
        self, tmp_path: Path
    ) -> None:
        result = _result([_record(source_record_id="r1", name="A", score=1)])

        manifest = await write_snapshot(result, output_dir=tmp_path, record_type=_FakeRecord)

        snap_dir = tmp_path / SNAP
        index = {f.filename: f for f in manifest.files}

        assert set(index) == {"records.parquet", "reports.jsonl", "schema.json"}
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
                value=None,
                quality=FieldQuality.CONFLICTED,
                source_fields=("n1", "n2"),
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
    async def test_empty_pipeline_writes_empty_parquet_and_reports(self, tmp_path: Path) -> None:
        result = _result([])

        manifest = await write_snapshot(result, output_dir=tmp_path, record_type=_FakeRecord)

        assert _read_parquet(tmp_path / SNAP / "records.parquet") == []
        assert (tmp_path / SNAP / "reports.jsonl").read_text() == ""
        assert manifest.record_count == 0

    async def test_empty_pipeline_has_no_observed_mapper(self, tmp_path: Path) -> None:
        result = _result([])

        manifest = await write_snapshot(result, output_dir=tmp_path, record_type=_FakeRecord)

        assert manifest.mapper is None


class TestMissingDependency:
    async def test_missing_pyarrow_has_actionable_message(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        real_import_module = importlib.import_module

        def fake_import_module(name: str) -> Any:
            if name == "pyarrow":
                raise ModuleNotFoundError(name)

            return real_import_module(name)

        monkeypatch.setattr(writer.importlib, "import_module", fake_import_module)
        result = _result([])

        with pytest.raises(RuntimeError, match=r"civix\[parquet\]"):
            await write_snapshot(result, output_dir=tmp_path, record_type=_FakeRecord)
