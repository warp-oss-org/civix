"""Write a `PipelineResult` to a Parquet snapshot directory.

Layout written under `{output_dir}/{snapshot_id}/`:

- `records.parquet`  normalized records, one nested row per record
- `reports.jsonl`    one wrapped `MappingReport` per line
- `schema.json`      JSON Schema generated from `record_type`
- `manifest.json`    `ExportManifest` written last

Parquet support is optional. Install with `civix[parquet]`.
"""

from __future__ import annotations

import hashlib
import importlib
import json
from collections import Counter
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from civix.core.export import ExportedFile, ExportManifest, MappingSummary
from civix.core.pipeline import PipelineResult
from civix.core.provenance import MapperVersion
from civix.core.quality import FieldQuality, MappedField

_RECORDS_FILE = "records.parquet"
_REPORTS_FILE = "reports.jsonl"
_SCHEMA_FILE = "schema.json"
_MANIFEST_FILE = "manifest.json"
_DEFAULT_ROW_GROUP_SIZE = 10_000


async def write_snapshot[TNorm: BaseModel](
    result: PipelineResult[TNorm],
    *,
    output_dir: Path,
    record_type: type[TNorm],
    _row_group_size: int = _DEFAULT_ROW_GROUP_SIZE,
) -> ExportManifest:
    """Write `result` to a Parquet snapshot directory and return the manifest.

    Parquet V1 infers the Arrow schema from the first non-empty row group.
    Deriving Arrow schemas directly from Pydantic model contracts is future
    work; batching keeps memory bounded without taking on that compiler yet.
    """
    pa, pq = _load_pyarrow()

    if _row_group_size <= 0:
        raise ValueError("_row_group_size must be greater than zero")

    snapshot = result.snapshot
    snapshot_dir = output_dir / snapshot.snapshot_id
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    schema_file = _write_schema(snapshot_dir / _SCHEMA_FILE, record_type)

    quality_counts: Counter[FieldQuality] = Counter()
    unmapped_total = 0
    conflicts_total = 0
    mapper: MapperVersion | None = None
    record_count = 0
    batch: list[dict[str, Any]] = []

    reports_path = snapshot_dir / _REPORTS_FILE
    reports_tmp = _tmp_path(reports_path)
    reports_hasher = hashlib.sha256()
    reports_bytes = 0

    records_path = snapshot_dir / _RECORDS_FILE
    records_tmp = _tmp_path(records_path)
    parquet_writer: Any | None = None

    def write_batch() -> None:
        nonlocal parquet_writer

        if not batch:
            return

        table = pa.Table.from_pylist(batch)

        if parquet_writer is None:
            writer = pq.ParquetWriter(records_tmp, table.schema)
            parquet_writer = writer
        else:
            writer = parquet_writer
            table = pa.Table.from_pylist(batch, schema=writer.schema)

        writer.write_table(table)
        batch.clear()

    with reports_tmp.open("wb") as reports_fh:
        async for paired in result.records:
            normalized = paired.mapped.record
            report = paired.mapped.report

            if mapper is None:
                mapper = _extract_mapper(normalized)

            for quality in _walk_qualities(normalized):
                quality_counts[quality] += 1
            unmapped_total += len(report.unmapped_source_fields)
            conflicts_total += len(report.conflicts)

            batch.append(normalized.model_dump(mode="json"))
            record_count += 1

            report_line = (
                json.dumps(
                    {
                        "source_record_id": paired.raw.source_record_id,
                        "report": report.model_dump(mode="json"),
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8")
                + b"\n"
            )
            reports_fh.write(report_line)
            reports_hasher.update(report_line)
            reports_bytes += len(report_line)

            if len(batch) >= _row_group_size:
                write_batch()

    write_batch()

    if parquet_writer is None:
        pq.write_table(pa.table({}), records_tmp)
    else:
        parquet_writer.close()

    reports_tmp.rename(reports_path)
    records_tmp.rename(records_path)

    manifest = ExportManifest(
        snapshot_id=snapshot.snapshot_id,
        source_id=snapshot.source_id,
        dataset_id=snapshot.dataset_id,
        jurisdiction=snapshot.jurisdiction,
        fetched_at=snapshot.fetched_at,
        record_count=record_count,
        mapper=mapper,
        files=(
            schema_file,
            _file_entry(records_path, filename=_RECORDS_FILE),
            ExportedFile(
                filename=_REPORTS_FILE,
                sha256=reports_hasher.hexdigest(),
                byte_count=reports_bytes,
            ),
        ),
        mapping_summary=MappingSummary(
            quality_counts=dict(quality_counts),
            unmapped_source_fields_total=unmapped_total,
            conflicts_total=conflicts_total,
        ),
    )

    manifest_path = snapshot_dir / _MANIFEST_FILE
    manifest_tmp = _tmp_path(manifest_path)
    manifest_tmp.write_bytes(manifest.model_dump_json(indent=2).encode("utf-8"))
    manifest_tmp.rename(manifest_path)

    return manifest


def _load_pyarrow() -> tuple[Any, Any]:
    try:
        pyarrow = importlib.import_module("pyarrow")
        parquet = importlib.import_module("pyarrow.parquet")
    except ModuleNotFoundError as e:
        raise RuntimeError(
            "Parquet export requires the optional dependency pyarrow; "
            "install it with `civix[parquet]`."
        ) from e

    return pyarrow, parquet


def _write_schema[TNorm: BaseModel](path: Path, record_type: type[TNorm]) -> ExportedFile:
    body = json.dumps(record_type.model_json_schema(), indent=2, sort_keys=True).encode("utf-8")
    tmp = _tmp_path(path)
    tmp.write_bytes(body)
    tmp.rename(path)

    return ExportedFile(
        filename=path.name,
        sha256=hashlib.sha256(body).hexdigest(),
        byte_count=len(body),
    )


def _walk_qualities(record: BaseModel) -> Iterator[FieldQuality]:
    for field_name in record.__class__.model_fields:
        attr = getattr(record, field_name)

        if isinstance(attr, MappedField):
            yield attr.quality


def _extract_mapper(record: BaseModel) -> MapperVersion | None:
    provenance = getattr(record, "provenance", None)

    if provenance is None:
        return None

    mapper = getattr(provenance, "mapper", None)

    return mapper if isinstance(mapper, MapperVersion) else None


def _file_entry(path: Path, *, filename: str) -> ExportedFile:
    body = path.read_bytes()

    return ExportedFile(
        filename=filename,
        sha256=hashlib.sha256(body).hexdigest(),
        byte_count=len(body),
    )


def _tmp_path(path: Path) -> Path:
    return path.with_name(path.name + ".tmp")
