"""Stream a `PipelineResult` to a JSON snapshot directory.

Layout written under `{output_dir}/{snapshot_id}/`:

- `records.jsonl`  one normalized record per line
- `reports.jsonl`  one wrapped `MappingReport` per line, keyed by
                   `source_record_id`
- `schema.json`    JSON Schema generated from `record_type`
- `manifest.json`  `ExportManifest` written last; its presence implies
                   the other three are complete

Each file is staged as `*.tmp` and renamed in place, so a partial export
never advertises itself as complete via `manifest.json`.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Iterator
from pathlib import Path

from pydantic import BaseModel

from civix.core.export import ExportedFile, ExportManifest, MappingSummary
from civix.core.pipeline import PipelineResult
from civix.core.provenance import MapperVersion
from civix.core.quality import FieldQuality, MappedField

_RECORDS_FILE = "records.jsonl"
_REPORTS_FILE = "reports.jsonl"
_SCHEMA_FILE = "schema.json"
_MANIFEST_FILE = "manifest.json"


async def write_snapshot[TNorm: BaseModel](
    result: PipelineResult[TNorm],
    *,
    output_dir: Path,
    record_type: type[TNorm],
) -> ExportManifest:
    """Write `result` to a JSON snapshot directory and return the manifest.

    The pipeline is consumed lazily: records and reports stream to disk
    one line at a time. The mapper version recorded on the manifest is
    observed from the first record's `provenance.mapper`; for an empty
    snapshot it is `None`.
    """
    snapshot = result.snapshot
    snapshot_dir = output_dir / snapshot.snapshot_id
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    schema_file = _write_schema(snapshot_dir / _SCHEMA_FILE, record_type)

    quality_counts: Counter[FieldQuality] = Counter()
    unmapped_total = 0
    conflicts_total = 0
    record_count = 0
    mapper: MapperVersion | None = None

    records_path = snapshot_dir / _RECORDS_FILE
    reports_path = snapshot_dir / _REPORTS_FILE
    records_tmp = _tmp_path(records_path)
    reports_tmp = _tmp_path(reports_path)

    records_hasher = hashlib.sha256()
    reports_hasher = hashlib.sha256()
    records_bytes = 0
    reports_bytes = 0

    with records_tmp.open("wb") as records_fh, reports_tmp.open("wb") as reports_fh:
        async for paired in result.records:
            normalized = paired.mapped.record
            report = paired.mapped.report

            if mapper is None:
                mapper = _extract_mapper(normalized)

            for quality in _walk_qualities(normalized):
                quality_counts[quality] += 1
            unmapped_total += len(report.unmapped_source_fields)
            conflicts_total += len(report.conflicts)

            record_line = normalized.model_dump_json().encode("utf-8") + b"\n"
            records_fh.write(record_line)
            records_hasher.update(record_line)
            records_bytes += len(record_line)

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

            record_count += 1

    records_tmp.rename(records_path)
    reports_tmp.rename(reports_path)

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
            ExportedFile(
                filename=_RECORDS_FILE,
                sha256=records_hasher.hexdigest(),
                byte_count=records_bytes,
            ),
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


def _tmp_path(path: Path) -> Path:
    return path.with_name(path.name + ".tmp")
