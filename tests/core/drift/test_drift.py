"""Tests for schema drift contracts and analysis."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from civix.core.drift import (
    DriftSeverity,
    JsonFieldKind,
    SchemaDriftKind,
    SchemaFieldSpec,
    SourceSchemaSpec,
    analyze_schema,
)
from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, SnapshotId, SourceId
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot

PINNED_NOW = datetime(2026, 4, 25, 12, 0, tzinfo=UTC)


def _snapshot(*, record_count: int = 2) -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId("snap-1"),
        source_id=SourceId("test-source"),
        dataset_id=DatasetId("test-dataset"),
        jurisdiction=Jurisdiction(country="CA", region="BC", locality="Vancouver"),
        fetched_at=PINNED_NOW,
        record_count=record_count,
    )


def _record(source_record_id: str, **raw_data: object) -> RawRecord:
    return RawRecord(
        snapshot_id=SnapshotId("snap-1"),
        raw_data=raw_data,
        source_record_id=source_record_id,
    )


def _spec() -> SourceSchemaSpec:
    return SourceSchemaSpec(
        spec_id="test-source-schema",
        version="2026-04-25",
        fields=(
            SchemaFieldSpec(name="name", kinds=(JsonFieldKind.STRING,)),
            SchemaFieldSpec(name="status", kinds=(JsonFieldKind.STRING,)),
            SchemaFieldSpec(name="count", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        ),
    )


class TestAnalyzeSchema:
    def test_no_findings_for_matching_schema(self) -> None:
        report = analyze_schema(
            _snapshot(),
            [
                _record("r1", name="A", status="Issued", count=1),
                _record("r2", name="B", status="Cancelled", count=None),
            ],
            _spec(),
        )

        assert report.findings == ()
        assert not report.has_errors

    def test_unexpected_field_is_warning(self) -> None:
        report = analyze_schema(
            _snapshot(),
            [
                _record("r1", name="A", status="Issued", count=1),
                _record("r2", name="B", status="Cancelled", count=2, new_field=True),
            ],
            _spec(),
        )

        assert len(report.findings) == 1
        finding = report.findings[0]

        assert finding.kind is SchemaDriftKind.UNEXPECTED_FIELD
        assert finding.severity is DriftSeverity.WARNING
        assert finding.field_name == "new_field"
        assert finding.count == 1
        assert finding.sample_source_record_ids == ("r2",)
        assert not report.has_errors

    def test_missing_expected_field_is_error(self) -> None:
        report = analyze_schema(
            _snapshot(),
            [
                _record("r1", name="A", status="Issued", count=1),
                _record("r2", name="B", count=2),
            ],
            _spec(),
        )

        assert len(report.findings) == 1
        finding = report.findings[0]

        assert finding.kind is SchemaDriftKind.MISSING_FIELD
        assert finding.severity is DriftSeverity.ERROR
        assert finding.field_name == "status"
        assert finding.count == 1
        assert finding.sample_source_record_ids == ("r2",)
        assert report.has_errors

    def test_wrong_json_kind_is_error(self) -> None:
        report = analyze_schema(
            _snapshot(),
            [
                _record("r1", name="A", status="Issued", count=1),
                _record("r2", name="B", status=404, count=2),
            ],
            _spec(),
        )

        assert len(report.findings) == 1
        finding = report.findings[0]

        assert finding.kind is SchemaDriftKind.TYPE_MISMATCH
        assert finding.severity is DriftSeverity.ERROR
        assert finding.field_name == "status"
        assert finding.expected == "string"
        assert finding.observed == "number"
        assert finding.count == 1
        assert finding.sample_source_record_ids == ("r2",)

    def test_disallowed_null_is_error(self) -> None:
        report = analyze_schema(
            _snapshot(),
            [
                _record("r1", name="A", status="Issued", count=1),
                _record("r2", name=None, status="Cancelled", count=2),
            ],
            _spec(),
        )

        assert len(report.findings) == 1
        finding = report.findings[0]

        assert finding.kind is SchemaDriftKind.NULLABILITY_MISMATCH
        assert finding.severity is DriftSeverity.ERROR
        assert finding.field_name == "name"
        assert finding.count == 1
        assert finding.sample_source_record_ids == ("r2",)

    def test_counts_and_sample_source_record_ids_are_stable(self) -> None:
        records = [
            _record(f"r{i}", name=f"N{i}", status="Issued", count=i, extra="x") for i in range(1, 7)
        ]

        report = analyze_schema(_snapshot(record_count=6), records, _spec())

        assert len(report.findings) == 1
        finding = report.findings[0]

        assert finding.field_name == "extra"
        assert finding.count == 6
        assert finding.sample_source_record_ids == ("r1", "r2", "r3", "r4", "r5")

    def test_report_metadata_mirrors_snapshot(self) -> None:
        snapshot = _snapshot(record_count=1)
        spec = _spec()

        report = analyze_schema(
            snapshot,
            [_record("r1", name="A", status="Issued", count=1)],
            spec,
        )

        assert report.snapshot_id == snapshot.snapshot_id
        assert report.source_id == snapshot.source_id
        assert report.dataset_id == snapshot.dataset_id
        assert report.jurisdiction == snapshot.jurisdiction
        assert report.fetched_at == snapshot.fetched_at
        assert report.spec_id == spec.spec_id
        assert report.spec_version == spec.version
        assert report.checked_record_count == 1

    def test_empty_dataset_is_valid_to_analyze(self) -> None:
        report = analyze_schema(_snapshot(record_count=0), [], _spec())

        assert report.checked_record_count == 0
        assert report.findings == ()
        assert not report.has_errors


class TestContracts:
    def test_models_are_frozen_and_strict(self) -> None:
        field = SchemaFieldSpec(name="name", kinds=(JsonFieldKind.STRING,))

        with pytest.raises(ValidationError):
            field.__setattr__("name", "other")

        with pytest.raises(ValidationError):
            SchemaFieldSpec.model_validate({"name": "x", "kinds": ("string",), "unexpected": True})

    def test_duplicate_spec_fields_are_rejected(self) -> None:
        with pytest.raises(ValidationError, match="unique"):
            SourceSchemaSpec(
                spec_id="dup",
                version="1",
                fields=(
                    SchemaFieldSpec(name="name", kinds=(JsonFieldKind.STRING,)),
                    SchemaFieldSpec(name="name", kinds=(JsonFieldKind.NUMBER,)),
                ),
            )

    def test_empty_kind_set_is_rejected(self) -> None:
        with pytest.raises(ValidationError, match="must not be empty"):
            SchemaFieldSpec(name="name", kinds=())
