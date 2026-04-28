"""Tests for the incremental drift observers."""

from __future__ import annotations

from datetime import UTC, datetime

from civix.core.drift import (
    DriftSeverity,
    JsonFieldKind,
    SchemaDriftKind,
    SchemaFieldSpec,
    SchemaObserver,
    SourceSchemaSpec,
    TaxonomyDriftKind,
    TaxonomyObserver,
    TaxonomySpec,
)
from civix.core.identity import DatasetId, Jurisdiction, SnapshotId, SourceId
from civix.core.snapshots import RawRecord, SourceSnapshot

PINNED_NOW = datetime(2026, 4, 25, 12, 0, tzinfo=UTC)


def _snapshot() -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId("snap-1"),
        source_id=SourceId("test-source"),
        dataset_id=DatasetId("test-dataset"),
        jurisdiction=Jurisdiction(country="CA", region="BC", locality="Vancouver"),
        fetched_at=PINNED_NOW,
        record_count=2,
    )


def _record(source_record_id: str, **raw_data: object) -> RawRecord:
    return RawRecord(
        snapshot_id=SnapshotId("snap-1"),
        raw_data=raw_data,
        source_record_id=source_record_id,
    )


class TestSchemaObserver:
    def test_finalize_matches_analyze_for_full_iteration(self) -> None:
        spec = SourceSchemaSpec(
            spec_id="s",
            version="1",
            fields=(
                SchemaFieldSpec(name="name", kinds=(JsonFieldKind.STRING,)),
                SchemaFieldSpec(name="count", kinds=(JsonFieldKind.NUMBER,), nullable=True),
            ),
        )
        observer = SchemaObserver(spec=spec)

        observer.observe(_record("r1", name="A", count=1))
        observer.observe(_record("r2", name="B", count=None))
        report = observer.finalize(_snapshot())

        assert report.findings == ()
        assert report.checked_record_count == 2

    def test_finalize_after_partial_iteration_is_partial(self) -> None:
        spec = SourceSchemaSpec(
            spec_id="s",
            version="1",
            fields=(SchemaFieldSpec(name="name", kinds=(JsonFieldKind.STRING,)),),
        )
        observer = SchemaObserver(spec=spec)

        observer.observe(_record("r1", name="A"))
        report = observer.finalize(_snapshot())

        assert report.checked_record_count == 1

    def test_unexpected_field_observed_incrementally(self) -> None:
        spec = SourceSchemaSpec(
            spec_id="s",
            version="1",
            fields=(SchemaFieldSpec(name="name", kinds=(JsonFieldKind.STRING,)),),
        )
        observer = SchemaObserver(spec=spec)

        observer.observe(_record("r1", name="A"))
        observer.observe(_record("r2", name="B", extra="surprise"))
        report = observer.finalize(_snapshot())

        kinds = {f.kind for f in report.findings}

        assert SchemaDriftKind.UNEXPECTED_FIELD in kinds


class TestTaxonomyObserver:
    def test_finalize_returns_empty_when_all_known(self) -> None:
        spec = TaxonomySpec(
            taxonomy_id="status",
            version="1",
            source_field="status",
            normalization="strip_casefold",
            known_values=frozenset({"issued", "cancelled"}),
        )
        observer = TaxonomyObserver(specs=(spec,))

        observer.observe(_record("r1", status="Issued"))
        observer.observe(_record("r2", status="cancelled"))
        report = observer.finalize(_snapshot())

        assert report.findings == ()
        assert report.checked_record_count == 2

    def test_finalize_flags_unrecognized(self) -> None:
        spec = TaxonomySpec(
            taxonomy_id="status",
            version="1",
            source_field="status",
            normalization="strip_casefold",
            known_values=frozenset({"issued"}),
        )
        observer = TaxonomyObserver(specs=(spec,))

        observer.observe(_record("r1", status="Issued"))
        observer.observe(_record("r2", status="Surrendered"))
        observer.observe(_record("r3", status="surrendered"))
        report = observer.finalize(_snapshot())

        unrecognized = [
            f for f in report.findings if f.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE
        ]

        assert len(unrecognized) == 1
        finding = unrecognized[0]
        assert finding.observed_value == "surrendered"
        assert finding.count == 2
        assert finding.severity is DriftSeverity.ERROR
        assert set(finding.raw_samples) == {"Surrendered", "surrendered"}
