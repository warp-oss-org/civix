"""Tests for `validate_snapshot`'s default rule table."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta

from civix.core.drift import (
    DriftSeverity,
    SchemaDriftFinding,
    SchemaDriftKind,
    SchemaDriftReport,
    TaxonomyDriftFinding,
    TaxonomyDriftKind,
    TaxonomyDriftReport,
)
from civix.core.export import ExportedFile, ExportManifest, MappingSummary
from civix.core.identity import DatasetId, Jurisdiction, SnapshotId, SourceId
from civix.core.quality import FieldQuality
from civix.core.validation import ValidationOutcome, validate_snapshot

PINNED_NOW = datetime(2026, 4, 28, 12, 0, tzinfo=UTC)
SNAP = SnapshotId("snap-1")
SOURCE = SourceId("vancouver-open-data")
DATASET = DatasetId("business-licences")
JURISDICTION = Jurisdiction(country="CA", region="BC", locality="Vancouver")


def _file() -> ExportedFile:
    return ExportedFile(
        filename="records.jsonl",
        sha256="0" * 64,
        byte_count=0,
    )


def _manifest(
    *,
    record_count: int = 10,
    quality_counts: Mapping[FieldQuality, int] | None = None,
    unmapped_source_fields_total: int = 0,
    conflicts_total: int = 0,
) -> ExportManifest:
    return ExportManifest(
        snapshot_id=SNAP,
        source_id=SOURCE,
        dataset_id=DATASET,
        jurisdiction=JURISDICTION,
        fetched_at=PINNED_NOW,
        record_count=record_count,
        files=(_file(),),
        mapping_summary=MappingSummary(
            quality_counts=dict(quality_counts or {}),
            unmapped_source_fields_total=unmapped_source_fields_total,
            conflicts_total=conflicts_total,
        ),
    )


def _schema_report(
    *,
    findings: tuple[SchemaDriftFinding, ...] = (),
    checked_record_count: int = 10,
    snapshot_id: SnapshotId = SNAP,
) -> SchemaDriftReport:
    return SchemaDriftReport(
        snapshot_id=snapshot_id,
        source_id=SOURCE,
        dataset_id=DATASET,
        jurisdiction=JURISDICTION,
        fetched_at=PINNED_NOW,
        spec_id="spec",
        spec_version="1",
        checked_record_count=checked_record_count,
        findings=findings,
    )


def _taxonomy_report(
    *,
    findings: tuple[TaxonomyDriftFinding, ...] = (),
    checked_record_count: int = 10,
    snapshot_id: SnapshotId = SNAP,
) -> TaxonomyDriftReport:
    return TaxonomyDriftReport(
        snapshot_id=snapshot_id,
        source_id=SOURCE,
        dataset_id=DATASET,
        jurisdiction=JURISDICTION,
        fetched_at=PINNED_NOW,
        spec_versions={"status": "1"},
        checked_record_count=checked_record_count,
        findings=findings,
    )


def _schema_finding(severity: DriftSeverity = DriftSeverity.ERROR) -> SchemaDriftFinding:
    return SchemaDriftFinding(
        kind=SchemaDriftKind.MISSING_FIELD,
        severity=severity,
        field_name="businessname",
        expected="present",
        observed="missing",
        count=3,
    )


def _taxonomy_finding(severity: DriftSeverity = DriftSeverity.ERROR) -> TaxonomyDriftFinding:
    return TaxonomyDriftFinding(
        kind=(
            TaxonomyDriftKind.UNRECOGNIZED_VALUE
            if severity is DriftSeverity.ERROR
            else TaxonomyDriftKind.RETIRED_VALUE_OBSERVED
        ),
        severity=severity,
        taxonomy_id="vancouver-business-licence-status",
        source_field="status",
        observed_value="ghost",
        count=2,
        raw_samples=("Ghost",),
    )


class TestPassPath:
    def test_minimal_inputs_with_clean_manifest_pass(self) -> None:
        report = validate_snapshot(_manifest())

        assert report.outcome is ValidationOutcome.PASS
        assert report.findings == ()

    def test_clean_drift_reports_pass(self) -> None:
        report = validate_snapshot(
            _manifest(),
            schema_report=_schema_report(),
            taxonomy_report=_taxonomy_report(),
        )

        assert report.outcome is ValidationOutcome.PASS
        assert report.findings == ()

    def test_report_inherits_identity_from_manifest(self) -> None:
        manifest = _manifest()

        report = validate_snapshot(manifest)

        assert report.snapshot_id == manifest.snapshot_id
        assert report.source_id == manifest.source_id
        assert report.dataset_id == manifest.dataset_id
        assert report.jurisdiction == manifest.jurisdiction
        assert report.fetched_at == manifest.fetched_at


class TestSchemaDrift:
    def test_error_finding_fails_validation(self) -> None:
        report = validate_snapshot(
            _manifest(),
            schema_report=_schema_report(findings=(_schema_finding(DriftSeverity.ERROR),)),
        )

        assert report.outcome is ValidationOutcome.FAIL
        sources = {f.source for f in report.findings}

        assert "schema_drift" in sources

    def test_warning_finding_does_not_fail_validation(self) -> None:
        report = validate_snapshot(
            _manifest(),
            schema_report=_schema_report(findings=(_schema_finding(DriftSeverity.WARNING),)),
        )

        assert report.outcome is ValidationOutcome.PASS
        assert any(f.source == "schema_drift" for f in report.findings)

    def test_finding_carries_field_name_as_detail_ref(self) -> None:
        report = validate_snapshot(
            _manifest(),
            schema_report=_schema_report(findings=(_schema_finding(),)),
        )

        schema_findings = [f for f in report.findings if f.source == "schema_drift"]

        assert schema_findings[0].detail_ref == "businessname"


class TestTaxonomyDrift:
    def test_error_finding_fails_validation(self) -> None:
        report = validate_snapshot(
            _manifest(),
            taxonomy_report=_taxonomy_report(findings=(_taxonomy_finding(DriftSeverity.ERROR),)),
        )

        assert report.outcome is ValidationOutcome.FAIL
        assert any(f.source == "taxonomy_drift" for f in report.findings)

    def test_warning_finding_does_not_fail_validation(self) -> None:
        report = validate_snapshot(
            _manifest(),
            taxonomy_report=_taxonomy_report(findings=(_taxonomy_finding(DriftSeverity.WARNING),)),
        )

        assert report.outcome is ValidationOutcome.PASS
        assert any(f.source == "taxonomy_drift" for f in report.findings)

    def test_finding_carries_taxonomy_id_as_detail_ref(self) -> None:
        report = validate_snapshot(
            _manifest(),
            taxonomy_report=_taxonomy_report(findings=(_taxonomy_finding(),)),
        )

        tax_findings = [f for f in report.findings if f.source == "taxonomy_drift"]

        assert tax_findings[0].detail_ref == "vancouver-business-licence-status"


class TestMappingCoverage:
    def test_unmapped_quality_count_fails(self) -> None:
        report = validate_snapshot(_manifest(quality_counts={FieldQuality.UNMAPPED: 1}))

        assert report.outcome is ValidationOutcome.FAIL
        finding = next(f for f in report.findings if f.detail_ref == "unmapped")

        assert finding.severity is DriftSeverity.ERROR

    def test_conflicted_quality_count_fails(self) -> None:
        report = validate_snapshot(_manifest(quality_counts={FieldQuality.CONFLICTED: 2}))

        assert report.outcome is ValidationOutcome.FAIL
        assert any(f.detail_ref == "conflicted" for f in report.findings)

    def test_inferred_quality_count_warns_only(self) -> None:
        report = validate_snapshot(_manifest(quality_counts={FieldQuality.INFERRED: 5}))

        assert report.outcome is ValidationOutcome.PASS
        finding = next(f for f in report.findings if f.detail_ref == "inferred")

        assert finding.severity is DriftSeverity.WARNING

    def test_unmapped_source_fields_total_fails(self) -> None:
        report = validate_snapshot(_manifest(unmapped_source_fields_total=3))

        assert report.outcome is ValidationOutcome.FAIL
        assert any(f.detail_ref == "unmapped_source_fields_total" for f in report.findings)

    def test_conflicts_total_fails(self) -> None:
        report = validate_snapshot(_manifest(conflicts_total=1))

        assert report.outcome is ValidationOutcome.FAIL
        assert any(f.detail_ref == "conflicts_total" for f in report.findings)

    def test_zero_counts_emit_no_findings(self) -> None:
        report = validate_snapshot(
            _manifest(quality_counts={FieldQuality.DIRECT: 100, FieldQuality.NOT_PROVIDED: 5})
        )

        assert report.outcome is ValidationOutcome.PASS
        assert all(f.source != "mapping_coverage" for f in report.findings)


class TestRecordCount:
    def test_schema_count_mismatch_fails(self) -> None:
        report = validate_snapshot(
            _manifest(record_count=10),
            schema_report=_schema_report(checked_record_count=9),
        )

        assert report.outcome is ValidationOutcome.FAIL
        finding = next(f for f in report.findings if f.source == "record_count")

        assert finding.detail_ref == "schema_drift"

    def test_taxonomy_count_mismatch_fails(self) -> None:
        report = validate_snapshot(
            _manifest(record_count=10),
            taxonomy_report=_taxonomy_report(checked_record_count=11),
        )

        assert report.outcome is ValidationOutcome.FAIL
        finding = next(f for f in report.findings if f.source == "record_count")

        assert finding.detail_ref == "taxonomy_drift"

    def test_matching_counts_emit_no_record_count_findings(self) -> None:
        report = validate_snapshot(
            _manifest(record_count=10),
            schema_report=_schema_report(checked_record_count=10),
            taxonomy_report=_taxonomy_report(checked_record_count=10),
        )

        assert all(f.source != "record_count" for f in report.findings)


class TestInputIdentityMismatch:
    def test_schema_report_with_different_snapshot_id_fails(self) -> None:
        report = validate_snapshot(
            _manifest(),
            schema_report=_schema_report(snapshot_id=SnapshotId("other-snap")),
        )

        assert report.outcome is ValidationOutcome.FAIL
        finding = next(f for f in report.findings if f.source == "input_identity")

        assert finding.detail_ref == "schema_drift"

    def test_mismatched_schema_findings_are_skipped(self) -> None:
        # The schema report carries an ERROR finding, but identity does not
        # match; that ERROR must not propagate into the validation report.
        report = validate_snapshot(
            _manifest(),
            schema_report=_schema_report(
                snapshot_id=SnapshotId("other-snap"),
                findings=(_schema_finding(DriftSeverity.ERROR),),
            ),
        )

        assert all(f.source != "schema_drift" for f in report.findings)

    def test_mismatched_record_count_check_is_skipped(self) -> None:
        report = validate_snapshot(
            _manifest(record_count=10),
            schema_report=_schema_report(
                snapshot_id=SnapshotId("other-snap"),
                checked_record_count=999,
            ),
        )

        assert all(f.source != "record_count" for f in report.findings)

    def test_taxonomy_report_with_different_fetched_at_fails(self) -> None:
        # Build a taxonomy report whose only difference is fetched_at; we have
        # to construct it directly because the helper pins fetched_at.
        report_with_other_time = TaxonomyDriftReport(
            snapshot_id=SNAP,
            source_id=SOURCE,
            dataset_id=DATASET,
            jurisdiction=JURISDICTION,
            fetched_at=PINNED_NOW + timedelta(hours=1),
            spec_versions={"status": "1"},
            checked_record_count=10,
            findings=(),
        )

        report = validate_snapshot(_manifest(), taxonomy_report=report_with_other_time)

        assert report.outcome is ValidationOutcome.FAIL
        assert any(f.source == "input_identity" for f in report.findings)
