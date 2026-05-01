"""Pass/fail validator over a snapshot's artifacts.

`validate_snapshot` is pure: given the manifest and any drift reports
the caller produced, it applies V1's hard-coded rules and returns a
`ValidationReport`. No I/O, no record re-reads, no policy plumbing.

The manifest is the identity-bearing input. Drift reports are optional;
each is checked against the manifest's identity before its findings are
considered. If a drift report disagrees on identity, the finding for
that mismatch is the only thing recorded for it — its drift findings
are not comparable to the manifest and are skipped.
"""

from __future__ import annotations

from datetime import datetime

from civix.core.drift.models.report import (
    DriftSeverity,
    SchemaDriftReport,
    TaxonomyDriftReport,
)
from civix.core.export.models.manifest import ExportManifest
from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, SnapshotId, SourceId
from civix.core.quality.models.fields import FieldQuality
from civix.core.validation.models.report import (
    ValidationFinding,
    ValidationOutcome,
    ValidationReport,
)


def validate_snapshot(
    manifest: ExportManifest,
    schema_report: SchemaDriftReport | None = None,
    taxonomy_report: TaxonomyDriftReport | None = None,
) -> ValidationReport:
    """Apply V1 validation rules to one snapshot's artifacts.

    `manifest` is required: it carries snapshot identity and mapping
    coverage. Drift reports are optional; if provided, their identity
    must match `manifest`'s — a mismatch is its own FAIL finding and
    suppresses further checks against that report.
    """
    findings: list[ValidationFinding] = []

    schema_ok = _check_identity(
        findings=findings,
        manifest=manifest,
        report=schema_report,
        report_kind="schema_drift",
    )
    taxonomy_ok = _check_identity(
        findings=findings,
        manifest=manifest,
        report=taxonomy_report,
        report_kind="taxonomy_drift",
    )

    if schema_report is not None and schema_ok:
        findings.extend(_schema_findings(schema_report))
        findings.extend(
            _record_count_findings(
                manifest_count=manifest.record_count,
                report_count=schema_report.checked_record_count,
                report_kind="schema_drift",
            )
        )

    if taxonomy_report is not None and taxonomy_ok:
        findings.extend(_taxonomy_findings(taxonomy_report))
        findings.extend(
            _record_count_findings(
                manifest_count=manifest.record_count,
                report_count=taxonomy_report.checked_record_count,
                report_kind="taxonomy_drift",
            )
        )

    findings.extend(_mapping_coverage_findings(manifest))

    return ValidationReport(
        snapshot_id=manifest.snapshot_id,
        source_id=manifest.source_id,
        dataset_id=manifest.dataset_id,
        jurisdiction=manifest.jurisdiction,
        fetched_at=manifest.fetched_at,
        outcome=_outcome_from(findings),
        findings=tuple(findings),
    )


def _check_identity(
    *,
    findings: list[ValidationFinding],
    manifest: ExportManifest,
    report: SchemaDriftReport | TaxonomyDriftReport | None,
    report_kind: str,
) -> bool:
    if report is None:
        return True

    expected = _identity_quintet(
        snapshot_id=manifest.snapshot_id,
        source_id=manifest.source_id,
        dataset_id=manifest.dataset_id,
        jurisdiction=manifest.jurisdiction,
        fetched_at=manifest.fetched_at,
    )
    actual = _identity_quintet(
        snapshot_id=report.snapshot_id,
        source_id=report.source_id,
        dataset_id=report.dataset_id,
        jurisdiction=report.jurisdiction,
        fetched_at=report.fetched_at,
    )

    if expected == actual:
        return True

    findings.append(
        ValidationFinding(
            source="input_identity",
            severity=DriftSeverity.ERROR,
            message=(
                f"{report_kind} identity does not match manifest "
                f"(expected {expected}, got {actual})"
            ),
            detail_ref=report_kind,
        )
    )

    return False


def _schema_findings(report: SchemaDriftReport) -> list[ValidationFinding]:
    return [
        ValidationFinding(
            source="schema_drift",
            severity=finding.severity,
            message=(
                f"{finding.kind.value} on field {finding.field_name!r}: "
                f"expected {finding.expected!r}, observed {finding.observed!r} "
                f"in {finding.count:,} record(s)"
            ),
            detail_ref=finding.field_name,
        )
        for finding in report.findings
    ]


def _taxonomy_findings(report: TaxonomyDriftReport) -> list[ValidationFinding]:
    return [
        ValidationFinding(
            source="taxonomy_drift",
            severity=finding.severity,
            message=(
                f"{finding.kind.value} for taxonomy {finding.taxonomy_id!r} "
                f"on field {finding.source_field!r}: value "
                f"{finding.observed_value!r} in {finding.count:,} record(s)"
            ),
            detail_ref=finding.taxonomy_id,
        )
        for finding in report.findings
    ]


def _mapping_coverage_findings(manifest: ExportManifest) -> list[ValidationFinding]:
    summary = manifest.mapping_summary
    findings: list[ValidationFinding] = []

    for quality, severity in (
        (FieldQuality.UNMAPPED, DriftSeverity.ERROR),
        (FieldQuality.CONFLICTED, DriftSeverity.ERROR),
        (FieldQuality.INFERRED, DriftSeverity.WARNING),
    ):
        count = summary.quality_counts.get(quality, 0)
        if count > 0:
            findings.append(
                ValidationFinding(
                    source="mapping_coverage",
                    severity=severity,
                    message=(f"{count:,} mapped field(s) have quality {quality.value!r}"),
                    detail_ref=quality.value,
                )
            )

    if summary.unmapped_source_fields_total > 0:
        findings.append(
            ValidationFinding(
                source="mapping_coverage",
                severity=DriftSeverity.ERROR,
                message=(
                    f"{summary.unmapped_source_fields_total:,} unmapped source "
                    f"field reference(s) across mapping reports"
                ),
                detail_ref="unmapped_source_fields_total",
            )
        )

    if summary.conflicts_total > 0:
        findings.append(
            ValidationFinding(
                source="mapping_coverage",
                severity=DriftSeverity.ERROR,
                message=(f"{summary.conflicts_total:,} mapping conflict(s) across mapping reports"),
                detail_ref="conflicts_total",
            )
        )

    return findings


def _record_count_findings(
    *,
    manifest_count: int,
    report_count: int,
    report_kind: str,
) -> list[ValidationFinding]:
    if manifest_count == report_count:
        return []

    return [
        ValidationFinding(
            source="record_count",
            severity=DriftSeverity.ERROR,
            message=(
                f"manifest record_count {manifest_count:,} does not match "
                f"{report_kind} checked_record_count {report_count:,}"
            ),
            detail_ref=report_kind,
        )
    ]


def _identity_quintet(
    *,
    snapshot_id: SnapshotId,
    source_id: SourceId,
    dataset_id: DatasetId,
    jurisdiction: Jurisdiction,
    fetched_at: datetime,
) -> tuple[str, str, str, tuple[str, str | None, str | None], str]:
    return (
        snapshot_id,
        source_id,
        dataset_id,
        (jurisdiction.country, jurisdiction.region, jurisdiction.locality),
        fetched_at.isoformat(),
    )


def _outcome_from(findings: list[ValidationFinding]) -> ValidationOutcome:
    if any(finding.severity is DriftSeverity.ERROR for finding in findings):
        return ValidationOutcome.FAIL

    return ValidationOutcome.PASS
