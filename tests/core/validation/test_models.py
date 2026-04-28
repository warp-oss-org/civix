"""Tests for the validation report contracts."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from civix.core.drift import DriftSeverity
from civix.core.identity import DatasetId, Jurisdiction, SnapshotId, SourceId
from civix.core.validation import (
    ValidationFinding,
    ValidationOutcome,
    ValidationReport,
)

PINNED_NOW = datetime(2026, 4, 28, 12, 0, tzinfo=UTC)
SNAP = SnapshotId("snap-1")
SOURCE = SourceId("vancouver-open-data")
DATASET = DatasetId("business-licences")
JURISDICTION = Jurisdiction(country="CA", region="BC", locality="Vancouver")


def _report(
    *,
    outcome: ValidationOutcome = ValidationOutcome.PASS,
    findings: tuple[ValidationFinding, ...] = (),
) -> ValidationReport:
    return ValidationReport(
        snapshot_id=SNAP,
        source_id=SOURCE,
        dataset_id=DATASET,
        jurisdiction=JURISDICTION,
        fetched_at=PINNED_NOW,
        outcome=outcome,
        findings=findings,
    )


class TestValidationFinding:
    def test_constructs_with_required_fields(self) -> None:
        finding = ValidationFinding(
            source="schema_drift",
            severity=DriftSeverity.ERROR,
            message="x",
        )

        assert finding.detail_ref is None

    def test_rejects_empty_message(self) -> None:
        with pytest.raises(ValidationError):
            ValidationFinding(
                source="schema_drift",
                severity=DriftSeverity.ERROR,
                message="",
            )

    def test_rejects_unknown_source(self) -> None:
        with pytest.raises(ValidationError):
            ValidationFinding(
                source="not_a_source",  # type: ignore[arg-type]
                severity=DriftSeverity.ERROR,
                message="x",
            )


class TestValidationReport:
    def test_has_errors_true_when_outcome_is_fail(self) -> None:
        report = _report(outcome=ValidationOutcome.FAIL)

        assert report.has_errors is True

    def test_has_errors_false_when_outcome_is_pass(self) -> None:
        report = _report(outcome=ValidationOutcome.PASS)

        assert report.has_errors is False

    def test_rejects_naive_fetched_at(self) -> None:
        with pytest.raises(ValidationError):
            ValidationReport(
                snapshot_id=SNAP,
                source_id=SOURCE,
                dataset_id=DATASET,
                jurisdiction=JURISDICTION,
                fetched_at=datetime(2026, 4, 28, 12, 0),
                outcome=ValidationOutcome.PASS,
            )

    def test_is_frozen(self) -> None:
        report = _report()

        with pytest.raises(ValidationError):
            report.outcome = ValidationOutcome.FAIL  # type: ignore[misc]
