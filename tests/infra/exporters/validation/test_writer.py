"""Unit tests for the validation sibling-artifact writer."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

from civix.core.drift import DriftSeverity
from civix.core.identity import DatasetId, Jurisdiction, SnapshotId, SourceId
from civix.core.validation import (
    ValidationFinding,
    ValidationOutcome,
    ValidationReport,
)
from civix.infra.exporters.validation import write_validation

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


class TestWriteValidation:
    def test_writes_validation_json_round_trips(self, tmp_path: Path) -> None:
        report = _report(
            outcome=ValidationOutcome.FAIL,
            findings=(
                ValidationFinding(
                    source="schema_drift",
                    severity=DriftSeverity.ERROR,
                    message="missing field",
                    detail_ref="businessname",
                ),
            ),
        )

        entry = write_validation(snapshot_dir=tmp_path, report=report)
        on_disk = json.loads((tmp_path / "validation.json").read_text())

        assert entry.filename == "validation.json"
        assert on_disk["outcome"] == "fail"
        assert on_disk["findings"][0]["detail_ref"] == "businessname"

    def test_file_entry_hash_matches_on_disk_bytes(self, tmp_path: Path) -> None:
        entry = write_validation(snapshot_dir=tmp_path, report=_report())

        on_disk_bytes = (tmp_path / "validation.json").read_bytes()

        assert entry.sha256 == hashlib.sha256(on_disk_bytes).hexdigest()
        assert entry.byte_count == len(on_disk_bytes)

    def test_creates_snapshot_dir_if_missing(self, tmp_path: Path) -> None:
        nested = tmp_path / "snap" / "deeper"

        write_validation(snapshot_dir=nested, report=_report())

        assert (nested / "validation.json").is_file()

    def test_no_tmp_file_remains_after_write(self, tmp_path: Path) -> None:
        write_validation(snapshot_dir=tmp_path, report=_report())

        assert list(tmp_path.glob("*.tmp")) == []
