"""Unit tests for the drift sibling-artifact writer."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from civix.core.drift import (
    DriftSeverity,
    SchemaDriftFinding,
    SchemaDriftKind,
    SchemaDriftReport,
    TaxonomyDriftFinding,
    TaxonomyDriftKind,
    TaxonomyDriftReport,
)
from civix.core.identity import DatasetId, Jurisdiction, SnapshotId, SourceId
from civix.infra.exporters.drift import write_drift

PINNED_NOW = datetime(2026, 4, 25, 12, 0, tzinfo=UTC)
SNAP = SnapshotId("snap-1")
SOURCE = SourceId("vancouver-open-data")
DATASET = DatasetId("business-licences")
JURISDICTION = Jurisdiction(country="CA", region="BC", locality="Vancouver")


def _schema_report(findings: tuple[SchemaDriftFinding, ...] = ()) -> SchemaDriftReport:
    return SchemaDriftReport(
        snapshot_id=SNAP,
        source_id=SOURCE,
        dataset_id=DATASET,
        jurisdiction=JURISDICTION,
        fetched_at=PINNED_NOW,
        spec_id="spec",
        spec_version="1",
        checked_record_count=10,
        findings=findings,
    )


def _taxonomy_report(findings: tuple[TaxonomyDriftFinding, ...] = ()) -> TaxonomyDriftReport:
    return TaxonomyDriftReport(
        snapshot_id=SNAP,
        source_id=SOURCE,
        dataset_id=DATASET,
        jurisdiction=JURISDICTION,
        fetched_at=PINNED_NOW,
        spec_versions={"status": "1"},
        checked_record_count=10,
        findings=findings,
    )


class TestWriteDrift:
    def test_writes_drift_json_with_both_sections(self, tmp_path: Path) -> None:
        entry = write_drift(
            snapshot_dir=tmp_path,
            schema=_schema_report(),
            taxonomy=_taxonomy_report(),
        )

        on_disk = json.loads((tmp_path / "drift.json").read_text())

        assert entry.filename == "drift.json"
        assert set(on_disk) == {"schema", "taxonomy"}

    def test_omits_section_when_observer_not_attached(self, tmp_path: Path) -> None:
        write_drift(snapshot_dir=tmp_path, schema=_schema_report())

        on_disk = json.loads((tmp_path / "drift.json").read_text())

        assert set(on_disk) == {"schema"}

    def test_file_entry_hash_matches_on_disk_bytes(self, tmp_path: Path) -> None:
        entry = write_drift(snapshot_dir=tmp_path, taxonomy=_taxonomy_report())

        on_disk = (tmp_path / "drift.json").read_bytes()

        assert entry.sha256 == hashlib.sha256(on_disk).hexdigest()
        assert entry.byte_count == len(on_disk)

    def test_findings_round_trip_through_report_models(self, tmp_path: Path) -> None:
        schema_finding = SchemaDriftFinding(
            kind=SchemaDriftKind.UNEXPECTED_FIELD,
            severity=DriftSeverity.WARNING,
            field_name="surprise",
            expected="not present",
            observed="present",
            count=1,
            sample_source_record_ids=("r1",),
        )
        taxonomy_finding = TaxonomyDriftFinding(
            kind=TaxonomyDriftKind.UNRECOGNIZED_VALUE,
            severity=DriftSeverity.ERROR,
            taxonomy_id="status",
            source_field="status",
            observed_value="surrendered",
            count=2,
            raw_samples=("Surrendered", "surrendered"),
            sample_source_record_ids=("r1", "r2"),
        )

        write_drift(
            snapshot_dir=tmp_path,
            schema=_schema_report((schema_finding,)),
            taxonomy=_taxonomy_report((taxonomy_finding,)),
        )
        payload = json.loads((tmp_path / "drift.json").read_text())

        assert payload["schema"]["findings"] == [schema_finding.model_dump(mode="json")]
        assert payload["taxonomy"]["findings"] == [taxonomy_finding.model_dump(mode="json")]
        assert payload["schema"]["has_errors"] is False
        assert payload["taxonomy"]["has_errors"] is True

    def test_empty_call_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="at least one"):
            write_drift(snapshot_dir=tmp_path)

    def test_creates_snapshot_dir_if_missing(self, tmp_path: Path) -> None:
        nested = tmp_path / "a" / "b"

        write_drift(snapshot_dir=nested, schema=_schema_report())

        assert (nested / "drift.json").exists()

    def test_no_tmp_files_remain(self, tmp_path: Path) -> None:
        write_drift(snapshot_dir=tmp_path, schema=_schema_report())

        assert not list(tmp_path.glob("*.tmp"))
