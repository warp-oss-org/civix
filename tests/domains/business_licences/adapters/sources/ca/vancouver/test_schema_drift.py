"""Schema drift tests for Vancouver business-licence raw records."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from civix.core.drift import DriftSeverity, SchemaDriftKind, SchemaDriftReport, analyze_schema
from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, SnapshotId, SourceId
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.domains.business_licences.adapters.sources.ca.vancouver import (
    VANCOUVER_BUSINESS_LICENCES_SCHEMA,
)

PINNED_NOW = datetime(2026, 4, 25, 12, 0, tzinfo=UTC)
FIXTURES = Path(__file__).parent / "fixtures"


def _snapshot(*, record_count: int) -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId("snap-1"),
        source_id=SourceId("vancouver-open-data"),
        dataset_id=DatasetId("business-licences"),
        jurisdiction=Jurisdiction(country="CA", region="BC", locality="Vancouver"),
        fetched_at=PINNED_NOW,
        record_count=record_count,
    )


def _fixture_raw_rows() -> list[dict[str, Any]]:
    return [
        json.loads(line) for line in (FIXTURES / "records.jsonl").read_text().splitlines() if line
    ]


def _records(rows: list[dict[str, Any]]) -> list[RawRecord]:
    return [
        RawRecord(
            snapshot_id=SnapshotId("snap-1"),
            raw_data=row,
            source_record_id=str(row["licencersn"]),
        )
        for row in rows
    ]


def _analyze(rows: list[dict[str, Any]]) -> SchemaDriftReport:
    return analyze_schema(
        _snapshot(record_count=len(rows)),
        _records(rows),
        VANCOUVER_BUSINESS_LICENCES_SCHEMA,
    )


class TestVancouverSchemaDrift:
    def test_fixture_raw_records_match_schema_spec(self) -> None:
        report = _analyze(_fixture_raw_rows())

        assert report.findings == ()
        assert not report.has_errors

    def test_synthetic_added_field_reports_warning(self) -> None:
        rows = _fixture_raw_rows()
        rows[0]["new_portal_field"] = "surprise"

        report = _analyze(rows)

        assert len(report.findings) == 1
        finding = report.findings[0]

        assert finding.kind is SchemaDriftKind.UNEXPECTED_FIELD
        assert finding.severity is DriftSeverity.WARNING
        assert finding.field_name == "new_portal_field"
        assert finding.sample_source_record_ids == ("1234567",)

    def test_synthetic_removed_mapped_field_reports_error(self) -> None:
        rows = _fixture_raw_rows()
        del rows[1]["businessname"]

        report = _analyze(rows)

        assert len(report.findings) == 1
        finding = report.findings[0]

        assert finding.kind is SchemaDriftKind.MISSING_FIELD
        assert finding.severity is DriftSeverity.ERROR
        assert finding.field_name == "businessname"
        assert finding.count == 1
        assert finding.sample_source_record_ids == ("1234568",)
        assert report.has_errors

    def test_nullable_fields_accept_null(self) -> None:
        rows = _fixture_raw_rows()
        rows[0]["businesssubtype"] = None
        rows[0]["postalcode"] = None
        rows[0]["geo_point_2d"] = None
        rows[0]["numberofemployees"] = None

        report = _analyze(rows)

        assert report.findings == ()
