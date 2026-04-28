"""Tests for taxonomy drift contracts and analysis."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from civix.core.drift import (
    DriftSeverity,
    TaxonomyDriftKind,
    TaxonomyNormalization,
    TaxonomySpec,
    analyze_taxonomy,
)
from civix.core.identity import DatasetId, Jurisdiction, SnapshotId, SourceId
from civix.core.snapshots import RawRecord, SourceSnapshot

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


def _status_spec(*, normalization: TaxonomyNormalization = "exact") -> TaxonomySpec:
    return TaxonomySpec(
        taxonomy_id="status",
        version="2026-04-25",
        source_field="status",
        normalization=normalization,
        known_values=frozenset({"issued", "active", "cancelled"}),
    )


class TestTaxonomySpec:
    def test_overlapping_known_and_retired_rejected(self) -> None:
        with pytest.raises(ValidationError, match="must not overlap"):
            TaxonomySpec(
                taxonomy_id="x",
                version="1",
                source_field="f",
                known_values=frozenset({"a", "b"}),
                retired_values=frozenset({"b"}),
            )

    def test_blank_taxonomy_id_rejected(self) -> None:
        with pytest.raises(ValidationError):
            TaxonomySpec(
                taxonomy_id="",
                version="1",
                source_field="f",
                known_values=frozenset({"a"}),
            )


class TestAnalyzeTaxonomy:
    def test_no_findings_when_all_values_known_exact(self) -> None:
        report = analyze_taxonomy(
            _snapshot(),
            [
                _record("r1", status="issued"),
                _record("r2", status="cancelled"),
            ],
            (_status_spec(),),
        )

        assert report.findings == ()
        assert not report.has_errors

    def test_unrecognized_value_is_error(self) -> None:
        report = analyze_taxonomy(
            _snapshot(),
            [
                _record("r1", status="issued"),
                _record("r2", status="surrendered"),
            ],
            (_status_spec(),),
        )

        assert len(report.findings) == 1
        finding = report.findings[0]

        assert finding.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE
        assert finding.severity is DriftSeverity.ERROR
        assert finding.observed_value == "surrendered"
        assert finding.count == 1
        assert finding.sample_source_record_ids == ("r2",)
        assert report.has_errors

    def test_strip_casefold_normalizes_before_compare(self) -> None:
        report = analyze_taxonomy(
            _snapshot(),
            [
                _record("r1", status="Issued"),  # raw differs, normalizes to "issued"
                _record("r2", status="  CANCELLED  "),
            ],
            (_status_spec(normalization="strip_casefold"),),
        )

        assert report.findings == ()

    def test_raw_samples_preserve_unnormalized_variants(self) -> None:
        report = analyze_taxonomy(
            _snapshot(),
            [
                _record("r1", status="Surrendered"),
                _record("r2", status="surrendered"),
                _record("r3", status="SURRENDERED"),
            ],
            (_status_spec(normalization="strip_casefold"),),
        )

        assert len(report.findings) == 1
        finding = report.findings[0]

        assert finding.observed_value == "surrendered"
        assert finding.count == 3
        assert set(finding.raw_samples) == {"Surrendered", "surrendered", "SURRENDERED"}

    def test_retired_value_observed_is_warning(self) -> None:
        spec = TaxonomySpec(
            taxonomy_id="status",
            version="2026-04-25",
            source_field="status",
            known_values=frozenset({"issued", "active"}),
            retired_values=frozenset({"superseded"}),
        )
        report = analyze_taxonomy(
            _snapshot(),
            [_record("r1", status="superseded")],
            (spec,),
        )

        assert len(report.findings) == 1
        finding = report.findings[0]

        assert finding.kind is TaxonomyDriftKind.RETIRED_VALUE_OBSERVED
        assert finding.severity is DriftSeverity.WARNING
        assert not report.has_errors

    def test_missing_source_field_is_silent(self) -> None:
        # Schema observer's job to flag missing fields; taxonomy stays focused.
        report = analyze_taxonomy(
            _snapshot(),
            [
                _record("r1", status="issued"),
                _record("r2", other_field="x"),
            ],
            (_status_spec(),),
        )

        assert report.findings == ()

    def test_non_string_value_is_silent(self) -> None:
        report = analyze_taxonomy(
            _snapshot(),
            [
                _record("r1", status="issued"),
                _record("r2", status=42),
            ],
            (_status_spec(),),
        )

        assert report.findings == ()

    def test_multiple_specs_report_independently(self) -> None:
        type_spec = TaxonomySpec(
            taxonomy_id="business_type",
            version="2026-04-25",
            source_field="businesstype",
            known_values=frozenset({"restaurant"}),
        )
        report = analyze_taxonomy(
            _snapshot(),
            [
                _record("r1", status="issued", businesstype="Office"),
                _record("r2", status="surrendered", businesstype="restaurant"),
            ],
            (_status_spec(), type_spec),
        )

        kinds = {(f.taxonomy_id, f.observed_value) for f in report.findings}

        assert kinds == {("status", "surrendered"), ("business_type", "Office")}
        assert report.spec_versions == {"status": "2026-04-25", "business_type": "2026-04-25"}
