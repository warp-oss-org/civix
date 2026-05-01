"""Comparison passes that produce drift reports from observations + specs."""

from __future__ import annotations

from collections.abc import Iterable

from civix.core.drift.models.report import (
    DriftSeverity,
    SchemaDriftFinding,
    SchemaDriftKind,
    SchemaDriftReport,
    TaxonomyDriftFinding,
    TaxonomyDriftKind,
    TaxonomyDriftReport,
)
from civix.core.drift.models.spec import (
    JsonFieldKind,
    SchemaFieldSpec,
    SourceSchemaSpec,
    TaxonomySpec,
)
from civix.core.drift.observation import (
    ObservedField,
    ObservedSchema,
    ObservedTaxonomy,
    ObservedTaxonomyValue,
    observe_schema,
    observe_taxonomy,
)
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot


def compare_schema(
    snapshot: SourceSnapshot,
    observed: ObservedSchema,
    spec: SourceSchemaSpec,
) -> SchemaDriftReport:
    """Compare an observed schema to an explicit source schema spec."""
    observed_fields = observed.fields
    findings: list[SchemaDriftFinding] = []

    for field_spec in spec.fields:
        findings.extend(
            _findings_for_expected_field(
                field_spec=field_spec,
                observed_field=observed_fields.get(field_spec.name),
                checked_record_count=observed.record_count,
            )
        )

    findings.extend(_unexpected_field_findings(observed=observed, spec=spec))

    return SchemaDriftReport(
        snapshot_id=snapshot.snapshot_id,
        source_id=snapshot.source_id,
        dataset_id=snapshot.dataset_id,
        jurisdiction=snapshot.jurisdiction,
        fetched_at=snapshot.fetched_at,
        spec_id=spec.spec_id,
        spec_version=spec.version,
        checked_record_count=observed.record_count,
        findings=tuple(findings),
    )


def analyze_schema(
    snapshot: SourceSnapshot,
    records: Iterable[RawRecord],
    spec: SourceSchemaSpec,
) -> SchemaDriftReport:
    """Observe and compare raw records against a source schema spec."""
    observed = observe_schema(records)

    return compare_schema(snapshot=snapshot, observed=observed, spec=spec)


def compare_taxonomy(
    snapshot: SourceSnapshot,
    observed: ObservedTaxonomy,
    specs: tuple[TaxonomySpec, ...],
) -> TaxonomyDriftReport:
    """Compare observed taxonomy values to one or more explicit specs."""
    findings: list[TaxonomyDriftFinding] = []

    for spec in specs:
        findings.extend(
            _taxonomy_findings_for_spec(
                spec=spec,
                observed_values=observed.by_taxonomy.get(spec.taxonomy_id, ()),
            )
        )

    return TaxonomyDriftReport(
        snapshot_id=snapshot.snapshot_id,
        source_id=snapshot.source_id,
        dataset_id=snapshot.dataset_id,
        jurisdiction=snapshot.jurisdiction,
        fetched_at=snapshot.fetched_at,
        spec_versions={spec.taxonomy_id: spec.version for spec in specs},
        checked_record_count=observed.record_count,
        findings=tuple(findings),
    )


def analyze_taxonomy(
    snapshot: SourceSnapshot,
    records: Iterable[RawRecord],
    specs: tuple[TaxonomySpec, ...],
) -> TaxonomyDriftReport:
    """Observe and compare raw records against one or more taxonomy specs."""
    observed = observe_taxonomy(records, specs)

    return compare_taxonomy(snapshot=snapshot, observed=observed, specs=specs)


def _findings_for_expected_field(
    *,
    field_spec: SchemaFieldSpec,
    observed_field: ObservedField | None,
    checked_record_count: int,
) -> list[SchemaDriftFinding]:
    if observed_field is None:
        return _missing_whole_field_findings(
            field_spec=field_spec,
            checked_record_count=checked_record_count,
        )

    findings: list[SchemaDriftFinding] = []
    findings.extend(_missing_value_findings(field_spec=field_spec, observed_field=observed_field))
    findings.extend(_nullability_findings(field_spec=field_spec, observed_field=observed_field))
    findings.extend(_type_mismatch_findings(field_spec=field_spec, observed_field=observed_field))

    return findings


def _missing_whole_field_findings(
    *,
    field_spec: SchemaFieldSpec,
    checked_record_count: int,
) -> list[SchemaDriftFinding]:
    if checked_record_count == 0:
        return []

    return [
        _schema_finding(
            kind=SchemaDriftKind.MISSING_FIELD,
            severity=DriftSeverity.ERROR,
            field_name=field_spec.name,
            expected="present",
            observed="missing",
            count=checked_record_count,
        )
    ]


def _missing_value_findings(
    *,
    field_spec: SchemaFieldSpec,
    observed_field: ObservedField,
) -> list[SchemaDriftFinding]:
    if observed_field.missing_count == 0:
        return []

    return [
        _schema_finding(
            kind=SchemaDriftKind.MISSING_FIELD,
            severity=DriftSeverity.ERROR,
            field_name=field_spec.name,
            expected="present",
            observed="missing",
            count=observed_field.missing_count,
            sample_source_record_ids=observed_field.missing_sample_source_record_ids,
        )
    ]


def _nullability_findings(
    *,
    field_spec: SchemaFieldSpec,
    observed_field: ObservedField,
) -> list[SchemaDriftFinding]:
    if field_spec.nullable or observed_field.null_count == 0:
        return []

    return [
        _schema_finding(
            kind=SchemaDriftKind.NULLABILITY_MISMATCH,
            severity=DriftSeverity.ERROR,
            field_name=field_spec.name,
            expected="non-null",
            observed="null",
            count=observed_field.null_count,
            sample_source_record_ids=observed_field.null_sample_source_record_ids,
        )
    ]


def _type_mismatch_findings(
    *,
    field_spec: SchemaFieldSpec,
    observed_field: ObservedField,
) -> list[SchemaDriftFinding]:
    findings: list[SchemaDriftFinding] = []
    allowed_kinds = set(field_spec.kinds)

    for kind, count in observed_field.kind_counts.items():
        if kind in allowed_kinds:
            continue

        findings.append(
            _schema_finding(
                kind=SchemaDriftKind.TYPE_MISMATCH,
                severity=DriftSeverity.ERROR,
                field_name=field_spec.name,
                expected=_format_kinds(field_spec.kinds),
                observed=kind.value,
                count=count,
                sample_source_record_ids=observed_field.kind_sample_source_record_ids.get(kind, ()),
            )
        )

    for type_name, count in observed_field.unsupported_type_counts.items():
        findings.append(
            _schema_finding(
                kind=SchemaDriftKind.TYPE_MISMATCH,
                severity=DriftSeverity.ERROR,
                field_name=field_spec.name,
                expected=_format_kinds(field_spec.kinds),
                observed=f"unsupported:{type_name}",
                count=count,
                sample_source_record_ids=(
                    observed_field.unsupported_type_sample_source_record_ids.get(type_name, ())
                ),
            )
        )

    return findings


def _unexpected_field_findings(
    *,
    observed: ObservedSchema,
    spec: SourceSchemaSpec,
) -> list[SchemaDriftFinding]:
    expected_field_names = {field.name for field in spec.fields}
    unexpected_field_names = sorted(set(observed.fields) - expected_field_names)

    return [
        _schema_finding(
            kind=SchemaDriftKind.UNEXPECTED_FIELD,
            severity=DriftSeverity.WARNING,
            field_name=field_name,
            expected="not present",
            observed="present",
            count=observed.fields[field_name].present_count,
            sample_source_record_ids=observed.fields[field_name].sample_source_record_ids,
        )
        for field_name in unexpected_field_names
    ]


def _taxonomy_findings_for_spec(
    *,
    spec: TaxonomySpec,
    observed_values: tuple[ObservedTaxonomyValue, ...],
) -> list[TaxonomyDriftFinding]:
    findings: list[TaxonomyDriftFinding] = []

    for observed_value in observed_values:
        finding = _taxonomy_finding_for_value(spec=spec, observed_value=observed_value)
        if finding is not None:
            findings.append(finding)

    return findings


def _taxonomy_finding_for_value(
    *,
    spec: TaxonomySpec,
    observed_value: ObservedTaxonomyValue,
) -> TaxonomyDriftFinding | None:
    if observed_value.value in spec.retired_values:
        return _taxonomy_finding(
            kind=TaxonomyDriftKind.RETIRED_VALUE_OBSERVED,
            severity=DriftSeverity.WARNING,
            spec=spec,
            observed_value=observed_value.value,
            count=observed_value.count,
            raw_samples=observed_value.raw_samples,
            sample_source_record_ids=observed_value.sample_source_record_ids,
        )

    if observed_value.value in spec.known_values:
        return None

    return _taxonomy_finding(
        kind=TaxonomyDriftKind.UNRECOGNIZED_VALUE,
        severity=DriftSeverity.ERROR,
        spec=spec,
        observed_value=observed_value.value,
        count=observed_value.count,
        raw_samples=observed_value.raw_samples,
        sample_source_record_ids=observed_value.sample_source_record_ids,
    )


def _schema_finding(
    *,
    kind: SchemaDriftKind,
    severity: DriftSeverity,
    field_name: str,
    expected: str,
    observed: str,
    count: int,
    sample_source_record_ids: tuple[str, ...] = (),
) -> SchemaDriftFinding:
    return SchemaDriftFinding(
        kind=kind,
        severity=severity,
        field_name=field_name,
        expected=expected,
        observed=observed,
        count=count,
        sample_source_record_ids=sample_source_record_ids,
    )


def _taxonomy_finding(
    *,
    kind: TaxonomyDriftKind,
    severity: DriftSeverity,
    spec: TaxonomySpec,
    observed_value: str,
    count: int,
    raw_samples: tuple[str, ...],
    sample_source_record_ids: tuple[str, ...],
) -> TaxonomyDriftFinding:
    return TaxonomyDriftFinding(
        kind=kind,
        severity=severity,
        taxonomy_id=spec.taxonomy_id,
        source_field=spec.source_field,
        observed_value=observed_value,
        count=count,
        raw_samples=raw_samples,
        sample_source_record_ids=sample_source_record_ids,
    )


def _format_kinds(kinds: tuple[JsonFieldKind, ...]) -> str:
    return "|".join(kind.value for kind in kinds)
