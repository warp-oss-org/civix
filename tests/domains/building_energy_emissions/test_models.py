from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from civix.core.identity.models.identifiers import (
    DatasetId,
    Jurisdiction,
    MapperId,
    SnapshotId,
    SourceId,
)
from civix.core.provenance.models.provenance import MapperVersion, ProvenanceRef
from civix.core.quality.models.fields import FieldQuality, MappedField
from civix.core.spatial.models.location import Address, Coordinate
from civix.core.taxonomy.models.category import CategoryRef
from civix.core.temporal import TemporalPeriod, TemporalPeriodPrecision, TemporalTimezoneStatus
from civix.domains.building_energy_emissions import (
    BuildingComplianceCase,
    BuildingEnergyReport,
    BuildingEnergySubject,
    BuildingMetricValue,
    BuildingSubjectKind,
    CategoryMetricMeasure,
    ComplianceLifecycleStatus,
    EmissionsMetricType,
    EnergyMetricType,
    IdentityCertainty,
    MetricFamily,
    MetricValueSource,
    NumericMetricMeasure,
    ReportingPeriodPrecision,
    SourceIdentifier,
    SourceValueState,
    TextMetricMeasure,
    WaterMetricType,
    build_building_compliance_case_key,
    build_building_energy_report_key,
    build_building_energy_subject_key,
    build_building_metric_value_key,
)

SOURCE_ID = SourceId("bee-test-source")
DATASET_ID = DatasetId("bee-test-dataset")


def _mapped[T](
    value: T | None,
    *source_fields: str,
    quality: FieldQuality = FieldQuality.DIRECT,
) -> MappedField[T]:
    return MappedField[T](value=value, quality=quality, source_fields=source_fields)


def _unmapped() -> MappedField[Any]:
    return MappedField[Any](value=None, quality=FieldQuality.UNMAPPED, source_fields=())


def _provenance(source_record_id: str = "report-row-1") -> ProvenanceRef:
    return ProvenanceRef(
        snapshot_id=SnapshotId("snap-bee-1"),
        source_id=SOURCE_ID,
        dataset_id=DATASET_ID,
        jurisdiction=Jurisdiction(country="US"),
        fetched_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
        mapper=MapperVersion(
            mapper_id=MapperId("bee-test-mapper"),
            version="0.1.0",
        ),
        source_record_id=source_record_id,
    )


def _category(code: str = "source-category") -> CategoryRef:
    return CategoryRef(
        code=code,
        label=code.replace("-", " ").title(),
        taxonomy_id="civix.bee.test",
        taxonomy_version="2026-05-01",
    )


def _period(value: date = date(2024, 12, 31)) -> TemporalPeriod:
    return TemporalPeriod(
        precision=TemporalPeriodPrecision.DATE,
        date_value=value,
        timezone_status=TemporalTimezoneStatus.UNKNOWN,
    )


def _subject_key(source_subject_id: str = "subject-1") -> str:
    return build_building_energy_subject_key(SOURCE_ID, DATASET_ID, source_subject_id)


def _report_key(source_report_id: str = "subject-1", reporting_period_token: str = "2024") -> str:
    return build_building_energy_report_key(
        SOURCE_ID, DATASET_ID, source_report_id, reporting_period_token
    )


def _case_key(source_case_id: str = "case-1", covered_period_token: str = "2024") -> str:
    return build_building_compliance_case_key(
        SOURCE_ID, DATASET_ID, source_case_id, covered_period_token
    )


def _metric_key(metric_discriminator: str = "site_eui") -> str:
    return build_building_metric_value_key(
        SOURCE_ID, DATASET_ID, _report_key(), metric_discriminator
    )


def _subject(**overrides: Any) -> BuildingEnergySubject:
    defaults: dict[str, Any] = {
        "provenance": _provenance("subject-row-1"),
        "subject_key": _subject_key(),
        "source_subject_identifiers": _mapped(
            (SourceIdentifier(value="prop-1", identifier_kind=_category("property-id")),),
            "propertyId",
        ),
        "subject_kind": _mapped(
            BuildingSubjectKind.PROPERTY,
            "subjectKind",
            quality=FieldQuality.STANDARDIZED,
        ),
        "source_subject_kind": _mapped(_category("portfolio-manager-property"), "subjectKind"),
        "identity_certainty": _mapped(
            IdentityCertainty.STABLE_CROSS_YEAR,
            "identitySource",
            quality=FieldQuality.STANDARDIZED,
        ),
        "parent_subject_key": _unmapped(),
        "name": _mapped("Test Property", "propertyName"),
        "jurisdiction": _mapped(Jurisdiction(country="US", region="NY"), "state"),
        "address": _mapped(
            Address(country="US", region="NY", locality="New York", postal_code="10001"),
            "address",
        ),
        "coordinate": _mapped(
            Coordinate(latitude=40.7, longitude=-74.0),
            "latitude",
            "longitude",
            quality=FieldQuality.STANDARDIZED,
        ),
        "property_types": _mapped((_category("office"),), "propertyType"),
        "floor_area": _mapped(Decimal("125000"), "grossFloorArea"),
        "floor_area_unit": _mapped(_category("ft2"), "grossFloorArea"),
        "year_built": _mapped(1985, "yearBuilt"),
        "occupancy_label": _mapped(_category("commercial"), "occupancy"),
        "ownership_label": _mapped(_category("private"), "ownership"),
        "source_caveats": _mapped((_category("self-reported"),), "metadata"),
    }
    defaults.update(overrides)

    return BuildingEnergySubject(**defaults)


def _report(**overrides: Any) -> BuildingEnergyReport:
    defaults: dict[str, Any] = {
        "provenance": _provenance("report-row-1"),
        "report_key": _report_key(),
        "subject_key": _subject_key(),
        "source_report_identifiers": _mapped(
            (SourceIdentifier(value="prop-1", identifier_kind=_category("report-id")),),
            "propertyId",
        ),
        "reporting_period": _mapped(_period(), "reportYear"),
        "reporting_period_precision": _mapped(
            ReportingPeriodPrecision.CALENDAR_YEAR,
            "reportYear",
            quality=FieldQuality.STANDARDIZED,
        ),
        "report_submission_date": _mapped(date(2025, 5, 1), "submissionDate"),
        "report_generation_date": _mapped(date(2025, 5, 2), "generationDate"),
        "report_status": _mapped(_category("submitted"), "reportStatus"),
        "data_quality_caveats": _mapped((_category("estimated-electricity"),), "dataQuality"),
        "source_caveats": _mapped((_category("self-reported"),), "metadata"),
    }
    defaults.update(overrides)

    return BuildingEnergyReport(**defaults)


def _metric(**overrides: Any) -> BuildingMetricValue:
    defaults: dict[str, Any] = {
        "provenance": _provenance("metric-row-1"),
        "metric_key": _metric_key(),
        "report_key": _mapped(_report_key(), "linkedReport"),
        "case_key": _unmapped(),
        "subject_key": _subject_key(),
        "metric_family": MetricFamily.ENERGY,
        "energy_metric_type": _mapped(
            EnergyMetricType.SITE_EUI,
            "siteEui",
            quality=FieldQuality.STANDARDIZED,
        ),
        "emissions_metric_type": _unmapped(),
        "water_metric_type": _unmapped(),
        "source_metric_label": _mapped(_category("site-eui-kbtu-ft2"), "siteEui"),
        "measure": _mapped(NumericMetricMeasure(value=Decimal("82.4")), "siteEui"),
        "value_state": _mapped(
            SourceValueState.REPORTED,
            "siteEui",
            quality=FieldQuality.STANDARDIZED,
        ),
        "unit": _mapped(_category("kbtu-per-ft2"), "siteEui"),
        "denominator": _mapped(_category("ft2"), "siteEui"),
        "normalization": _mapped(_category("not-normalized"), "methodology"),
        "fuel_or_scope": _mapped(_category("all-fuels"), "siteEui"),
        "methodology_label": _mapped("ENERGY STAR Portfolio Manager", "methodology"),
        "methodology_version": _mapped("ESPM-2024", "methodologyVersion"),
        "methodology_url": _unmapped(),
        "emission_factor_version": _unmapped(),
        "value_source": _mapped(
            MetricValueSource.SOURCE_PUBLISHED,
            "siteEui",
            quality=FieldQuality.STANDARDIZED,
        ),
        "effective_period": _mapped(_period(), "reportYear"),
        "source_caveats": _mapped((_category("self-reported"),), "metadata"),
    }
    defaults.update(overrides)

    return BuildingMetricValue(**defaults)


def _case(**overrides: Any) -> BuildingComplianceCase:
    defaults: dict[str, Any] = {
        "provenance": _provenance("case-row-1"),
        "case_key": _case_key(),
        "subject_key": _subject_key(),
        "related_report_key": _mapped(_report_key(), "linkedReport"),
        "source_case_identifiers": _mapped(
            (SourceIdentifier(value="case-1", identifier_kind=_category("case-id")),),
            "caseId",
        ),
        "covered_period": _mapped(_period(), "compliancePeriod"),
        "filing_period": _mapped(_period(date(2025, 5, 1)), "filingDate"),
        "covered_building_status": _mapped(
            ComplianceLifecycleStatus.COVERED,
            "coveredStatus",
            quality=FieldQuality.STANDARDIZED,
        ),
        "source_covered_status": _mapped(_category("covered"), "coveredStatus"),
        "compliance_pathway": _mapped(_category("article-320"), "pathway"),
        "compliance_status": _mapped(
            ComplianceLifecycleStatus.COMPLIANT,
            "complianceStatus",
            quality=FieldQuality.STANDARDIZED,
        ),
        "source_compliance_status": _mapped(_category("compliant"), "complianceStatus"),
        "exemption_status": _mapped(
            ComplianceLifecycleStatus.NOT_APPLICABLE,
            "exemption",
            quality=FieldQuality.STANDARDIZED,
        ),
        "extension_status": _mapped(
            ComplianceLifecycleStatus.NOT_APPLICABLE,
            "extension",
            quality=FieldQuality.STANDARDIZED,
        ),
        "emissions_limit_metric_key": _mapped(
            build_building_metric_value_key(
                SOURCE_ID, DATASET_ID, _report_key(), "emissions_limit"
            ),
            "emissionsLimit",
        ),
        "final_emissions_metric_key": _mapped(
            build_building_metric_value_key(
                SOURCE_ID, DATASET_ID, _report_key(), "final_emissions"
            ),
            "finalEmissions",
        ),
        "excess_emissions_metric_key": _unmapped(),
        "penalty_amount": _mapped(Decimal("0"), "penaltyAmount"),
        "penalty_currency": _mapped(_category("usd"), "penaltyCurrency"),
        "penalty_status": _mapped(
            ComplianceLifecycleStatus.NOT_APPLICABLE,
            "penaltyStatus",
            quality=FieldQuality.STANDARDIZED,
        ),
        "dispute_status": _mapped(
            ComplianceLifecycleStatus.NOT_APPLICABLE,
            "disputeStatus",
            quality=FieldQuality.STANDARDIZED,
        ),
        "source_caveats": _mapped((_category("preliminary"),), "metadata"),
    }
    defaults.update(overrides)

    return BuildingComplianceCase(**defaults)


class TestBuildingEnergyKeys:
    @pytest.mark.parametrize(
        ("builder", "prefix"),
        [
            (
                lambda: build_building_energy_subject_key(SOURCE_ID, DATASET_ID, "subject-1"),
                "bee-subject:v1:",
            ),
            (
                lambda: build_building_energy_report_key(
                    SOURCE_ID, DATASET_ID, "subject-1", "2024"
                ),
                "bee-report:v1:",
            ),
            (
                lambda: build_building_compliance_case_key(
                    SOURCE_ID, DATASET_ID, "case-1", "2024"
                ),
                "bee-case:v1:",
            ),
            (
                lambda: build_building_metric_value_key(
                    SOURCE_ID, DATASET_ID, _report_key(), "site_eui"
                ),
                "bee-metric:v1:",
            ),
        ],
    )
    def test_keys_are_deterministic_and_versioned(
        self, builder: Callable[[], str], prefix: str
    ) -> None:
        key = builder()

        assert key == builder()
        assert key.startswith(prefix)
        assert len(key.removeprefix(prefix)) == 64

    def test_report_key_changes_with_reporting_period(self) -> None:
        a = build_building_energy_report_key(SOURCE_ID, DATASET_ID, "subject-1", "2023")

        b = build_building_energy_report_key(SOURCE_ID, DATASET_ID, "subject-1", "2024")

        assert a != b

    def test_metric_key_changes_with_discriminator(self) -> None:
        report_key = _report_key()

        a = build_building_metric_value_key(SOURCE_ID, DATASET_ID, report_key, "site_eui")
        b = build_building_metric_value_key(SOURCE_ID, DATASET_ID, report_key, "source_eui")

        assert a != b

    def test_case_key_changes_with_covered_period(self) -> None:
        a = build_building_compliance_case_key(SOURCE_ID, DATASET_ID, "case-1", "2024")

        b = build_building_compliance_case_key(SOURCE_ID, DATASET_ID, "case-1", "2025")

        assert a != b

    def test_subject_key_changes_with_source_subject(self) -> None:
        a = build_building_energy_subject_key(SOURCE_ID, DATASET_ID, "subject-1")

        b = build_building_energy_subject_key(SOURCE_ID, DATASET_ID, "subject-2")

        assert a != b

    @pytest.mark.parametrize(
        "builder",
        [
            lambda: build_building_energy_subject_key(SOURCE_ID, DATASET_ID, ""),
            lambda: build_building_energy_report_key(SOURCE_ID, DATASET_ID, "", "2024"),
            lambda: build_building_energy_report_key(SOURCE_ID, DATASET_ID, "subject-1", ""),
            lambda: build_building_compliance_case_key(SOURCE_ID, DATASET_ID, "", "2024"),
            lambda: build_building_metric_value_key(
                SOURCE_ID, DATASET_ID, _report_key(), ""
            ),
        ],
    )
    def test_empty_key_parts_rejected(self, builder: Callable[[], str]) -> None:
        with pytest.raises(ValueError, match="must be non-empty"):
            builder()

    def test_whitespace_key_parts_rejected(self) -> None:
        with pytest.raises(ValueError, match="must not have surrounding whitespace"):
            build_building_energy_subject_key(SOURCE_ID, DATASET_ID, " subject-1 ")


class TestBuildingEnergySubject:
    def test_minimum_valid_subject(self) -> None:
        subject = _subject()

        assert subject.subject_key == _subject_key()
        assert subject.subject_kind.value is BuildingSubjectKind.PROPERTY
        assert subject.identity_certainty.value is IdentityCertainty.STABLE_CROSS_YEAR

    def test_invalid_subject_key_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _subject(subject_key="subject-1")

    def test_frozen(self) -> None:
        subject = _subject()

        with pytest.raises(ValidationError):
            subject.subject_key = _subject_key("subject-2")  # type: ignore[misc]

    def test_extra_fields_rejected(self) -> None:
        payload = _subject().model_dump(mode="python")
        payload["unexpected"] = True

        with pytest.raises(ValidationError):
            BuildingEnergySubject.model_validate(payload)

    def test_parent_subject_key_self_link_rejected(self) -> None:
        key = _subject_key()

        with pytest.raises(ValidationError, match="parent_subject_key"):
            _subject(
                subject_key=key,
                parent_subject_key=_mapped(key, "parentPropertyId"),
            )

    def test_campus_parent_link_round_trips(self) -> None:
        parent_key = _subject_key("campus-1")
        child_key = _subject_key("child-1")

        child = _subject(
            subject_key=child_key,
            subject_kind=_mapped(
                BuildingSubjectKind.BUILDING,
                "subjectKind",
                quality=FieldQuality.STANDARDIZED,
            ),
            parent_subject_key=_mapped(parent_key, "parentPropertyId"),
        )

        assert child.parent_subject_key.value == parent_key
        assert child.subject_kind.value is BuildingSubjectKind.BUILDING

    def test_multi_identifier_subject_preserves_each_identifier(self) -> None:
        identifiers = (
            SourceIdentifier(value="prop-1", identifier_kind=_category("property-id")),
            SourceIdentifier(value="bin-1234567", identifier_kind=_category("bin")),
            SourceIdentifier(value="bbl-1-00001-0001", identifier_kind=_category("bbl")),
        )

        subject = _subject(
            source_subject_identifiers=_mapped(identifiers, "propertyId", "bin", "bbl"),
        )

        assert subject.source_subject_identifiers.value == identifiers

    def test_negative_floor_area_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _subject(floor_area=_mapped(Decimal("-1"), "grossFloorArea"))

    def test_redacted_address_round_trips(self) -> None:
        subject = _subject(address=_mapped(None, "address", quality=FieldQuality.REDACTED))

        assert subject.address.quality is FieldQuality.REDACTED
        assert subject.address.value is None


class TestBuildingEnergyReport:
    def test_minimum_valid_report(self) -> None:
        report = _report()

        assert report.report_key == _report_key()
        assert report.subject_key == _subject_key()
        assert report.reporting_period_precision.value is ReportingPeriodPrecision.CALENDAR_YEAR

    def test_invalid_report_key_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _report(report_key="report-1")

    def test_invalid_subject_key_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _report(subject_key="subject-1")

    def test_subject_key_required(self) -> None:
        payload = _report().model_dump(mode="python")
        del payload["subject_key"]

        with pytest.raises(ValidationError):
            BuildingEnergyReport.model_validate(payload)

    def test_frozen(self) -> None:
        report = _report()

        with pytest.raises(ValidationError):
            report.report_key = _report_key("subject-2", "2025")  # type: ignore[misc]

    def test_extra_fields_rejected(self) -> None:
        payload = _report().model_dump(mode="python")
        payload["unexpected"] = "x"

        with pytest.raises(ValidationError):
            BuildingEnergyReport.model_validate(payload)

    def test_partial_year_precision_round_trips(self) -> None:
        report = _report(
            reporting_period_precision=_mapped(
                ReportingPeriodPrecision.PARTIAL_YEAR,
                "reportYear",
                quality=FieldQuality.STANDARDIZED,
            ),
        )

        assert report.reporting_period_precision.value is ReportingPeriodPrecision.PARTIAL_YEAR


class TestBuildingMetricValue:
    def test_minimum_valid_metric(self) -> None:
        metric = _metric()

        assert metric.metric_family is MetricFamily.ENERGY
        assert metric.energy_metric_type.value is EnergyMetricType.SITE_EUI
        assert isinstance(metric.measure.value, NumericMetricMeasure)

    def test_invalid_metric_key_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _metric(metric_key="metric-1")

    def test_frozen(self) -> None:
        metric = _metric()

        with pytest.raises(ValidationError):
            metric.metric_family = MetricFamily.EMISSIONS  # type: ignore[misc]

    def test_extra_fields_rejected(self) -> None:
        payload = _metric().model_dump(mode="python")
        payload["unexpected"] = True

        with pytest.raises(ValidationError):
            BuildingMetricValue.model_validate(payload)

    def test_emissions_family_with_emissions_type(self) -> None:
        metric = _metric(
            metric_family=MetricFamily.EMISSIONS,
            energy_metric_type=_unmapped(),
            emissions_metric_type=_mapped(
                EmissionsMetricType.TOTAL_GHG,
                "totalGhg",
                quality=FieldQuality.STANDARDIZED,
            ),
            measure=_mapped(NumericMetricMeasure(value=Decimal("412.5")), "totalGhg"),
            unit=_mapped(_category("metric-tons-co2e"), "totalGhg"),
        )

        assert metric.emissions_metric_type.value is EmissionsMetricType.TOTAL_GHG

    def test_water_family_with_water_type(self) -> None:
        metric = _metric(
            metric_family=MetricFamily.WATER,
            energy_metric_type=_unmapped(),
            water_metric_type=_mapped(
                WaterMetricType.WATER_USE,
                "waterUse",
                quality=FieldQuality.STANDARDIZED,
            ),
            measure=_mapped(NumericMetricMeasure(value=Decimal("12000")), "waterUse"),
            unit=_mapped(_category("kgal"), "waterUse"),
        )

        assert metric.water_metric_type.value is WaterMetricType.WATER_USE

    def test_family_slot_mismatch_rejected(self) -> None:
        with pytest.raises(ValidationError, match="metric_family"):
            _metric(
                metric_family=MetricFamily.EMISSIONS,
                energy_metric_type=_mapped(
                    EnergyMetricType.SITE_EUI,
                    "siteEui",
                    quality=FieldQuality.STANDARDIZED,
                ),
                emissions_metric_type=_unmapped(),
            )

    def test_family_missing_required_typed_slot_rejected(self) -> None:
        with pytest.raises(ValidationError, match="metric_family"):
            _metric(
                metric_family=MetricFamily.EMISSIONS,
                energy_metric_type=_unmapped(),
                emissions_metric_type=_unmapped(),
            )

    def test_source_specific_family_requires_no_typed_slot(self) -> None:
        metric = _metric(
            metric_family=MetricFamily.SOURCE_SPECIFIC,
            energy_metric_type=_unmapped(),
            emissions_metric_type=_unmapped(),
            water_metric_type=_unmapped(),
            source_metric_label=_mapped(_category("vancouver-thermal-ghgi"), "thermalGhgi"),
        )

        assert metric.metric_family is MetricFamily.SOURCE_SPECIFIC

    def test_negative_energy_value_rejected(self) -> None:
        with pytest.raises(ValidationError, match="non-negative"):
            _metric(measure=_mapped(NumericMetricMeasure(value=Decimal("-1")), "siteEui"))

    def test_negative_emissions_value_rejected_for_total_ghg(self) -> None:
        with pytest.raises(ValidationError, match="non-negative"):
            _metric(
                metric_family=MetricFamily.EMISSIONS,
                energy_metric_type=_unmapped(),
                emissions_metric_type=_mapped(
                    EmissionsMetricType.TOTAL_GHG,
                    "totalGhg",
                    quality=FieldQuality.STANDARDIZED,
                ),
                measure=_mapped(NumericMetricMeasure(value=Decimal("-5")), "totalGhg"),
            )

    @pytest.mark.parametrize(
        "negative_allowed_type",
        [
            EmissionsMetricType.AVOIDED_EMISSIONS,
            EmissionsMetricType.EMISSIONS_OVERAGE,
            EmissionsMetricType.NET_GHG,
            EmissionsMetricType.OFFSETS,
        ],
    )
    def test_negative_emissions_value_allowed_for_signed_types(
        self, negative_allowed_type: EmissionsMetricType
    ) -> None:
        metric = _metric(
            metric_family=MetricFamily.EMISSIONS,
            energy_metric_type=_unmapped(),
            emissions_metric_type=_mapped(
                negative_allowed_type,
                "signedEmissions",
                quality=FieldQuality.STANDARDIZED,
            ),
            measure=_mapped(NumericMetricMeasure(value=Decimal("-12.5")), "signedEmissions"),
        )

        assert metric.measure.value == NumericMetricMeasure(value=Decimal("-12.5"))

    def test_category_measure_round_trips(self) -> None:
        metric = _metric(
            metric_family=MetricFamily.ENERGY,
            energy_metric_type=_mapped(
                EnergyMetricType.ENERGY_STAR_SCORE,
                "energyStarScore",
                quality=FieldQuality.STANDARDIZED,
            ),
            measure=_mapped(
                CategoryMetricMeasure(value=_category("rating-a")),
                "energyStarRating",
            ),
        )

        assert isinstance(metric.measure.value, CategoryMetricMeasure)

    def test_text_measure_round_trips(self) -> None:
        metric = _metric(
            measure=_mapped(TextMetricMeasure(value="Possible Issue"), "qualityNote"),
        )

        assert isinstance(metric.measure.value, TextMetricMeasure)

    def test_invalid_measure_discriminator_rejected(self) -> None:
        payload = _metric().model_dump(mode="python")
        payload["measure"] = {
            "value": {"kind": "unsupported", "value": "x"},
            "quality": FieldQuality.DIRECT,
            "source_fields": ("siteEui",),
        }

        with pytest.raises(ValidationError):
            BuildingMetricValue.model_validate(payload)

    def test_value_state_distinct_from_field_quality(self) -> None:
        metric = _metric(
            measure=_mapped(None, "siteEui", quality=FieldQuality.NOT_PROVIDED),
            value_state=_mapped(
                SourceValueState.NOT_AVAILABLE,
                "siteEui",
                quality=FieldQuality.STANDARDIZED,
            ),
        )

        assert metric.measure.quality is FieldQuality.NOT_PROVIDED
        assert metric.value_state.value is SourceValueState.NOT_AVAILABLE

    def test_value_source_mapper_derived_preserved(self) -> None:
        metric = _metric(
            value_source=_mapped(
                MetricValueSource.MAPPER_DERIVED,
                "calculation",
                quality=FieldQuality.STANDARDIZED,
            ),
            emission_factor_version=_mapped("nrcan-2023-08", "factorVersion"),
        )

        assert metric.value_source.value is MetricValueSource.MAPPER_DERIVED
        assert metric.emission_factor_version.value == "nrcan-2023-08"

    def test_metric_attached_to_case_only(self) -> None:
        case_key = _case_key()
        metric = _metric(
            metric_key=build_building_metric_value_key(
                SOURCE_ID, DATASET_ID, case_key, "emissions_limit"
            ),
            report_key=_unmapped(),
            case_key=_mapped(case_key, "caseId"),
            metric_family=MetricFamily.EMISSIONS,
            energy_metric_type=_unmapped(),
            emissions_metric_type=_mapped(
                EmissionsMetricType.EMISSIONS_LIMIT,
                "emissionsLimit",
                quality=FieldQuality.STANDARDIZED,
            ),
            measure=_mapped(NumericMetricMeasure(value=Decimal("250")), "emissionsLimit"),
            unit=_mapped(_category("metric-tons-co2e"), "emissionsLimit"),
        )

        assert metric.report_key.value is None
        assert metric.case_key.value == case_key

    def test_metric_with_both_parents_rejected(self) -> None:
        with pytest.raises(ValidationError, match="not both"):
            _metric(case_key=_mapped(_case_key(), "caseId"))

    def test_metric_with_no_parent_rejected(self) -> None:
        with pytest.raises(ValidationError, match="either a report or a case"):
            _metric(report_key=_unmapped(), case_key=_unmapped())

    @pytest.mark.parametrize(
        "state",
        [SourceValueState.SUPPRESSED, SourceValueState.PARTIALLY_DISCLOSED],
    )
    def test_disclosure_states_distinct_from_missing(self, state: SourceValueState) -> None:
        metric = _metric(
            measure=_mapped(None, "siteEui", quality=FieldQuality.NOT_PROVIDED),
            value_state=_mapped(state, "siteEui", quality=FieldQuality.STANDARDIZED),
        )

        assert metric.value_state.value is state
        assert metric.value_state.value is not SourceValueState.NOT_AVAILABLE
        assert metric.value_state.value is not SourceValueState.WITHHELD


class TestBuildingComplianceCase:
    def test_minimum_valid_case(self) -> None:
        case = _case()

        assert case.case_key == _case_key()
        assert case.compliance_status.value is ComplianceLifecycleStatus.COMPLIANT
        assert case.subject_key == _subject_key()

    def test_invalid_case_key_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _case(case_key="case-1")

    def test_frozen(self) -> None:
        case = _case()

        with pytest.raises(ValidationError):
            case.case_key = _case_key("case-2", "2024")  # type: ignore[misc]

    def test_extra_fields_rejected(self) -> None:
        payload = _case().model_dump(mode="python")
        payload["unexpected"] = "x"

        with pytest.raises(ValidationError):
            BuildingComplianceCase.model_validate(payload)

    def test_negative_penalty_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _case(penalty_amount=_mapped(Decimal("-100"), "penaltyAmount"))

    def test_emissions_limit_referenced_as_metric_key(self) -> None:
        limit_key = build_building_metric_value_key(
            SOURCE_ID, DATASET_ID, _report_key(), "emissions_limit"
        )

        case = _case(emissions_limit_metric_key=_mapped(limit_key, "emissionsLimit"))

        assert case.emissions_limit_metric_key.value == limit_key

    def test_multiple_cases_can_reference_one_report(self) -> None:
        report = _report()
        first = _case(case_key=_case_key("case-a"))
        second = _case(case_key=_case_key("case-b"))

        assert first.related_report_key.value == report.report_key
        assert second.related_report_key.value == report.report_key
        assert first.case_key != second.case_key

    def test_invalid_lifecycle_status_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _case(
                compliance_status=_mapped(
                    "compliant-ish",
                    "complianceStatus",
                    quality=FieldQuality.STANDARDIZED,
                )
            )


class TestPressureFixtures:
    """Cross-source design fixtures: confirm models can carry NYC reporting,
    NYC compliance, and an Ontario/Toronto comparator without leaking source
    field names into shared domain objects."""

    def test_annual_property_year_report_with_typed_metrics(self) -> None:
        subject = _subject()
        report = _report()
        metrics = (
            _metric(
                metric_key=build_building_metric_value_key(
                    SOURCE_ID, DATASET_ID, report.report_key, "site_eui"
                ),
            ),
            _metric(
                metric_key=build_building_metric_value_key(
                    SOURCE_ID, DATASET_ID, report.report_key, "source_eui"
                ),
                energy_metric_type=_mapped(
                    EnergyMetricType.SOURCE_EUI,
                    "sourceEui",
                    quality=FieldQuality.STANDARDIZED,
                ),
                source_metric_label=_mapped(_category("source-eui-kbtu-ft2"), "sourceEui"),
                measure=_mapped(NumericMetricMeasure(value=Decimal("162.0")), "sourceEui"),
                unit=_mapped(_category("kbtu-per-ft2"), "sourceEui"),
            ),
            _metric(
                metric_key=build_building_metric_value_key(
                    SOURCE_ID, DATASET_ID, report.report_key, "total_ghg"
                ),
                metric_family=MetricFamily.EMISSIONS,
                energy_metric_type=_unmapped(),
                emissions_metric_type=_mapped(
                    EmissionsMetricType.TOTAL_GHG,
                    "totalLocationBasedGhg",
                    quality=FieldQuality.STANDARDIZED,
                ),
                source_metric_label=_mapped(_category("total-location-based-ghg"), "ghg"),
                measure=_mapped(NumericMetricMeasure(value=Decimal("412.5")), "totalGhg"),
                unit=_mapped(_category("metric-tons-co2e"), "totalGhg"),
            ),
            _metric(
                metric_key=build_building_metric_value_key(
                    SOURCE_ID, DATASET_ID, report.report_key, "energy_star_score"
                ),
                energy_metric_type=_mapped(
                    EnergyMetricType.ENERGY_STAR_SCORE,
                    "energyStarScore",
                    quality=FieldQuality.STANDARDIZED,
                ),
                source_metric_label=_mapped(_category("energy-star-score"), "energyStarScore"),
                measure=_mapped(NumericMetricMeasure(value=Decimal("78")), "energyStarScore"),
                unit=_mapped(_category("score-points"), "energyStarScore"),
            ),
        )

        assert subject.subject_key == report.subject_key
        for metric in metrics:
            assert metric.report_key.value == report.report_key
            assert metric.case_key.value is None
            assert metric.subject_key == subject.subject_key
        assert len({metric.metric_key for metric in metrics}) == len(metrics)

    def test_campus_parent_child_with_multi_bin_identifiers(self) -> None:
        parent = _subject(
            subject_key=_subject_key("campus-1"),
            subject_kind=_mapped(
                BuildingSubjectKind.CAMPUS,
                "subjectKind",
                quality=FieldQuality.STANDARDIZED,
            ),
        )
        child = _subject(
            subject_key=_subject_key("child-1"),
            subject_kind=_mapped(
                BuildingSubjectKind.BUILDING,
                "subjectKind",
                quality=FieldQuality.STANDARDIZED,
            ),
            source_subject_identifiers=_mapped(
                (
                    SourceIdentifier(
                        value="bin-1234567", identifier_kind=_category("bin")
                    ),
                    SourceIdentifier(
                        value="bin-1234568", identifier_kind=_category("bin")
                    ),
                    SourceIdentifier(
                        value="bbl-1-00001-0001", identifier_kind=_category("bbl")
                    ),
                ),
                "bin",
                "bbl",
            ),
            parent_subject_key=_mapped(parent.subject_key, "parentPropertyId"),
        )

        assert child.parent_subject_key.value == parent.subject_key
        assert child.subject_kind.value is BuildingSubjectKind.BUILDING
        assert parent.subject_kind.value is BuildingSubjectKind.CAMPUS

    def test_textual_sentinel_value_state_alongside_quality_caveat(self) -> None:
        suppressed_metric = _metric(
            measure=_mapped(None, "siteEui", quality=FieldQuality.NOT_PROVIDED),
            value_state=_mapped(
                SourceValueState.UNABLE_TO_CHECK,
                "siteEui",
                quality=FieldQuality.STANDARDIZED,
            ),
            source_caveats=_mapped(
                (_category("data-quality-flag-electricity"),),
                "dataQualityChecker",
            ),
        )

        assert suppressed_metric.value_state.value is SourceValueState.UNABLE_TO_CHECK
        assert suppressed_metric.measure.quality is FieldQuality.NOT_PROVIDED

    def test_compliance_case_links_limit_final_excess_metrics(self) -> None:
        report = _report()
        case_key = _case_key()
        # LL84-style published metric: attaches to the report.
        final_key = build_building_metric_value_key(
            SOURCE_ID, DATASET_ID, report.report_key, "final_emissions"
        )
        # LL97-only compliance metrics: attach to the case directly,
        # because no LL84-shaped report row carries them.
        limit_key = build_building_metric_value_key(
            SOURCE_ID, DATASET_ID, case_key, "emissions_limit"
        )
        excess_key = build_building_metric_value_key(
            SOURCE_ID, DATASET_ID, case_key, "excess_emissions"
        )

        case = _case(
            case_key=case_key,
            emissions_limit_metric_key=_mapped(limit_key, "emissionsLimit"),
            final_emissions_metric_key=_mapped(final_key, "finalEmissions"),
            excess_emissions_metric_key=_mapped(excess_key, "excessEmissions"),
            compliance_status=_mapped(
                ComplianceLifecycleStatus.NON_COMPLIANT,
                "complianceStatus",
                quality=FieldQuality.STANDARDIZED,
            ),
        )
        limit_metric = _metric(
            metric_key=limit_key,
            report_key=_unmapped(),
            case_key=_mapped(case_key, "caseId"),
            metric_family=MetricFamily.EMISSIONS,
            energy_metric_type=_unmapped(),
            emissions_metric_type=_mapped(
                EmissionsMetricType.EMISSIONS_LIMIT,
                "emissionsLimit",
                quality=FieldQuality.STANDARDIZED,
            ),
            measure=_mapped(NumericMetricMeasure(value=Decimal("300.0")), "emissionsLimit"),
            unit=_mapped(_category("metric-tons-co2e"), "emissionsLimit"),
        )
        excess_metric = _metric(
            metric_key=excess_key,
            report_key=_unmapped(),
            case_key=_mapped(case_key, "caseId"),
            metric_family=MetricFamily.EMISSIONS,
            energy_metric_type=_unmapped(),
            emissions_metric_type=_mapped(
                EmissionsMetricType.EMISSIONS_OVERAGE,
                "excessEmissions",
                quality=FieldQuality.STANDARDIZED,
            ),
            measure=_mapped(NumericMetricMeasure(value=Decimal("142.0")), "excessEmissions"),
            unit=_mapped(_category("metric-tons-co2e"), "excessEmissions"),
        )

        assert case.emissions_limit_metric_key.value == limit_metric.metric_key
        assert case.final_emissions_metric_key.value == final_key
        assert case.excess_emissions_metric_key.value == excess_metric.metric_key
        assert limit_metric.case_key.value == case.case_key
        assert excess_metric.case_key.value == case.case_key
        assert limit_metric.report_key.value is None
        assert case.compliance_status.value is ComplianceLifecycleStatus.NON_COMPLIANT

    def test_canadian_units_and_partial_geography(self) -> None:
        ca_subject = _subject(
            jurisdiction=_mapped(Jurisdiction(country="CA", region="ON"), "province"),
            address=_mapped(
                Address(country="CA", region="ON", postal_code="M5H"),
                "partialPostalCode",
            ),
            coordinate=_unmapped(),
            floor_area=_unmapped(),
            floor_area_unit=_unmapped(),
        )
        ca_metric = _metric(
            measure=_mapped(NumericMetricMeasure(value=Decimal("0.412")), "ghgIntensity"),
            metric_family=MetricFamily.EMISSIONS,
            energy_metric_type=_unmapped(),
            emissions_metric_type=_mapped(
                EmissionsMetricType.EMISSIONS_INTENSITY,
                "ghgIntensity",
                quality=FieldQuality.STANDARDIZED,
            ),
            unit=_mapped(_category("kgco2e-per-m2-per-year"), "ghgIntensity"),
            denominator=_mapped(_category("m2"), "ghgIntensity"),
            emission_factor_version=_mapped("nrcan-2023-08", "factorVersion"),
        )

        assert ca_subject.address.value is not None
        assert ca_subject.address.value.postal_code == "M5H"
        assert ca_subject.coordinate.quality is FieldQuality.UNMAPPED
        assert ca_metric.unit.value == _category("kgco2e-per-m2-per-year")
        assert ca_metric.emission_factor_version.value == "nrcan-2023-08"

    def test_reassigned_identity_subject_round_trips(self) -> None:
        subject = _subject(
            identity_certainty=_mapped(
                IdentityCertainty.REASSIGNED,
                "identityNote",
                quality=FieldQuality.STANDARDIZED,
            ),
        )

        assert subject.identity_certainty.value is IdentityCertainty.REASSIGNED
