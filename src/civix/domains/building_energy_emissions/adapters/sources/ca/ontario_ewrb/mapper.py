"""Ontario EWRB annual reporting mappers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Final, cast

from pydantic import BaseModel

from civix.core.identity.models.identifiers import DatasetId, MapperId, SourceId
from civix.core.mapping.errors import MappingError
from civix.core.mapping.models.mapper import MappingReport, MapResult
from civix.core.mapping.parsers import require_text, slugify, str_or_none
from civix.core.provenance.models.provenance import MapperVersion, ProvenanceRef
from civix.core.quality.models.fields import FieldQuality, MappedField
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.spatial.models.location import Address
from civix.core.taxonomy.models.category import CategoryRef
from civix.core.temporal import (
    TemporalPeriod,
    TemporalPeriodPrecision,
    TemporalTimezoneStatus,
)
from civix.domains.building_energy_emissions.adapters.sources.ca.ontario_ewrb.caveats import (
    EWRB_CAVEAT_SOURCE_FIELD,
    ontario_ewrb_caveat_categories,
)
from civix.domains.building_energy_emissions.adapters.sources.ca.ontario_ewrb.schema import (
    EWRB_REPORTING_YEAR_FIELD,
    EWRB_TAXONOMY_VERSION,
)
from civix.domains.building_energy_emissions.models import (
    BuildingEnergyReport,
    BuildingEnergySubject,
    BuildingMetricValue,
    BuildingSubjectKind,
    EmissionsMetricType,
    EnergyMetricType,
    IdentityCertainty,
    MetricFamily,
    MetricValueSource,
    NumericMetricMeasure,
    ReportingPeriodPrecision,
    SourceIdentifier,
    SourceValueState,
    WaterMetricType,
    build_building_energy_report_key,
    build_building_energy_subject_key,
    build_building_metric_value_key,
)
from civix.domains.building_energy_emissions.models.metric import MetricMeasure

SUBJECT_MAPPER_ID: Final[MapperId] = MapperId("ontario-ewrb-subject")
REPORT_MAPPER_ID: Final[MapperId] = MapperId("ontario-ewrb-report")
METRICS_MAPPER_ID: Final[MapperId] = MapperId("ontario-ewrb-metrics")
SUBJECT_MAPPER_VERSION: Final[str] = "0.1.0"
REPORT_MAPPER_VERSION: Final[str] = "0.1.0"
METRICS_MAPPER_VERSION: Final[str] = "0.1.0"

METHODOLOGY_LABEL: Final[str] = "ENERGY STAR Portfolio Manager"
METHODOLOGY_URL: Final[str] = "https://www.ontario.ca/page/report-energy-water-use-large-buildings"

# Methodology version tokens applied to source-side metrics depending on the
# row-level NRCan source-factor flag. NRCan changed source factors as of
# 2023-08-28; submissions calculated under either factor must remain
# distinguishable per metric.
METHODOLOGY_VERSION_POST_AUG_2023: Final[str] = "nrcan-source-factor-post-2023-08-28"
METHODOLOGY_VERSION_PRE_AUG_2023: Final[str] = "nrcan-source-factor-pre-2023-08-28"

_IDENTIFIER_TAXONOMY_ID: Final[str] = "ontario-ewrb-source-identifier-kind"
_PROPERTY_TYPE_TAXONOMY_ID: Final[str] = "ontario-ewrb-property-use-type"
_CERTIFICATION_TAXONOMY_ID: Final[str] = "ontario-ewrb-third-party-certification"
_METRIC_UNIT_TAXONOMY_ID: Final[str] = "ontario-ewrb-metric-unit"
_METRIC_LABEL_TAXONOMY_ID: Final[str] = "ontario-ewrb-metric-label"
_DATA_QUALITY_TAXONOMY_ID: Final[str] = "ontario-ewrb-data-quality-checker-result"
_SUBJECT_KIND_TAXONOMY_ID: Final[str] = "ontario-ewrb-subject-kind"

_ADDRESS_FIELDS: Final[tuple[str, ...]] = ("city", "postal_code")
_PROPERTY_TYPE_SOURCE_FIELDS: Final[tuple[str, ...]] = (
    "primary_property_type_calculated",
    "primary_property_type_self",
    "largest_property_use_type",
    "all_property_use_types",
)


# (source_field, family, typed_metric, unit_code, unit_label, label,
#  uses_source_factor)
_MetricSpec = tuple[
    str,
    MetricFamily,
    EnergyMetricType | EmissionsMetricType | WaterMetricType,
    str,
    str,
    str,
    bool,
]


_DISCLOSED_METRIC_SPECS: Final[tuple[_MetricSpec, ...]] = (
    (
        "weather_normalized_site_electricity_intensity_gj_per_m2",
        MetricFamily.ENERGY,
        EnergyMetricType.ELECTRICITY_USE,
        "gj-per-m2",
        "GJ/m2",
        "Weather Normalized Electricity Intensity",
        False,
    ),
    (
        "weather_normalized_site_electricity_intensity_kwh_per_ft2",
        MetricFamily.ENERGY,
        EnergyMetricType.ELECTRICITY_USE,
        "kwh-per-ft2",
        "kWh/ft2",
        "Weather Normalized Electricity Intensity",
        False,
    ),
    (
        "weather_normalized_site_natural_gas_intensity_gj_per_m2",
        MetricFamily.ENERGY,
        EnergyMetricType.NATURAL_GAS_USE,
        "gj-per-m2",
        "GJ/m2",
        "Weather Normalized Natural Gas Intensity",
        False,
    ),
    (
        "weather_normalized_site_natural_gas_intensity_m3_per_m2",
        MetricFamily.ENERGY,
        EnergyMetricType.NATURAL_GAS_USE,
        "m3-per-m2",
        "m3/m2",
        "Weather Normalized Natural Gas Intensity",
        False,
    ),
    (
        "weather_normalized_site_natural_gas_intensity_m3_per_ft2",
        MetricFamily.ENERGY,
        EnergyMetricType.NATURAL_GAS_USE,
        "m3-per-ft2",
        "m3/ft2",
        "Weather Normalized Natural Gas Intensity",
        False,
    ),
    (
        "site_eui_gj_per_m2",
        MetricFamily.ENERGY,
        EnergyMetricType.SITE_EUI,
        "gj-per-m2",
        "GJ/m2",
        "Site EUI",
        False,
    ),
    (
        "site_eui_ekwh_per_ft2",
        MetricFamily.ENERGY,
        EnergyMetricType.SITE_EUI,
        "ekwh-per-ft2",
        "ekWh/ft2",
        "Site EUI",
        False,
    ),
    (
        "source_eui_gj_per_m2",
        MetricFamily.ENERGY,
        EnergyMetricType.SOURCE_EUI,
        "gj-per-m2",
        "GJ/m2",
        "Source EUI",
        True,
    ),
    (
        "source_eui_ekwh_per_ft2",
        MetricFamily.ENERGY,
        EnergyMetricType.SOURCE_EUI,
        "ekwh-per-ft2",
        "ekWh/ft2",
        "Source EUI",
        True,
    ),
    (
        "weather_normalized_site_eui_gj_per_m2",
        MetricFamily.ENERGY,
        EnergyMetricType.WEATHER_NORMALIZED_SITE_EUI,
        "gj-per-m2",
        "GJ/m2",
        "Weather Normalized Site EUI",
        False,
    ),
    (
        "weather_normalized_site_eui_ekwh_per_ft2",
        MetricFamily.ENERGY,
        EnergyMetricType.WEATHER_NORMALIZED_SITE_EUI,
        "ekwh-per-ft2",
        "ekWh/ft2",
        "Weather Normalized Site EUI",
        False,
    ),
    (
        "weather_normalized_source_eui_gj_per_m2",
        MetricFamily.ENERGY,
        EnergyMetricType.WEATHER_NORMALIZED_SOURCE_EUI,
        "gj-per-m2",
        "GJ/m2",
        "Weather Normalized Source EUI",
        True,
    ),
    (
        "weather_normalized_source_eui_ekwh_per_ft2",
        MetricFamily.ENERGY,
        EnergyMetricType.WEATHER_NORMALIZED_SOURCE_EUI,
        "ekwh-per-ft2",
        "ekWh/ft2",
        "Weather Normalized Source EUI",
        True,
    ),
    (
        "all_water_intensity_m3_per_m2",
        MetricFamily.WATER,
        WaterMetricType.WATER_USE_INTENSITY,
        "m3-per-m2",
        "m3/m2",
        "All Water Intensity",
        False,
    ),
    (
        "all_water_intensity_m3_per_ft2",
        MetricFamily.WATER,
        WaterMetricType.WATER_USE_INTENSITY,
        "m3-per-ft2",
        "m3/ft2",
        "All Water Intensity",
        False,
    ),
    (
        "indoor_water_intensity_m3_per_m2",
        MetricFamily.WATER,
        WaterMetricType.WATER_USE_INTENSITY,
        "m3-per-m2",
        "m3/m2",
        "Indoor Water Intensity",
        False,
    ),
    (
        "indoor_water_intensity_m3_per_ft2",
        MetricFamily.WATER,
        WaterMetricType.WATER_USE_INTENSITY,
        "m3-per-ft2",
        "m3/ft2",
        "Indoor Water Intensity",
        False,
    ),
    (
        "ghg_emissions_intensity_kgco2e_per_m2",
        MetricFamily.EMISSIONS,
        EmissionsMetricType.EMISSIONS_INTENSITY,
        "kgco2e-per-m2",
        "kgCO2e/m2",
        "GHG Emissions Intensity",
        False,
    ),
    (
        "ghg_emissions_intensity_kgco2e_per_ft2",
        MetricFamily.EMISSIONS,
        EmissionsMetricType.EMISSIONS_INTENSITY,
        "kgco2e-per-ft2",
        "kgCO2e/ft2",
        "GHG Emissions Intensity",
        False,
    ),
    (
        "energy_star_score",
        MetricFamily.ENERGY,
        EnergyMetricType.ENERGY_STAR_SCORE,
        "score",
        "Score Points",
        "ENERGY STAR Score",
        False,
    ),
)


# Ontario systematically withholds total energy, total water, total GHG, and
# gross floor area; only intensities are disclosed. Synthesized metric
# records preserve that distinction end-to-end so consumers do not confuse
# "the source did not publish a total" with "the value is null/missing".
_WithheldSpec = tuple[
    str,  # synthetic discriminator
    MetricFamily,
    EnergyMetricType | EmissionsMetricType | WaterMetricType,
    str,
]
_WITHHELD_TOTAL_SPECS: Final[tuple[_WithheldSpec, ...]] = (
    (
        "total-site-energy-withheld",
        MetricFamily.ENERGY,
        EnergyMetricType.SITE_ENERGY,
        "Total Site Energy",
    ),
    (
        "total-natural-gas-use-withheld",
        MetricFamily.ENERGY,
        EnergyMetricType.NATURAL_GAS_USE,
        "Total Natural Gas Use",
    ),
    (
        "total-ghg-withheld",
        MetricFamily.EMISSIONS,
        EmissionsMetricType.TOTAL_GHG,
        "Total GHG Emissions",
    ),
    (
        "total-water-use-withheld",
        MetricFamily.WATER,
        WaterMetricType.WATER_USE,
        "Total Water Use",
    ),
)


@dataclass(frozen=True, slots=True)
class OntarioEwrbSubjectMapper:
    """Maps one Ontario EWRB row to a `BuildingEnergySubject`."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=SUBJECT_MAPPER_ID, version=SUBJECT_MAPPER_VERSION)

    def __call__(
        self, record: RawRecord, snapshot: SourceSnapshot
    ) -> MapResult[BuildingEnergySubject]:
        raw = record.raw_data
        ewrb_id = _required_text(raw, "ewrb_id", self.version, record)

        subject = BuildingEnergySubject(
            provenance=_build_provenance(record=record, snapshot=snapshot, mapper=self.version),
            subject_key=build_building_energy_subject_key(
                SourceId(str(snapshot.source_id)),
                DatasetId(str(snapshot.dataset_id)),
                ewrb_id,
            ),
            source_subject_identifiers=_map_source_identifiers(ewrb_id),
            subject_kind=MappedField(
                value=BuildingSubjectKind.REPORTING_ACCOUNT,
                quality=FieldQuality.STANDARDIZED,
                source_fields=("ewrb_id",),
            ),
            source_subject_kind=MappedField(
                value=_category(
                    "ontario-ewrb-id",
                    label="Ontario EWRB Reporting ID",
                    taxonomy_id=_SUBJECT_KIND_TAXONOMY_ID,
                ),
                quality=FieldQuality.STANDARDIZED,
                source_fields=("ewrb_id",),
            ),
            # The Ontario data dictionary says EWRB_ID is provincially
            # assigned and "should not be interpreted as exact physical
            # configuration when building count/configuration is unknown".
            # Subject identity is therefore ambiguous across reporting years
            # even when the same EWRB_ID appears.
            identity_certainty=MappedField(
                value=IdentityCertainty.AMBIGUOUS,
                quality=FieldQuality.STANDARDIZED,
                source_fields=("ewrb_id",),
            ),
            parent_subject_key=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            name=MappedField(
                value=None,
                quality=FieldQuality.NOT_PROVIDED,
                source_fields=("ewrb_id",),
            ),
            jurisdiction=MappedField(
                value=snapshot.jurisdiction,
                quality=FieldQuality.STANDARDIZED,
                source_fields=("city",),
            ),
            address=_map_address(raw, snapshot),
            coordinate=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            property_types=_map_property_types(raw),
            # Ontario withholds gross floor area; REDACTED records that
            # explicit suppression rather than collapsing it into a plain
            # missing value.
            floor_area=MappedField(
                value=None,
                quality=FieldQuality.REDACTED,
                source_fields=(EWRB_CAVEAT_SOURCE_FIELD,),
            ),
            floor_area_unit=MappedField(
                value=None,
                quality=FieldQuality.REDACTED,
                source_fields=(EWRB_CAVEAT_SOURCE_FIELD,),
            ),
            year_built=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            occupancy_label=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            ownership_label=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            source_caveats=_map_source_caveats(),
        )

        return MapResult(record=subject, report=_mapping_report(raw, subject))


@dataclass(frozen=True, slots=True)
class OntarioEwrbReportMapper:
    """Maps one Ontario EWRB row to a `BuildingEnergyReport`."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=REPORT_MAPPER_ID, version=REPORT_MAPPER_VERSION)

    def __call__(
        self, record: RawRecord, snapshot: SourceSnapshot
    ) -> MapResult[BuildingEnergyReport]:
        raw = record.raw_data
        ewrb_id = _required_text(raw, "ewrb_id", self.version, record)
        reporting_year = _require_year(raw, self.version, record)
        source_id = SourceId(str(snapshot.source_id))
        dataset_id = DatasetId(str(snapshot.dataset_id))
        subject_key = build_building_energy_subject_key(source_id, dataset_id, ewrb_id)
        report_key = build_building_energy_report_key(
            source_id, dataset_id, ewrb_id, str(reporting_year)
        )

        report = BuildingEnergyReport(
            provenance=_build_provenance(record=record, snapshot=snapshot, mapper=self.version),
            report_key=report_key,
            subject_key=subject_key,
            source_report_identifiers=MappedField(
                value=(
                    SourceIdentifier(
                        value=ewrb_id,
                        identifier_kind=_category(
                            "ontario-ewrb-id",
                            label="Ontario EWRB Reporting ID",
                            taxonomy_id=_IDENTIFIER_TAXONOMY_ID,
                        ),
                    ),
                    SourceIdentifier(
                        value=str(reporting_year),
                        identifier_kind=_category(
                            "reporting-year",
                            label="Reporting Year",
                            taxonomy_id=_IDENTIFIER_TAXONOMY_ID,
                        ),
                    ),
                ),
                quality=FieldQuality.DIRECT,
                source_fields=("ewrb_id", EWRB_REPORTING_YEAR_FIELD),
            ),
            reporting_period=MappedField(
                value=TemporalPeriod(
                    precision=TemporalPeriodPrecision.YEAR,
                    year_value=reporting_year,
                    timezone_status=TemporalTimezoneStatus.UNKNOWN,
                ),
                quality=FieldQuality.STANDARDIZED,
                source_fields=(EWRB_REPORTING_YEAR_FIELD,),
            ),
            reporting_period_precision=MappedField(
                value=ReportingPeriodPrecision.CALENDAR_YEAR,
                quality=FieldQuality.STANDARDIZED,
                source_fields=(EWRB_REPORTING_YEAR_FIELD,),
            ),
            report_submission_date=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            report_generation_date=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            report_status=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            data_quality_caveats=_map_data_quality_caveats(raw),
            source_caveats=_map_source_caveats(),
        )

        return MapResult(record=report, report=_mapping_report(raw, report))


@dataclass(frozen=True, slots=True)
class OntarioEwrbMetricsMapper:
    """Maps one Ontario EWRB row to its `BuildingMetricValue` rows.

    Emits one metric per disclosed intensity/score field plus one synthetic
    `WITHHELD` metric per Ontario-suppressed total (site energy, natural gas,
    GHG, water) so downstream consumers can distinguish "source withheld
    this aggregate" from "source did not publish".
    """

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=METRICS_MAPPER_ID, version=METRICS_MAPPER_VERSION)

    def __call__(
        self, record: RawRecord, snapshot: SourceSnapshot
    ) -> MapResult[tuple[BuildingMetricValue, ...]]:
        raw = record.raw_data
        ewrb_id = _required_text(raw, "ewrb_id", self.version, record)
        reporting_year = _require_year(raw, self.version, record)
        source_id = SourceId(str(snapshot.source_id))
        dataset_id = DatasetId(str(snapshot.dataset_id))
        subject_key = build_building_energy_subject_key(source_id, dataset_id, ewrb_id)
        report_key = build_building_energy_report_key(
            source_id, dataset_id, ewrb_id, str(reporting_year)
        )
        effective_period = TemporalPeriod(
            precision=TemporalPeriodPrecision.YEAR,
            year_value=reporting_year,
            timezone_status=TemporalTimezoneStatus.UNKNOWN,
        )
        source_factor_flag = str_or_none(raw.get("post_aug_2023_source_factor"))
        source_factor_version = _source_factor_version(source_factor_flag)

        disclosed = tuple(
            self._map_disclosed(
                raw=raw,
                record=record,
                snapshot=snapshot,
                spec=spec,
                subject_key=subject_key,
                report_key=report_key,
                effective_period=effective_period,
                source_factor_version=source_factor_version,
                source_factor_flag=source_factor_flag,
            )
            for spec in _DISCLOSED_METRIC_SPECS
        )
        withheld = tuple(
            self._map_withheld(
                snapshot=snapshot,
                record=record,
                spec=spec,
                subject_key=subject_key,
                report_key=report_key,
                effective_period=effective_period,
            )
            for spec in _WITHHELD_TOTAL_SPECS
        )
        metrics = disclosed + withheld

        return MapResult(record=metrics, report=_mapping_report(raw, metrics))

    def _map_disclosed(
        self,
        *,
        raw: Mapping[str, Any],
        record: RawRecord,
        snapshot: SourceSnapshot,
        spec: _MetricSpec,
        subject_key: str,
        report_key: str,
        effective_period: TemporalPeriod,
        source_factor_version: str | None,
        source_factor_flag: str | None,
    ) -> BuildingMetricValue:
        field_name, family, metric_type, unit_code, unit_label, label, uses_factor = spec
        source_value = raw.get(field_name)
        measure_value, value_state = _parse_metric_value(
            source_value, field_name, self.version, record
        )

        metric_key = build_building_metric_value_key(
            SourceId(str(snapshot.source_id)),
            DatasetId(str(snapshot.dataset_id)),
            report_key,
            field_name,
        )

        methodology_version = _methodology_version_field(
            uses_factor=uses_factor,
            version=source_factor_version,
            flag_present=source_factor_flag is not None,
            field_name=field_name,
        )

        return BuildingMetricValue(
            provenance=_build_provenance(record=record, snapshot=snapshot, mapper=self.version),
            metric_key=metric_key,
            report_key=MappedField(
                value=report_key,
                quality=FieldQuality.STANDARDIZED,
                source_fields=("ewrb_id", EWRB_REPORTING_YEAR_FIELD),
            ),
            case_key=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            subject_key=subject_key,
            metric_family=family,
            energy_metric_type=_typed_slot(family, metric_type, MetricFamily.ENERGY, field_name),
            emissions_metric_type=_typed_slot(
                family, metric_type, MetricFamily.EMISSIONS, field_name
            ),
            water_metric_type=_typed_slot(family, metric_type, MetricFamily.WATER, field_name),
            source_metric_label=MappedField(
                value=_category(
                    slugify(f"{label}-{unit_code}"),
                    label=f"{label} ({unit_label})",
                    taxonomy_id=_METRIC_LABEL_TAXONOMY_ID,
                ),
                quality=FieldQuality.STANDARDIZED,
                source_fields=(field_name,),
            ),
            measure=_measure_field(measure_value, field_name),
            value_state=MappedField(
                value=value_state,
                quality=FieldQuality.STANDARDIZED,
                source_fields=(field_name,),
            ),
            unit=MappedField(
                value=_category(unit_code, label=unit_label, taxonomy_id=_METRIC_UNIT_TAXONOMY_ID),
                quality=FieldQuality.STANDARDIZED,
                source_fields=(field_name,),
            ),
            denominator=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            normalization=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            fuel_or_scope=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            methodology_label=MappedField(
                value=METHODOLOGY_LABEL,
                quality=FieldQuality.STANDARDIZED,
                source_fields=(field_name,),
            ),
            methodology_version=methodology_version,
            methodology_url=MappedField(
                value=METHODOLOGY_URL,
                quality=FieldQuality.STANDARDIZED,
                source_fields=(field_name,),
            ),
            emission_factor_version=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            value_source=MappedField(
                value=MetricValueSource.SOURCE_PUBLISHED,
                quality=FieldQuality.STANDARDIZED,
                source_fields=(field_name,),
            ),
            effective_period=MappedField(
                value=effective_period,
                quality=FieldQuality.STANDARDIZED,
                source_fields=(EWRB_REPORTING_YEAR_FIELD,),
            ),
            source_caveats=_map_source_caveats(),
        )

    def _map_withheld(
        self,
        *,
        snapshot: SourceSnapshot,
        record: RawRecord,
        spec: _WithheldSpec,
        subject_key: str,
        report_key: str,
        effective_period: TemporalPeriod,
    ) -> BuildingMetricValue:
        discriminator, family, metric_type, label = spec
        metric_key = build_building_metric_value_key(
            SourceId(str(snapshot.source_id)),
            DatasetId(str(snapshot.dataset_id)),
            report_key,
            discriminator,
        )
        anchor = (EWRB_CAVEAT_SOURCE_FIELD,)

        return BuildingMetricValue(
            provenance=_build_provenance(record=record, snapshot=snapshot, mapper=self.version),
            metric_key=metric_key,
            report_key=MappedField(
                value=report_key,
                quality=FieldQuality.STANDARDIZED,
                source_fields=("ewrb_id", EWRB_REPORTING_YEAR_FIELD),
            ),
            case_key=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            subject_key=subject_key,
            metric_family=family,
            energy_metric_type=_typed_slot(family, metric_type, MetricFamily.ENERGY, anchor[0]),
            emissions_metric_type=_typed_slot(
                family, metric_type, MetricFamily.EMISSIONS, anchor[0]
            ),
            water_metric_type=_typed_slot(family, metric_type, MetricFamily.WATER, anchor[0]),
            source_metric_label=MappedField(
                value=_category(
                    discriminator,
                    label=label,
                    taxonomy_id=_METRIC_LABEL_TAXONOMY_ID,
                ),
                quality=FieldQuality.STANDARDIZED,
                source_fields=anchor,
            ),
            measure=MappedField(
                value=None,
                quality=FieldQuality.REDACTED,
                source_fields=anchor,
            ),
            value_state=MappedField(
                value=SourceValueState.WITHHELD,
                quality=FieldQuality.STANDARDIZED,
                source_fields=anchor,
            ),
            unit=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            denominator=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            normalization=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            fuel_or_scope=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            methodology_label=MappedField(
                value=METHODOLOGY_LABEL,
                quality=FieldQuality.STANDARDIZED,
                source_fields=anchor,
            ),
            methodology_version=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            methodology_url=MappedField(
                value=METHODOLOGY_URL,
                quality=FieldQuality.STANDARDIZED,
                source_fields=anchor,
            ),
            emission_factor_version=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            value_source=MappedField(
                value=MetricValueSource.SOURCE_PUBLISHED,
                quality=FieldQuality.STANDARDIZED,
                source_fields=anchor,
            ),
            effective_period=MappedField(
                value=effective_period,
                quality=FieldQuality.STANDARDIZED,
                source_fields=(EWRB_REPORTING_YEAR_FIELD,),
            ),
            source_caveats=_map_source_caveats(),
        )


def _parse_metric_value(
    value: object,
    field_name: str,
    mapper: MapperVersion,
    record: RawRecord,
) -> tuple[Decimal | None, SourceValueState]:
    text = str_or_none(value)
    if text is None:
        return None, SourceValueState.NOT_AVAILABLE

    lowered = text.lower()
    if lowered == "not available":
        return None, SourceValueState.NOT_AVAILABLE
    if lowered.startswith("not applicable"):
        return None, SourceValueState.NOT_APPLICABLE
    if lowered.startswith("unable to check"):
        return None, SourceValueState.UNABLE_TO_CHECK

    try:
        return Decimal(text), SourceValueState.REPORTED
    except InvalidOperation as e:
        raise MappingError(
            f"invalid decimal value for source field {field_name!r}",
            mapper=mapper,
            source_record_id=record.source_record_id,
            source_fields=(field_name,),
        ) from e


def _measure_field(measure_value: Decimal | None, field_name: str) -> MappedField[MetricMeasure]:
    if measure_value is None:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=(field_name,),
        )

    return MappedField(
        value=NumericMetricMeasure(value=measure_value),
        quality=FieldQuality.DIRECT,
        source_fields=(field_name,),
    )


def _typed_slot(
    actual_family: MetricFamily,
    metric_type: EnergyMetricType | EmissionsMetricType | WaterMetricType,
    slot_family: MetricFamily,
    field_name: str,
) -> MappedField[Any]:
    if actual_family is not slot_family:
        return MappedField(value=None, quality=FieldQuality.UNMAPPED, source_fields=())

    return MappedField(
        value=metric_type,
        quality=FieldQuality.STANDARDIZED,
        source_fields=(field_name,),
    )


def _source_factor_version(flag: str | None) -> str | None:
    if flag is None:
        return None

    normalized = flag.strip().lower()
    if normalized == "yes":
        return METHODOLOGY_VERSION_POST_AUG_2023
    if normalized == "no":
        return METHODOLOGY_VERSION_PRE_AUG_2023

    return None


def _methodology_version_field(
    *,
    uses_factor: bool,
    version: str | None,
    flag_present: bool,
    field_name: str,
) -> MappedField[str]:
    if not uses_factor:
        return MappedField(value=None, quality=FieldQuality.UNMAPPED, source_fields=())

    if version is None:
        quality = FieldQuality.NOT_PROVIDED if flag_present else FieldQuality.UNMAPPED
        source_fields = ("post_aug_2023_source_factor",) if flag_present else ()

        return MappedField(value=None, quality=quality, source_fields=source_fields)

    return MappedField(
        value=version,
        quality=FieldQuality.STANDARDIZED,
        source_fields=("post_aug_2023_source_factor", field_name),
    )


def _map_source_identifiers(ewrb_id: str) -> MappedField[tuple[SourceIdentifier, ...]]:
    return MappedField(
        value=(
            SourceIdentifier(
                value=ewrb_id,
                identifier_kind=_category(
                    "ontario-ewrb-id",
                    label="Ontario EWRB Reporting ID",
                    taxonomy_id=_IDENTIFIER_TAXONOMY_ID,
                ),
            ),
        ),
        quality=FieldQuality.DIRECT,
        source_fields=("ewrb_id",),
    )


def _map_address(raw: Mapping[str, Any], snapshot: SourceSnapshot) -> MappedField[Address]:
    city = str_or_none(raw.get("city"))
    postal_code = str_or_none(raw.get("postal_code"))

    if city is None and postal_code is None:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=_ADDRESS_FIELDS,
        )

    return MappedField(
        value=Address(
            country=snapshot.jurisdiction.country,
            region=snapshot.jurisdiction.region,
            locality=city,
            street=None,
            postal_code=postal_code,
        ),
        quality=FieldQuality.DIRECT,
        source_fields=_ADDRESS_FIELDS,
    )


def _map_property_types(raw: Mapping[str, Any]) -> MappedField[tuple[CategoryRef, ...]]:
    consumed: list[str] = []
    seen: set[str] = set()
    categories: list[CategoryRef] = []

    for field_name in _PROPERTY_TYPE_SOURCE_FIELDS:
        value = str_or_none(raw.get(field_name))
        if value is None:
            continue

        consumed.append(field_name)
        for part in _split_semicolon(value):
            code = slugify(part)
            if code in seen:
                continue

            seen.add(code)
            categories.append(_category(code, label=part, taxonomy_id=_PROPERTY_TYPE_TAXONOMY_ID))

    if not categories:
        return MappedField(
            value=(),
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=_PROPERTY_TYPE_SOURCE_FIELDS,
        )

    return MappedField(
        value=tuple(categories),
        quality=FieldQuality.STANDARDIZED,
        source_fields=tuple(consumed),
    )


def _map_data_quality_caveats(
    raw: Mapping[str, Any],
) -> MappedField[tuple[CategoryRef, ...]]:
    consumed: list[str] = []
    caveats: list[CategoryRef] = []

    checker = str_or_none(raw.get("data_quality_checker_run"))
    if checker is not None:
        consumed.append("data_quality_checker_run")
        if checker.strip().lower() == "no":
            caveats.append(
                _category(
                    "data-quality-checker-not-run",
                    label="Data Quality Checker Not Run",
                    taxonomy_id=_DATA_QUALITY_TAXONOMY_ID,
                )
            )

    certifications = str_or_none(raw.get("third_party_certifications"))
    if certifications is not None:
        consumed.append("third_party_certifications")
        for cert in _split_semicolon(certifications):
            caveats.append(
                _category(
                    slugify(cert),
                    label=cert,
                    taxonomy_id=_CERTIFICATION_TAXONOMY_ID,
                )
            )

    if not consumed:
        return MappedField(
            value=(),
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("data_quality_checker_run",),
        )

    return MappedField(
        value=tuple(caveats),
        quality=FieldQuality.STANDARDIZED,
        source_fields=tuple(consumed),
    )


def _map_source_caveats() -> MappedField[tuple[CategoryRef, ...]]:
    return MappedField(
        value=ontario_ewrb_caveat_categories(),
        quality=FieldQuality.STANDARDIZED,
        source_fields=(EWRB_CAVEAT_SOURCE_FIELD,),
    )


def _split_semicolon(value: object) -> tuple[str, ...]:
    text = str_or_none(value)
    if text is None:
        return ()

    return tuple(part.strip() for part in text.split(";") if part.strip())


def _required_text(
    raw: Mapping[str, Any],
    field_name: str,
    mapper: MapperVersion,
    record: RawRecord,
) -> str:
    return require_text(
        raw.get(field_name),
        field_name=field_name,
        mapper=mapper,
        source_record_id=record.source_record_id,
    )


def _require_year(
    raw: Mapping[str, Any],
    mapper: MapperVersion,
    record: RawRecord,
) -> int:
    text = _required_text(raw, EWRB_REPORTING_YEAR_FIELD, mapper, record)
    try:
        return int(text)
    except ValueError as e:
        raise MappingError(
            f"invalid integer value for source field {EWRB_REPORTING_YEAR_FIELD!r}",
            mapper=mapper,
            source_record_id=record.source_record_id,
            source_fields=(EWRB_REPORTING_YEAR_FIELD,),
        ) from e


def _category(
    code: str,
    *,
    taxonomy_id: str,
    label: str,
) -> CategoryRef:
    return CategoryRef(
        code=slugify(code),
        label=label,
        taxonomy_id=taxonomy_id,
        taxonomy_version=EWRB_TAXONOMY_VERSION,
    )


def _build_provenance(
    *,
    record: RawRecord,
    snapshot: SourceSnapshot,
    mapper: MapperVersion,
) -> ProvenanceRef:
    return ProvenanceRef(
        snapshot_id=snapshot.snapshot_id,
        source_id=snapshot.source_id,
        dataset_id=snapshot.dataset_id,
        jurisdiction=snapshot.jurisdiction,
        fetched_at=snapshot.fetched_at,
        mapper=mapper,
        source_record_id=record.source_record_id,
    )


def _mapping_report(
    raw: Mapping[str, Any],
    record: BaseModel | tuple[BaseModel, ...],
) -> MappingReport:
    consumed: set[str] = set()
    _collect_source_fields(record, consumed)

    return MappingReport(unmapped_source_fields=tuple(sorted(set(raw) - consumed)))


def _collect_source_fields(value: object, consumed: set[str]) -> None:
    if isinstance(value, MappedField):
        consumed.update(value.source_fields)
        return

    if isinstance(value, BaseModel):
        for field_name in value.__class__.model_fields:
            _collect_source_fields(getattr(value, field_name), consumed)
        return

    if isinstance(value, tuple):
        for item in cast(tuple[object, ...], value):
            _collect_source_fields(item, consumed)
