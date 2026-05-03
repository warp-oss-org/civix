"""NYC LL84 annual benchmarking mappers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
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
from civix.domains.building_energy_emissions.adapters.sources.us.nyc_ll84.caveats import (
    LL84_CAVEAT_SOURCE_FIELD,
    nyc_ll84_caveat_categories,
)
from civix.domains.building_energy_emissions.adapters.sources.us.nyc_ll84.schema import (
    LL84_TAXONOMY_VERSION,
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

SUBJECT_MAPPER_ID: Final[MapperId] = MapperId("nyc-ll84-subject")
REPORT_MAPPER_ID: Final[MapperId] = MapperId("nyc-ll84-report")
METRICS_MAPPER_ID: Final[MapperId] = MapperId("nyc-ll84-metrics")
SUBJECT_MAPPER_VERSION: Final[str] = "0.1.0"
REPORT_MAPPER_VERSION: Final[str] = "0.1.0"
METRICS_MAPPER_VERSION: Final[str] = "0.1.0"

METHODOLOGY_LABEL: Final[str] = "ENERGY STAR Portfolio Manager"

_IDENTIFIER_TAXONOMY_ID: Final[str] = "nyc-ll84-source-identifier-kind"
_PROPERTY_TYPE_TAXONOMY_ID: Final[str] = "nyc-ll84-property-use-type"
_FLOOR_AREA_UNIT_TAXONOMY_ID: Final[str] = "nyc-ll84-floor-area-unit"
_METRIC_UNIT_TAXONOMY_ID: Final[str] = "nyc-ll84-metric-unit"
_METRIC_LABEL_TAXONOMY_ID: Final[str] = "nyc-ll84-metric-label"
_METER_ALERT_TAXONOMY_ID: Final[str] = "nyc-ll84-meter-alert"
_DATA_QUALITY_TAXONOMY_ID: Final[str] = "nyc-ll84-data-quality-checker-result"
_SUBJECT_KIND_TAXONOMY_ID: Final[str] = "nyc-ll84-subject-kind"

_STANDALONE_PARENT_SENTINELS: Final[frozenset[str]] = frozenset(
    {"Not Applicable: Standalone Property"}
)
_CLASSIFIED_ADDRESS_TOKEN: Final[str] = "classified"

_ADDRESS_FIELDS: Final[tuple[str, ...]] = ("address_1", "city", "borough", "postcode")
_PARENT_FIELDS: Final[tuple[str, ...]] = ("parent_property_id", "parent_property_name")
_PROPERTY_TYPE_SOURCE_FIELDS: Final[tuple[str, ...]] = (
    "primary_property_type_self",
    "largest_property_use_type",
    "all_property_use_types",
)
_METER_ALERT_FIELDS: Final[tuple[str, ...]] = (
    "electric_meter_alert",
    "gas_meter_alert",
    "water_meter_alert",
)


_MetricSpec = tuple[
    str,  # source field name
    MetricFamily,
    EnergyMetricType | EmissionsMetricType | WaterMetricType | None,
    str,  # unit code
    str,  # unit label
    str,  # human label
]


_METRIC_SPECS: Final[tuple[_MetricSpec, ...]] = (
    (
        "site_eui_kbtu_ft2",
        MetricFamily.ENERGY,
        EnergyMetricType.SITE_EUI,
        "kbtu-per-ft2",
        "kBtu/ft2",
        "Site EUI",
    ),
    (
        "weather_normalized_site_eui_kbtu_ft2",
        MetricFamily.ENERGY,
        EnergyMetricType.WEATHER_NORMALIZED_SITE_EUI,
        "kbtu-per-ft2",
        "kBtu/ft2",
        "Weather Normalized Site EUI",
    ),
    (
        "source_eui_kbtu_ft2",
        MetricFamily.ENERGY,
        EnergyMetricType.SOURCE_EUI,
        "kbtu-per-ft2",
        "kBtu/ft2",
        "Source EUI",
    ),
    (
        "weather_normalized_source_eui_kbtu_ft2",
        MetricFamily.ENERGY,
        EnergyMetricType.WEATHER_NORMALIZED_SOURCE_EUI,
        "kbtu-per-ft2",
        "kBtu/ft2",
        "Weather Normalized Source EUI",
    ),
    (
        "electricity_use_grid_purchase_kbtu",
        MetricFamily.ENERGY,
        EnergyMetricType.ELECTRICITY_USE,
        "kbtu",
        "kBtu",
        "Electricity Use - Grid Purchase",
    ),
    (
        "natural_gas_use_kbtu",
        MetricFamily.ENERGY,
        EnergyMetricType.NATURAL_GAS_USE,
        "kbtu",
        "kBtu",
        "Natural Gas Use",
    ),
    (
        "energy_star_score",
        MetricFamily.ENERGY,
        EnergyMetricType.ENERGY_STAR_SCORE,
        "score",
        "Score Points",
        "ENERGY STAR Score",
    ),
    (
        "total_ghg_emissions_metric",
        MetricFamily.EMISSIONS,
        EmissionsMetricType.LOCATION_BASED_GHG,
        "metric-tons-co2e",
        "Metric Tons CO2e",
        "Total Location-Based GHG Emissions",
    ),
    (
        "direct_ghg_emissions_metric_tons_co2e",
        MetricFamily.EMISSIONS,
        EmissionsMetricType.DIRECT_GHG,
        "metric-tons-co2e",
        "Metric Tons CO2e",
        "Direct GHG Emissions",
    ),
    (
        "indirect_ghg_emissions_metric_tons_co2e",
        MetricFamily.EMISSIONS,
        EmissionsMetricType.INDIRECT_GHG,
        "metric-tons-co2e",
        "Metric Tons CO2e",
        "Indirect GHG Emissions",
    ),
    (
        "water_use_all_water_sources_kgal",
        MetricFamily.WATER,
        WaterMetricType.WATER_USE,
        "kgal",
        "kgal",
        "Water Use - All Water Sources",
    ),
)


@dataclass(frozen=True, slots=True)
class NycLl84SubjectMapper:
    """Maps one NYC LL84 row to a `BuildingEnergySubject`."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=SUBJECT_MAPPER_ID, version=SUBJECT_MAPPER_VERSION)

    def __call__(
        self, record: RawRecord, snapshot: SourceSnapshot
    ) -> MapResult[BuildingEnergySubject]:
        raw = record.raw_data
        property_id = _required_text(raw, "property_id", self.version, record)

        subject = BuildingEnergySubject(
            provenance=_build_provenance(record=record, snapshot=snapshot, mapper=self.version),
            subject_key=build_building_energy_subject_key(
                SourceId(str(snapshot.source_id)),
                DatasetId(str(snapshot.dataset_id)),
                property_id,
            ),
            source_subject_identifiers=_map_source_identifiers(raw),
            subject_kind=MappedField(
                value=BuildingSubjectKind.REPORTING_ACCOUNT,
                quality=FieldQuality.STANDARDIZED,
                source_fields=("property_id",),
            ),
            source_subject_kind=MappedField(
                value=_category(
                    "espm-reporting-account",
                    label="ENERGY STAR Portfolio Manager Reporting Account",
                    taxonomy_id=_SUBJECT_KIND_TAXONOMY_ID,
                ),
                quality=FieldQuality.STANDARDIZED,
                source_fields=("property_id",),
            ),
            identity_certainty=MappedField(
                value=IdentityCertainty.STABLE_CROSS_YEAR,
                quality=FieldQuality.STANDARDIZED,
                source_fields=("property_id",),
            ),
            parent_subject_key=_map_parent_subject_key(raw, snapshot, property_id),
            name=_map_subject_name(raw),
            jurisdiction=MappedField(
                value=snapshot.jurisdiction,
                quality=FieldQuality.STANDARDIZED,
                source_fields=("borough",),
            ),
            address=_map_address(raw, snapshot),
            coordinate=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            property_types=_map_property_types(raw),
            floor_area=_map_floor_area(raw, self.version, record),
            floor_area_unit=_map_floor_area_unit(raw),
            year_built=_map_year_built(raw, self.version, record),
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
class NycLl84ReportMapper:
    """Maps one NYC LL84 row to a `BuildingEnergyReport`."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=REPORT_MAPPER_ID, version=REPORT_MAPPER_VERSION)

    def __call__(
        self, record: RawRecord, snapshot: SourceSnapshot
    ) -> MapResult[BuildingEnergyReport]:
        raw = record.raw_data
        property_id = _required_text(raw, "property_id", self.version, record)
        report_year = _require_year(raw, self.version, record)
        subject_key = build_building_energy_subject_key(
            SourceId(str(snapshot.source_id)),
            DatasetId(str(snapshot.dataset_id)),
            property_id,
        )
        report_key = build_building_energy_report_key(
            SourceId(str(snapshot.source_id)),
            DatasetId(str(snapshot.dataset_id)),
            property_id,
            str(report_year),
        )

        report = BuildingEnergyReport(
            provenance=_build_provenance(record=record, snapshot=snapshot, mapper=self.version),
            report_key=report_key,
            subject_key=subject_key,
            source_report_identifiers=MappedField(
                value=(
                    SourceIdentifier(
                        value=property_id,
                        identifier_kind=_category(
                            "espm-property-id",
                            label="ENERGY STAR Portfolio Manager Property ID",
                            taxonomy_id=_IDENTIFIER_TAXONOMY_ID,
                        ),
                    ),
                    SourceIdentifier(
                        value=str(report_year),
                        identifier_kind=_category(
                            "report-year",
                            label="Reporting Year",
                            taxonomy_id=_IDENTIFIER_TAXONOMY_ID,
                        ),
                    ),
                ),
                quality=FieldQuality.DIRECT,
                source_fields=("property_id", "report_year"),
            ),
            reporting_period=MappedField(
                value=TemporalPeriod(
                    precision=TemporalPeriodPrecision.YEAR,
                    year_value=report_year,
                    timezone_status=TemporalTimezoneStatus.UNKNOWN,
                ),
                quality=FieldQuality.STANDARDIZED,
                source_fields=("report_year",),
            ),
            reporting_period_precision=MappedField(
                value=ReportingPeriodPrecision.CALENDAR_YEAR,
                quality=FieldQuality.STANDARDIZED,
                source_fields=("report_year",),
            ),
            report_submission_date=_map_iso_date(
                raw, "report_submission_date", self.version, record
            ),
            report_generation_date=_map_iso_date(
                raw, "report_generation_date", self.version, record
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
class NycLl84MetricsMapper:
    """Maps one NYC LL84 row to its tuple of `BuildingMetricValue` rows."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=METRICS_MAPPER_ID, version=METRICS_MAPPER_VERSION)

    def __call__(
        self, record: RawRecord, snapshot: SourceSnapshot
    ) -> MapResult[tuple[BuildingMetricValue, ...]]:
        raw = record.raw_data
        property_id = _required_text(raw, "property_id", self.version, record)
        report_year = _require_year(raw, self.version, record)
        source_id = SourceId(str(snapshot.source_id))
        dataset_id = DatasetId(str(snapshot.dataset_id))
        subject_key = build_building_energy_subject_key(source_id, dataset_id, property_id)
        report_key = build_building_energy_report_key(
            source_id, dataset_id, property_id, str(report_year)
        )
        effective_period = TemporalPeriod(
            precision=TemporalPeriodPrecision.YEAR,
            year_value=report_year,
            timezone_status=TemporalTimezoneStatus.UNKNOWN,
        )

        metrics = tuple(
            self._map_metric(
                raw=raw,
                record=record,
                snapshot=snapshot,
                spec=spec,
                subject_key=subject_key,
                report_key=report_key,
                effective_period=effective_period,
            )
            for spec in _METRIC_SPECS
        )

        return MapResult(record=metrics, report=_mapping_report(raw, metrics))

    def _map_metric(
        self,
        *,
        raw: Mapping[str, Any],
        record: RawRecord,
        snapshot: SourceSnapshot,
        spec: _MetricSpec,
        subject_key: str,
        report_key: str,
        effective_period: TemporalPeriod,
    ) -> BuildingMetricValue:
        field_name, family, metric_type, unit_code, unit_label, label = spec
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

        return BuildingMetricValue(
            provenance=_build_provenance(record=record, snapshot=snapshot, mapper=self.version),
            metric_key=metric_key,
            report_key=MappedField(
                value=report_key,
                quality=FieldQuality.STANDARDIZED,
                source_fields=("property_id", "report_year"),
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
                value=_category(field_name, label=label, taxonomy_id=_METRIC_LABEL_TAXONOMY_ID),
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
            methodology_version=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            methodology_url=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            emission_factor_version=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            value_source=MappedField(
                value=MetricValueSource.SOURCE_REPUBLISHED,
                quality=FieldQuality.STANDARDIZED,
                source_fields=(field_name,),
            ),
            effective_period=MappedField(
                value=effective_period,
                quality=FieldQuality.STANDARDIZED,
                source_fields=("report_year",),
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
    if lowered == "possible issue":
        return None, SourceValueState.FLAGGED_QUALITY_ISSUE
    if lowered == "ok":
        return None, SourceValueState.OK

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
    metric_type: EnergyMetricType | EmissionsMetricType | WaterMetricType | None,
    slot_family: MetricFamily,
    field_name: str,
) -> MappedField[Any]:
    if actual_family is not slot_family or metric_type is None:
        return MappedField(value=None, quality=FieldQuality.UNMAPPED, source_fields=())

    return MappedField(
        value=metric_type,
        quality=FieldQuality.STANDARDIZED,
        source_fields=(field_name,),
    )


def _map_source_identifiers(
    raw: Mapping[str, Any],
) -> MappedField[tuple[SourceIdentifier, ...]]:
    consumed: list[str] = []
    identifiers: list[SourceIdentifier] = []

    property_id = str_or_none(raw.get("property_id"))
    if property_id is not None:
        identifiers.append(
            SourceIdentifier(
                value=property_id,
                identifier_kind=_category(
                    "espm-property-id",
                    label="ENERGY STAR Portfolio Manager Property ID",
                    taxonomy_id=_IDENTIFIER_TAXONOMY_ID,
                ),
            )
        )
        consumed.append("property_id")

    for bbl_value in _split_semicolon(raw.get("bbl")):
        identifiers.append(
            SourceIdentifier(
                value=bbl_value,
                identifier_kind=_category(
                    "bbl",
                    label="Borough-Block-Lot",
                    taxonomy_id=_IDENTIFIER_TAXONOMY_ID,
                ),
            )
        )

    if str_or_none(raw.get("bbl")) is not None:
        consumed.append("bbl")

    for bin_value in _split_semicolon(raw.get("bin")):
        identifiers.append(
            SourceIdentifier(
                value=bin_value,
                identifier_kind=_category(
                    "bin",
                    label="Building Identification Number",
                    taxonomy_id=_IDENTIFIER_TAXONOMY_ID,
                ),
            )
        )

    if str_or_none(raw.get("bin")) is not None:
        consumed.append("bin")

    parent = _normalized_parent_property_id(raw)
    if parent is not None:
        identifiers.append(
            SourceIdentifier(
                value=parent,
                identifier_kind=_category(
                    "espm-parent-property-id",
                    label="ENERGY STAR Portfolio Manager Parent Property ID",
                    taxonomy_id=_IDENTIFIER_TAXONOMY_ID,
                ),
            )
        )
        consumed.append("parent_property_id")

    return MappedField(
        value=tuple(identifiers),
        quality=FieldQuality.DIRECT if identifiers else FieldQuality.NOT_PROVIDED,
        source_fields=tuple(consumed) if consumed else ("property_id",),
    )


def _map_parent_subject_key(
    raw: Mapping[str, Any],
    snapshot: SourceSnapshot,
    property_id: str,
) -> MappedField[str]:
    parent = _normalized_parent_property_id(raw)
    if parent is None or parent == property_id:
        return MappedField(value=None, quality=FieldQuality.UNMAPPED, source_fields=())

    parent_key = build_building_energy_subject_key(
        SourceId(str(snapshot.source_id)),
        DatasetId(str(snapshot.dataset_id)),
        parent,
    )

    return MappedField(
        value=parent_key,
        quality=FieldQuality.DERIVED,
        source_fields=("parent_property_id",),
    )


def _map_subject_name(raw: Mapping[str, Any]) -> MappedField[str]:
    # LL84 does not publish a per-property display name. `parent_property_name`
    # describes the campus the row's parent reporting account references, not
    # the row's own subject, so it cannot stand in for `subject.name`. The
    # campus name is recoverable from the parent subject's own row when one
    # exists; the parent-of relationship itself is preserved by
    # `parent_subject_key`.
    return MappedField(
        value=None,
        quality=FieldQuality.NOT_PROVIDED,
        source_fields=("property_id",),
    )


def _map_address(raw: Mapping[str, Any], snapshot: SourceSnapshot) -> MappedField[Address]:
    address_1 = str_or_none(raw.get("address_1"))

    if address_1 is not None and _CLASSIFIED_ADDRESS_TOKEN in address_1.lower():
        return MappedField(
            value=None,
            quality=FieldQuality.REDACTED,
            source_fields=_ADDRESS_FIELDS,
        )

    city = str_or_none(raw.get("city")) or snapshot.jurisdiction.locality
    region = snapshot.jurisdiction.region
    postal_code = str_or_none(raw.get("postcode"))

    if address_1 is None and city is None and postal_code is None:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=_ADDRESS_FIELDS,
        )

    return MappedField(
        value=Address(
            country=snapshot.jurisdiction.country,
            region=region,
            locality=city,
            street=address_1,
            postal_code=postal_code,
        ),
        quality=FieldQuality.DIRECT,
        source_fields=_ADDRESS_FIELDS,
    )


def _map_property_types(raw: Mapping[str, Any]) -> MappedField[tuple[CategoryRef, ...]]:
    raw_value = str_or_none(raw.get("all_property_use_types"))
    if raw_value is None:
        primary = str_or_none(raw.get("primary_property_type_self"))
        if primary is None:
            return MappedField(
                value=(),
                quality=FieldQuality.NOT_PROVIDED,
                source_fields=("all_property_use_types",),
            )

        return MappedField(
            value=(
                _category(
                    slugify(primary),
                    label=primary,
                    taxonomy_id=_PROPERTY_TYPE_TAXONOMY_ID,
                ),
            ),
            quality=FieldQuality.STANDARDIZED,
            source_fields=("primary_property_type_self",),
        )

    parts = tuple(
        _category(slugify(part), label=part, taxonomy_id=_PROPERTY_TYPE_TAXONOMY_ID)
        for part in _split_semicolon(raw_value)
    )

    return MappedField(
        value=parts,
        quality=FieldQuality.STANDARDIZED,
        source_fields=("all_property_use_types",),
    )


def _map_floor_area(
    raw: Mapping[str, Any],
    mapper: MapperVersion,
    record: RawRecord,
) -> MappedField[Decimal]:
    text = str_or_none(raw.get("property_gfa_self_reported"))
    if text is None:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("property_gfa_self_reported",),
        )

    try:
        decimal_value = Decimal(text)
    except InvalidOperation as e:
        raise MappingError(
            "invalid decimal value for source field 'property_gfa_self_reported'",
            mapper=mapper,
            source_record_id=record.source_record_id,
            source_fields=("property_gfa_self_reported",),
        ) from e

    return MappedField(
        value=decimal_value,
        quality=FieldQuality.DIRECT,
        source_fields=("property_gfa_self_reported",),
    )


def _map_floor_area_unit(raw: Mapping[str, Any]) -> MappedField[CategoryRef]:
    if str_or_none(raw.get("property_gfa_self_reported")) is None:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("property_gfa_self_reported",),
        )

    return MappedField(
        value=_category("ft2", label="ft2", taxonomy_id=_FLOOR_AREA_UNIT_TAXONOMY_ID),
        quality=FieldQuality.STANDARDIZED,
        source_fields=("property_gfa_self_reported",),
    )


def _map_year_built(
    raw: Mapping[str, Any],
    mapper: MapperVersion,
    record: RawRecord,
) -> MappedField[int]:
    text = str_or_none(raw.get("year_built"))
    if text is None:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("year_built",),
        )

    try:
        parsed = int(text)
    except ValueError as e:
        raise MappingError(
            "invalid integer value for source field 'year_built'",
            mapper=mapper,
            source_record_id=record.source_record_id,
            source_fields=("year_built",),
        ) from e

    return MappedField(
        value=parsed,
        quality=FieldQuality.DIRECT,
        source_fields=("year_built",),
    )


def _map_iso_date(
    raw: Mapping[str, Any],
    field_name: str,
    mapper: MapperVersion,
    record: RawRecord,
) -> MappedField[date]:
    text = str_or_none(raw.get(field_name))
    if text is None:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=(field_name,),
        )

    try:
        parsed = date.fromisoformat(text[:10])
    except ValueError as e:
        raise MappingError(
            f"invalid ISO date for source field {field_name!r}",
            mapper=mapper,
            source_record_id=record.source_record_id,
            source_fields=(field_name,),
        ) from e

    return MappedField(
        value=parsed,
        quality=FieldQuality.DIRECT,
        source_fields=(field_name,),
    )


def _map_data_quality_caveats(
    raw: Mapping[str, Any],
) -> MappedField[tuple[CategoryRef, ...]]:
    caveats: list[CategoryRef] = []
    consumed: list[str] = []

    checker = str_or_none(raw.get("data_quality_checker_run"))
    if checker is not None:
        consumed.append("data_quality_checker_run")
        if checker.lower() == "possible issue":
            caveats.append(
                _category(
                    "data-quality-checker-flagged",
                    label="Data Quality Checker Flagged Possible Issue",
                    taxonomy_id=_DATA_QUALITY_TAXONOMY_ID,
                )
            )

    for field_name in _METER_ALERT_FIELDS:
        alert = str_or_none(raw.get(field_name))
        if alert is None:
            continue

        consumed.append(field_name)
        if alert.lower() == "no alert":
            continue

        caveats.append(
            _category(
                slugify(f"{field_name}-{alert}"),
                label=f"{field_name.replace('_', ' ').title()}: {alert}",
                taxonomy_id=_METER_ALERT_TAXONOMY_ID,
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
        value=nyc_ll84_caveat_categories(),
        quality=FieldQuality.STANDARDIZED,
        source_fields=(LL84_CAVEAT_SOURCE_FIELD,),
    )


def _normalized_parent_property_id(raw: Mapping[str, Any]) -> str | None:
    parent = str_or_none(raw.get("parent_property_id"))
    if parent is None:
        return None

    if parent in _STANDALONE_PARENT_SENTINELS:
        return None

    return parent


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
    text = _required_text(raw, "report_year", mapper, record)
    try:
        return int(text)
    except ValueError as e:
        raise MappingError(
            "invalid integer value for source field 'report_year'",
            mapper=mapper,
            source_record_id=record.source_record_id,
            source_fields=("report_year",),
        ) from e


def _category(
    code: str,
    *,
    taxonomy_id: str,
    label: str | None = None,
) -> CategoryRef:
    resolved_label = (
        label if label is not None else code.replace("_", " ").replace("-", " ").title()
    )

    return CategoryRef(
        code=slugify(code),
        label=resolved_label,
        taxonomy_id=taxonomy_id,
        taxonomy_version=LL84_TAXONOMY_VERSION,
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
