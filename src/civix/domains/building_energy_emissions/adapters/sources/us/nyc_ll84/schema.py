"""Source-field schema and taxonomy specs for NYC LL84 fixture rows."""

from __future__ import annotations

from typing import Final

from civix.core.drift import JsonFieldKind, SchemaFieldSpec, SourceSchemaSpec, TaxonomySpec

LL84_SCHEMA_VERSION: Final[str] = "2026-05-03"
LL84_TAXONOMY_VERSION: Final[str] = "2026-05-03"

_IDENTITY_FIELDS: Final[tuple[str, ...]] = (
    "property_id",
    "parent_property_id",
    "parent_property_name",
    "bbl",
    "bin",
)
_PERIOD_FIELDS: Final[tuple[str, ...]] = (
    "report_year",
    "report_submission_date",
    "report_generation_date",
)
_LOCATION_FIELDS: Final[tuple[str, ...]] = (
    "address_1",
    "city",
    "borough",
    "postcode",
    "latitude",
    "longitude",
)
_DESCRIPTOR_FIELDS: Final[tuple[str, ...]] = (
    "primary_property_type_self",
    "largest_property_use_type",
    "all_property_use_types",
    "property_gfa_self_reported",
    "year_built",
)
_METRIC_FIELDS: Final[tuple[str, ...]] = (
    "site_eui_kbtu_ft2",
    "weather_normalized_site_eui_kbtu_ft2",
    "source_eui_kbtu_ft2",
    "weather_normalized_source_eui_kbtu_ft2",
    "electricity_use_grid_purchase_kbtu",
    "natural_gas_use_kbtu",
    "total_ghg_emissions_metric",
    "direct_ghg_emissions_metric_tons_co2e",
    "indirect_ghg_emissions_metric_tons_co2e",
    "energy_star_score",
    "water_use_all_water_sources_kgal",
)
_QUALITY_FIELDS: Final[tuple[str, ...]] = (
    "data_quality_checker_run",
    "electric_meter_alert",
    "gas_meter_alert",
    "water_meter_alert",
)

LL84_OUT_FIELDS: Final[tuple[str, ...]] = (
    *_IDENTITY_FIELDS,
    *_PERIOD_FIELDS,
    *_LOCATION_FIELDS,
    *_DESCRIPTOR_FIELDS,
    *_METRIC_FIELDS,
    *_QUALITY_FIELDS,
)

_STRING: Final[tuple[JsonFieldKind, ...]] = (JsonFieldKind.STRING,)

# Socrata returns every value as a JSON string for this dataset, including
# numeric columns. Schema is declared as STRING with nullable=True for
# value-bearing fields so that source sentinels (`Not Available`,
# `Not Applicable: Standalone Property`, `Unable to Check (not enough data)`)
# do not surface as schema drift; sentinel handling lives in the mapper.
LL84_RAW_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="nyc-ll84-benchmarking-raw",
    version=LL84_SCHEMA_VERSION,
    fields=(
        SchemaFieldSpec(name="property_id", kinds=_STRING),
        SchemaFieldSpec(name="parent_property_id", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="parent_property_name", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="bbl", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="bin", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="report_year", kinds=_STRING),
        SchemaFieldSpec(name="report_submission_date", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="report_generation_date", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="address_1", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="city", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="borough", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="postcode", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="latitude", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="longitude", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="primary_property_type_self", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="largest_property_use_type", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="all_property_use_types", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="property_gfa_self_reported", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="year_built", kinds=_STRING, nullable=True),
        *(SchemaFieldSpec(name=name, kinds=_STRING, nullable=True) for name in _METRIC_FIELDS),
        *(SchemaFieldSpec(name=name, kinds=_STRING, nullable=True) for name in _QUALITY_FIELDS),
    ),
)

_KNOWN_BOROUGHS: Final[frozenset[str]] = frozenset(
    {"manhattan", "bronx", "brooklyn", "queens", "staten island"}
)
_KNOWN_DATA_QUALITY_RESULTS: Final[frozenset[str]] = frozenset({"ok", "possible issue"})
_KNOWN_METER_ALERTS: Final[frozenset[str]] = frozenset(
    {"no alert", "possible issue", "not available"}
)
_KNOWN_PROPERTY_USE_TYPES: Final[frozenset[str]] = frozenset(
    {
        "Multifamily Housing",
        "Office",
        "K-12 School",
        "Hospital (General Medical & Surgical)",
        "Hotel",
        "Retail Store",
        "Non-Refrigerated Warehouse",
        "Worship Facility",
        "College/University",
        "Other",
    }
)

LL84_BOROUGH_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-ll84-borough",
    version=LL84_TAXONOMY_VERSION,
    source_field="borough",
    normalization="strip_casefold",
    known_values=_KNOWN_BOROUGHS,
)
LL84_PROPERTY_TYPE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-ll84-primary-property-type",
    version=LL84_TAXONOMY_VERSION,
    source_field="primary_property_type_self",
    normalization="exact",
    known_values=_KNOWN_PROPERTY_USE_TYPES,
)
LL84_DATA_QUALITY_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-ll84-data-quality-checker-result",
    version=LL84_TAXONOMY_VERSION,
    source_field="data_quality_checker_run",
    normalization="strip_casefold",
    known_values=_KNOWN_DATA_QUALITY_RESULTS,
)
LL84_METER_ALERT_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = tuple(
    TaxonomySpec(
        taxonomy_id=f"nyc-ll84-{field.replace('_', '-')}",
        version=LL84_TAXONOMY_VERSION,
        source_field=field,
        normalization="strip_casefold",
        known_values=_KNOWN_METER_ALERTS,
    )
    for field in ("electric_meter_alert", "gas_meter_alert", "water_meter_alert")
)

LL84_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    LL84_BOROUGH_TAXONOMY,
    LL84_PROPERTY_TYPE_TAXONOMY,
    LL84_DATA_QUALITY_TAXONOMY,
    *LL84_METER_ALERT_TAXONOMIES,
)
