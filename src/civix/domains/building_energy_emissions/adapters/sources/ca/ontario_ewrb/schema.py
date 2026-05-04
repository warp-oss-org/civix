"""Source-field schema and taxonomy specs for Ontario EWRB fixture rows."""

from __future__ import annotations

from typing import Final

from civix.core.drift import JsonFieldKind, SchemaFieldSpec, SourceSchemaSpec, TaxonomySpec

EWRB_SCHEMA_VERSION: Final[str] = "2026-05-03"
EWRB_TAXONOMY_VERSION: Final[str] = "2026-05-03"

# Synthetic field injected by the adapter to carry the reporting year. The
# EWRB workbooks themselves do not include a year column; the year is
# implied by the resource the adapter selected from the CKAN package.
EWRB_REPORTING_YEAR_FIELD: Final[str] = "_reporting_year"

_IDENTITY_FIELDS: Final[tuple[str, ...]] = ("ewrb_id",)
_PERIOD_FIELDS: Final[tuple[str, ...]] = (EWRB_REPORTING_YEAR_FIELD,)
_LOCATION_FIELDS: Final[tuple[str, ...]] = ("city", "postal_code")
_DESCRIPTOR_FIELDS: Final[tuple[str, ...]] = (
    "primary_property_type_calculated",
    "primary_property_type_self",
    "largest_property_use_type",
    "all_property_use_types",
    "third_party_certifications",
)
_ENERGY_INTENSITY_FIELDS: Final[tuple[str, ...]] = (
    "weather_normalized_site_electricity_intensity_gj_per_m2",
    "weather_normalized_site_electricity_intensity_kwh_per_ft2",
    "weather_normalized_site_natural_gas_intensity_gj_per_m2",
    "weather_normalized_site_natural_gas_intensity_m3_per_m2",
    "weather_normalized_site_natural_gas_intensity_m3_per_ft2",
    "site_eui_gj_per_m2",
    "site_eui_ekwh_per_ft2",
    "source_eui_gj_per_m2",
    "source_eui_ekwh_per_ft2",
    "weather_normalized_site_eui_gj_per_m2",
    "weather_normalized_site_eui_ekwh_per_ft2",
    "weather_normalized_source_eui_gj_per_m2",
    "weather_normalized_source_eui_ekwh_per_ft2",
)
_WATER_INTENSITY_FIELDS: Final[tuple[str, ...]] = (
    "all_water_intensity_m3_per_m2",
    "all_water_intensity_m3_per_ft2",
    "indoor_water_intensity_m3_per_m2",
    "indoor_water_intensity_m3_per_ft2",
)
_EMISSIONS_INTENSITY_FIELDS: Final[tuple[str, ...]] = (
    "ghg_emissions_intensity_kgco2e_per_m2",
    "ghg_emissions_intensity_kgco2e_per_ft2",
)
_SCORE_FIELDS: Final[tuple[str, ...]] = (
    "energy_star_score",
    "energy_star_certifications",
)
_QUALITY_FIELDS: Final[tuple[str, ...]] = (
    "data_quality_checker_run",
    "data_quality_checker_date",
)
# Ontario flags rows whose source EUI was calculated with the NRCan source
# factors that took effect 2023-08-28. The flag travels onto each affected
# `BuildingMetricValue` as factor/methodology version metadata so mappers do
# not silently mix pre- and post-change source-EUI submissions.
_METHODOLOGY_FIELDS: Final[tuple[str, ...]] = ("post_aug_2023_source_factor",)

EWRB_METRIC_FIELDS: Final[tuple[str, ...]] = (
    *_ENERGY_INTENSITY_FIELDS,
    *_WATER_INTENSITY_FIELDS,
    *_EMISSIONS_INTENSITY_FIELDS,
    *_SCORE_FIELDS[:1],
)

EWRB_FIELDS: Final[tuple[str, ...]] = (
    *_IDENTITY_FIELDS,
    *_PERIOD_FIELDS,
    *_LOCATION_FIELDS,
    *_DESCRIPTOR_FIELDS,
    *_ENERGY_INTENSITY_FIELDS,
    *_WATER_INTENSITY_FIELDS,
    *_EMISSIONS_INTENSITY_FIELDS,
    *_SCORE_FIELDS,
    *_QUALITY_FIELDS,
    *_METHODOLOGY_FIELDS,
)

# The published workbook headers in the order they appear in
# `Sheet1`. Mapped to the canonical snake_case field names above. A
# rename in the source workbook surfaces as schema drift via the
# missing-field check rather than a silent fallback here.
EWRB_HEADER_NORMALIZATION: Final[dict[str, str]] = {
    "EWRB_ID": "ewrb_id",
    "City": "city",
    "Postal_Code": "postal_code",
    "PrimPropTypCalc": "primary_property_type_calculated",
    "PrimPropTypSelf": "primary_property_type_self",
    "Largest_PropTyp": "largest_property_use_type",
    "All_Prop_Types": "all_property_use_types",
    "Thrd_Party_Cert": "third_party_certifications",
    "WN_Sit_Elc_Int1": "weather_normalized_site_electricity_intensity_gj_per_m2",
    "WN_Sit_Elc_Int2": "weather_normalized_site_electricity_intensity_kwh_per_ft2",
    "WN_Sit_Gas_Int1": "weather_normalized_site_natural_gas_intensity_gj_per_m2",
    "WN_Sit_Gas_Int2": "weather_normalized_site_natural_gas_intensity_m3_per_m2",
    "WN_Sit_Gas_Int3": "weather_normalized_site_natural_gas_intensity_m3_per_ft2",
    "All_Water_Int1": "all_water_intensity_m3_per_m2",
    "All_Water_Int2": "all_water_intensity_m3_per_ft2",
    "Ind_Water_Int1": "indoor_water_intensity_m3_per_m2",
    "Ind_Water_Int2": "indoor_water_intensity_m3_per_ft2",
    "Site_EUI1": "site_eui_gj_per_m2",
    "Site_EUI2": "site_eui_ekwh_per_ft2",
    "Source_EUI1": "source_eui_gj_per_m2",
    "Source_EUI2": "source_eui_ekwh_per_ft2",
    "WN_Site_EUI1": "weather_normalized_site_eui_gj_per_m2",
    "WN_Site_EUI2": "weather_normalized_site_eui_ekwh_per_ft2",
    "WN_Source_EUI1": "weather_normalized_source_eui_gj_per_m2",
    "WN_Source_EUI2": "weather_normalized_source_eui_ekwh_per_ft2",
    "GHG_Emiss_Int1": "ghg_emissions_intensity_kgco2e_per_m2",
    "GHG_Emiss_Int2": "ghg_emissions_intensity_kgco2e_per_ft2",
    "Ener_Star_Score": "energy_star_score",
    "Ener_Star_Certs": "energy_star_certifications",
    "Data_Qual_Check": "data_quality_checker_run",
    "Data_Qual_Date": "data_quality_checker_date",
    "Post_Aug_2023_Src_Factor": "post_aug_2023_source_factor",
}

_STRING: Final[tuple[JsonFieldKind, ...]] = (JsonFieldKind.STRING,)

# Cells are stringified by the adapter so heterogeneous spreadsheet types
# (int EWRB_ID, float intensities, string sentinels like `Not Available`,
# date strings) collapse to a single shape downstream. Sentinel handling
# lives in the mapper, not in the schema.
EWRB_RAW_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="ontario-ewrb-raw",
    version=EWRB_SCHEMA_VERSION,
    fields=(
        SchemaFieldSpec(name="ewrb_id", kinds=_STRING),
        SchemaFieldSpec(name=EWRB_REPORTING_YEAR_FIELD, kinds=_STRING),
        SchemaFieldSpec(name="city", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="postal_code", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="primary_property_type_calculated", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="primary_property_type_self", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="largest_property_use_type", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="all_property_use_types", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="third_party_certifications", kinds=_STRING, nullable=True),
        *(
            SchemaFieldSpec(name=name, kinds=_STRING, nullable=True)
            for name in (
                *_ENERGY_INTENSITY_FIELDS,
                *_WATER_INTENSITY_FIELDS,
                *_EMISSIONS_INTENSITY_FIELDS,
                *_SCORE_FIELDS,
                *_QUALITY_FIELDS,
                *_METHODOLOGY_FIELDS,
            )
        ),
    ),
)

# A small subset of the Portfolio Manager property-type vocabulary that
# Ontario surfaces in the EWRB workbook. The full vocabulary is large and
# evolves with Portfolio Manager releases; unrecognized values surface as
# taxonomy drift rather than being suppressed by a comprehensive list.
_KNOWN_PROPERTY_USE_TYPES: Final[frozenset[str]] = frozenset(
    {
        "Multifamily Housing",
        "Office",
        "Other",
        "Manufacturing/Industrial Plant",
        "Distribution Center",
        "Retail Store",
        "Non-Refrigerated Warehouse",
        "Strip Mall",
        "Senior Living Community",
        "Supermarket/Grocery Store",
        "Hotel",
        "K-12 School",
        "Hospital (General Medical & Surgical)",
        "College/University",
        "Mixed Use Property",
    }
)
_KNOWN_DATA_QUALITY_RESULTS: Final[frozenset[str]] = frozenset({"yes", "no"})

EWRB_PROPERTY_TYPE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="ontario-ewrb-primary-property-type",
    version=EWRB_TAXONOMY_VERSION,
    source_field="primary_property_type_calculated",
    normalization="exact",
    known_values=_KNOWN_PROPERTY_USE_TYPES,
)
EWRB_DATA_QUALITY_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="ontario-ewrb-data-quality-checker-run",
    version=EWRB_TAXONOMY_VERSION,
    source_field="data_quality_checker_run",
    normalization="strip_casefold",
    known_values=_KNOWN_DATA_QUALITY_RESULTS,
)
EWRB_SOURCE_FACTOR_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="ontario-ewrb-post-aug-2023-source-factor",
    version=EWRB_TAXONOMY_VERSION,
    source_field="post_aug_2023_source_factor",
    normalization="strip_casefold",
    known_values=_KNOWN_DATA_QUALITY_RESULTS,
)

EWRB_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    EWRB_PROPERTY_TYPE_TAXONOMY,
    EWRB_DATA_QUALITY_TAXONOMY,
    EWRB_SOURCE_FACTOR_TAXONOMY,
)
