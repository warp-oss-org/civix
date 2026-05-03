"""Source-field schema and taxonomy constants for England FCERM fixture rows."""

from __future__ import annotations

from typing import Final

from civix.core.drift import JsonFieldKind, SchemaFieldSpec, SourceSchemaSpec, TaxonomySpec

ENGLAND_FCERM_SCHEMA_VERSION: Final[str] = "2026-05-03"

PROJECT_NAME_FIELD: Final[str] = "Project Name"
ONS_REGION_FIELD: Final[str] = "Office of National Statistics Region"
RFCC_FIELD: Final[str] = "Regional Flood and Coastal Committee"
PARLIAMENTARY_CONSTITUENCY_FIELD: Final[str] = "Parliamentary Constituency"
CEREMONIAL_COUNTY_FIELD: Final[str] = "Ceremonial County - Project Location"
LEAD_AUTHORITY_FIELD: Final[str] = "Lead Risk Management Authority Name"
PROJECT_TYPE_FIELD: Final[str] = "Project Type"
RISK_SOURCE_FIELD: Final[str] = "Risk Source"
INDICATIVE_GOVERNMENT_INVESTMENT_FIELD: Final[str] = "Indicative Government Investment 2026/27 (£k)"

ENGLAND_FCERM_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    PROJECT_NAME_FIELD,
    ONS_REGION_FIELD,
    RFCC_FIELD,
    PARLIAMENTARY_CONSTITUENCY_FIELD,
    CEREMONIAL_COUNTY_FIELD,
    LEAD_AUTHORITY_FIELD,
    PROJECT_TYPE_FIELD,
    RISK_SOURCE_FIELD,
    INDICATIVE_GOVERNMENT_INVESTMENT_FIELD,
)

ENGLAND_FCERM_SCHEMES_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="england-fcerm-schemes-raw",
    version=ENGLAND_FCERM_SCHEMA_VERSION,
    fields=(
        SchemaFieldSpec(name=PROJECT_NAME_FIELD, kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name=ONS_REGION_FIELD, kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name=RFCC_FIELD, kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name=PARLIAMENTARY_CONSTITUENCY_FIELD, kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name=CEREMONIAL_COUNTY_FIELD, kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name=LEAD_AUTHORITY_FIELD, kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name=PROJECT_TYPE_FIELD, kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name=RISK_SOURCE_FIELD, kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(
            name=INDICATIVE_GOVERNMENT_INVESTMENT_FIELD,
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
        ),
    ),
)

ENGLAND_FCERM_PROJECT_TYPE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="england-fcerm-project-type",
    version=ENGLAND_FCERM_SCHEMA_VERSION,
    source_field=PROJECT_TYPE_FIELD,
    normalization="strip_casefold",
    known_values=frozenset({"capital maintenance", "defence", "property flood resilience"}),
)
ENGLAND_FCERM_RISK_SOURCE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="england-fcerm-risk-source",
    version=ENGLAND_FCERM_SCHEMA_VERSION,
    source_field=RISK_SOURCE_FIELD,
    normalization="strip_casefold",
    known_values=frozenset(
        {"coastal erosion", "river flooding", "sea flooding", "surface water flooding"}
    ),
)
ENGLAND_FCERM_ONS_REGION_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="england-fcerm-ons-region",
    version=ENGLAND_FCERM_SCHEMA_VERSION,
    source_field=ONS_REGION_FIELD,
    normalization="strip_casefold",
    known_values=frozenset({"north east", "north west", "south east", "south west"}),
)
ENGLAND_FCERM_RFCC_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="england-fcerm-rfcc",
    version=ENGLAND_FCERM_SCHEMA_VERSION,
    source_field=RFCC_FIELD,
    normalization="strip_casefold",
    known_values=frozenset({"northumbria", "north west", "south west", "thames"}),
)

ENGLAND_FCERM_SCHEMES_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    ENGLAND_FCERM_PROJECT_TYPE_TAXONOMY,
    ENGLAND_FCERM_RISK_SOURCE_TAXONOMY,
    ENGLAND_FCERM_ONS_REGION_TAXONOMY,
    ENGLAND_FCERM_RFCC_TAXONOMY,
)
