"""Source-field schema and taxonomy constants for Canada DMAF fixture rows."""

from __future__ import annotations

from typing import Final

from civix.core.drift import JsonFieldKind, SchemaFieldSpec, SourceSchemaSpec, TaxonomySpec

CANADA_DMAF_SCHEMA_VERSION: Final[str] = "2026-05-03"

ADAPTER_CONSUMED_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "projectNumber",
    }
)

CANADA_DMAF_PROJECTS_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="canada-dmaf-projects-raw",
    version=CANADA_DMAF_SCHEMA_VERSION,
    fields=(
        SchemaFieldSpec(name="projectNumber", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="projectTitle_en", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="projectTitle_fr", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="programCode_en", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="programCode_fr", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="program_en", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="program_fr", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="category_en", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="category_fr", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="location_en", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="location_fr", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="region", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="approvedDate", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="constructionStartDate", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="constructionEndDate", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="federalContribution", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="totalEligibleCost", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="ultimateRecipient_en", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="ultimateRecipient_fr", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="forecastedConstructionStartDate", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="forecastedConstructionEndDate", kinds=(JsonFieldKind.STRING,)),
    ),
)

CANADA_DMAF_PROGRAM_CODE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="canada-dmaf-program-code",
    version=CANADA_DMAF_SCHEMA_VERSION,
    source_field="programCode_en",
    normalization="strip_casefold",
    known_values=frozenset({"dmaf"}),
)
CANADA_DMAF_PROGRAM_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="canada-dmaf-program",
    version=CANADA_DMAF_SCHEMA_VERSION,
    source_field="program_en",
    normalization="strip_casefold",
    known_values=frozenset({"disaster mitigation and adaptation fund"}),
)
CANADA_DMAF_CATEGORY_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="canada-dmaf-category",
    version=CANADA_DMAF_SCHEMA_VERSION,
    source_field="category_en",
    normalization="strip_casefold",
    known_values=frozenset({"disaster mitigation", "green infrastructure"}),
)
CANADA_DMAF_REGION_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="canada-dmaf-region",
    version=CANADA_DMAF_SCHEMA_VERSION,
    source_field="region",
    normalization="strip_casefold",
    known_values=frozenset({"bc", "nb"}),
)

CANADA_DMAF_PROJECTS_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    CANADA_DMAF_PROGRAM_CODE_TAXONOMY,
    CANADA_DMAF_PROGRAM_TAXONOMY,
    CANADA_DMAF_CATEGORY_TAXONOMY,
    CANADA_DMAF_REGION_TAXONOMY,
)
