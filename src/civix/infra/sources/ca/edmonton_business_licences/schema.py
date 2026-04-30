"""Source-field schema and taxonomy constants for Edmonton business licences."""

from __future__ import annotations

from typing import Final

from civix.core.drift import (
    JsonFieldKind,
    SchemaFieldSpec,
    SourceSchemaSpec,
    TaxonomySpec,
)

ADAPTER_CONSUMED_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "externalid",  # -> record.source_record_id
    }
)

EDMONTON_BUSINESS_LICENCES_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="edmonton-business-licences-raw",
    version="2026-04-30",
    fields=(
        SchemaFieldSpec(name="business_licence_category", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="business_name", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="business_address", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="externalid", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="most_recent_issue_date", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="expiry_date", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(
            name="business_improvement_area",
            kinds=(JsonFieldKind.STRING,),
            nullable=True,
        ),
        SchemaFieldSpec(name="neighbourhood_id", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="neighbourhood", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="ward", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="latitude", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="longitude", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="location", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="count", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="geometry_point", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="originalissuedate", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="licenceduration", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="licencetype", kinds=(JsonFieldKind.STRING,)),
    ),
)


EDMONTON_LICENCE_TYPE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="edmonton-business-licence-type",
    version="2026-04-30",
    source_field="licencetype",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "commercial",
            "home based",
            "non-resident",
            "massage practitioner",
            "adult services",
        }
    ),
)


EDMONTON_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (EDMONTON_LICENCE_TYPE_TAXONOMY,)
