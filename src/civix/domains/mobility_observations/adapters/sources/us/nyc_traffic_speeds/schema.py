"""Source-field schema and taxonomy constants for NYC DOT traffic speeds."""

from __future__ import annotations

from typing import Final

from civix.core.drift import JsonFieldKind, SchemaFieldSpec, SourceSchemaSpec, TaxonomySpec

NYC_TRAFFIC_SPEEDS_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="nyc-traffic-speeds-raw",
    version="2026-05-02",
    fields=(
        SchemaFieldSpec(name="ID", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="SPEED", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="TRAVEL_TIME", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="STATUS", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="DATA_AS_OF", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="LINK_ID", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="LINK_POINTS", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="ENCODED_POLY_LINE", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(
            name="ENCODED_POLY_LINE_LVLS",
            kinds=(JsonFieldKind.STRING,),
            nullable=True,
        ),
        SchemaFieldSpec(name="OWNER", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="TRANSCOM_ID", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="BOROUGH", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="LINK_NAME", kinds=(JsonFieldKind.STRING,), nullable=True),
    ),
)

NYC_TRAFFIC_SPEEDS_BOROUGH_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-traffic-speeds-borough",
    version="2026-05-02",
    source_field="BOROUGH",
    normalization="strip_casefold",
    known_values=frozenset({"bronx", "brooklyn", "manhattan", "queens", "staten island"}),
)

NYC_TRAFFIC_SPEEDS_STATUS_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-traffic-speeds-status",
    version="2026-05-02",
    source_field="STATUS",
    normalization="strip_casefold",
    known_values=frozenset({"0", "1", "2"}),
)

NYC_TRAFFIC_SPEEDS_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    NYC_TRAFFIC_SPEEDS_BOROUGH_TAXONOMY,
    NYC_TRAFFIC_SPEEDS_STATUS_TAXONOMY,
)
