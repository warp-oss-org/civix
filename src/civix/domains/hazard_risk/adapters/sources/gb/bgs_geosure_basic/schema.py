"""Source-field schema and taxonomy constants for BGS GeoSure Basic fixtures."""

from __future__ import annotations

from typing import Final

from civix.core.drift import JsonFieldKind, SchemaFieldSpec, SourceSchemaSpec, TaxonomySpec

BGS_GEOSURE_BASIC_SCHEMA_VERSION: Final[str] = "2026-05-03"
BGS_GEOSURE_BASIC_TAXONOMY_VERSION: Final[str] = "2026-05-03"

BGS_GEOSURE_BASIC_FIELDS: Final[tuple[str, ...]] = (
    "hex_id",
    "area_name",
    "country_part",
    "geohazard_theme",
    "susceptibility_rating",
    "susceptibility_score",
    "publication_version",
    "publication_date",
    "product_url",
    "geometry_uri",
    "geometry_layer",
    "geometry_id",
    "source_crs",
)

_STRING: Final[tuple[JsonFieldKind, ...]] = (JsonFieldKind.STRING,)
_NUMBER: Final[tuple[JsonFieldKind, ...]] = (JsonFieldKind.NUMBER,)

BGS_GEOSURE_BASIC_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="bgs-geosure-basic-raw",
    version=BGS_GEOSURE_BASIC_SCHEMA_VERSION,
    fields=(
        SchemaFieldSpec(name="hex_id", kinds=_STRING),
        SchemaFieldSpec(name="area_name", kinds=_STRING),
        SchemaFieldSpec(name="country_part", kinds=_STRING),
        SchemaFieldSpec(name="geohazard_theme", kinds=_STRING),
        SchemaFieldSpec(name="susceptibility_rating", kinds=_STRING),
        SchemaFieldSpec(name="susceptibility_score", kinds=_NUMBER),
        SchemaFieldSpec(name="publication_version", kinds=_STRING),
        SchemaFieldSpec(name="publication_date", kinds=_STRING),
        SchemaFieldSpec(name="product_url", kinds=_STRING),
        SchemaFieldSpec(name="geometry_uri", kinds=_STRING),
        SchemaFieldSpec(name="geometry_layer", kinds=_STRING),
        SchemaFieldSpec(name="geometry_id", kinds=_STRING),
        SchemaFieldSpec(name="source_crs", kinds=_STRING),
    ),
)
BGS_GEOSURE_BASIC_THEME_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="bgs-geosure-basic-theme",
    version=BGS_GEOSURE_BASIC_TAXONOMY_VERSION,
    source_field="geohazard_theme",
    normalization="strip_casefold",
    known_values=frozenset({"combined geohazard", "landslides"}),
)
BGS_GEOSURE_BASIC_RATING_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="bgs-geosure-basic-rating",
    version=BGS_GEOSURE_BASIC_TAXONOMY_VERSION,
    source_field="susceptibility_rating",
    normalization="strip_casefold",
    known_values=frozenset({"negligible - very low", "low", "moderate - high"}),
)
BGS_GEOSURE_BASIC_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    BGS_GEOSURE_BASIC_THEME_TAXONOMY,
    BGS_GEOSURE_BASIC_RATING_TAXONOMY,
)
