"""Source-field schema and taxonomy constants for NRCan FSI fixtures."""

from __future__ import annotations

from typing import Final

from civix.core.drift import JsonFieldKind, SchemaFieldSpec, SourceSchemaSpec, TaxonomySpec

NRCAN_FSI_SCHEMA_VERSION: Final[str] = "2026-05-03"
NRCAN_FSI_TAXONOMY_VERSION: Final[str] = "2026-05-03"

NRCAN_FSI_FIELDS: Final[tuple[str, ...]] = (
    "cell_id",
    "community_id",
    "community_name",
    "province_code",
    "susceptibility_rating",
    "susceptibility_score",
    "flood_mechanism",
    "publication_version",
    "publication_date",
    "methodology_url",
    "geometry_uri",
    "geometry_layer",
    "geometry_id",
    "source_crs",
)

_STRING: Final[tuple[JsonFieldKind, ...]] = (JsonFieldKind.STRING,)
_NUMBER: Final[tuple[JsonFieldKind, ...]] = (JsonFieldKind.NUMBER,)

NRCAN_FSI_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="nrcan-fsi-raw",
    version=NRCAN_FSI_SCHEMA_VERSION,
    fields=(
        SchemaFieldSpec(name="cell_id", kinds=_STRING),
        SchemaFieldSpec(name="community_id", kinds=_STRING),
        SchemaFieldSpec(name="community_name", kinds=_STRING),
        SchemaFieldSpec(name="province_code", kinds=_STRING),
        SchemaFieldSpec(name="susceptibility_rating", kinds=_STRING),
        SchemaFieldSpec(name="susceptibility_score", kinds=_NUMBER),
        SchemaFieldSpec(name="flood_mechanism", kinds=_STRING),
        SchemaFieldSpec(name="publication_version", kinds=_STRING),
        SchemaFieldSpec(name="publication_date", kinds=_STRING),
        SchemaFieldSpec(name="methodology_url", kinds=_STRING),
        SchemaFieldSpec(name="geometry_uri", kinds=_STRING),
        SchemaFieldSpec(name="geometry_layer", kinds=_STRING),
        SchemaFieldSpec(name="geometry_id", kinds=_STRING),
        SchemaFieldSpec(name="source_crs", kinds=_STRING),
    ),
)

NRCAN_FSI_RATING_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nrcan-fsi-rating",
    version=NRCAN_FSI_TAXONOMY_VERSION,
    source_field="susceptibility_rating",
    normalization="strip_casefold",
    known_values=frozenset({"low", "moderate", "high", "very high"}),
)
NRCAN_FSI_FLOOD_MECHANISM_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nrcan-fsi-flood-mechanism",
    version=NRCAN_FSI_TAXONOMY_VERSION,
    source_field="flood_mechanism",
    normalization="strip_casefold",
    known_values=frozenset({"flood-prone", "riverine", "coastal", "rainfall", "combined"}),
)
NRCAN_FSI_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    NRCAN_FSI_RATING_TAXONOMY,
    NRCAN_FSI_FLOOD_MECHANISM_TAXONOMY,
)
