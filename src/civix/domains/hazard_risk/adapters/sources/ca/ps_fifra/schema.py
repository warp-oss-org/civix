"""Source-field schema and taxonomy constants for Public Safety Canada FIFRA fixtures."""

from __future__ import annotations

from typing import Final

from civix.core.drift import JsonFieldKind, SchemaFieldSpec, SourceSchemaSpec, TaxonomySpec

PS_FIFRA_SCHEMA_VERSION: Final[str] = "2026-05-03"
PS_FIFRA_TAXONOMY_VERSION: Final[str] = "2026-05-03"

PS_FIFRA_FIELDS: Final[tuple[str, ...]] = (
    "area_id",
    "area_name",
    "province_code",
    "flood_risk_rating",
    "flood_type",
    "publication_version",
    "publication_date",
    "methodology_url",
    "geometry_uri",
    "geometry_layer",
    "geometry_id",
    "source_crs",
)

_STRING: Final[tuple[JsonFieldKind, ...]] = (JsonFieldKind.STRING,)

PS_FIFRA_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="ps-fifra-raw",
    version=PS_FIFRA_SCHEMA_VERSION,
    fields=tuple(SchemaFieldSpec(name=field, kinds=_STRING) for field in PS_FIFRA_FIELDS),
)
PS_FIFRA_RATING_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="ps-fifra-flood-risk-rating",
    version=PS_FIFRA_TAXONOMY_VERSION,
    source_field="flood_risk_rating",
    normalization="strip_casefold",
    known_values=frozenset({"low", "moderate", "high", "extreme"}),
)
PS_FIFRA_FLOOD_TYPE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="ps-fifra-flood-type",
    version=PS_FIFRA_TAXONOMY_VERSION,
    source_field="flood_type",
    normalization="strip_casefold",
    known_values=frozenset({"riverine", "coastal", "rainfall", "combined"}),
)
PS_FIFRA_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    PS_FIFRA_RATING_TAXONOMY,
    PS_FIFRA_FLOOD_TYPE_TAXONOMY,
)
