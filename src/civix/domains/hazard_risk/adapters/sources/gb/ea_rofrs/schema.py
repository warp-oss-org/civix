"""Source-field schema and taxonomy constants for Environment Agency RoFRS fixtures."""

from __future__ import annotations

from typing import Final

from civix.core.drift import JsonFieldKind, SchemaFieldSpec, SourceSchemaSpec, TaxonomySpec

EA_ROFRS_SCHEMA_VERSION: Final[str] = "2026-05-03"
EA_ROFRS_TAXONOMY_VERSION: Final[str] = "2026-05-03"

EA_ROFRS_FIELDS: Final[tuple[str, ...]] = (
    "risk_area_id",
    "risk_area_name",
    "risk_band",
    "flood_source",
    "publication_date",
    "product_version",
    "product_url",
    "geometry_uri",
    "geometry_layer",
    "geometry_id",
    "source_crs",
)

_STRING: Final[tuple[JsonFieldKind, ...]] = (JsonFieldKind.STRING,)

EA_ROFRS_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="ea-rofrs-raw",
    version=EA_ROFRS_SCHEMA_VERSION,
    fields=tuple(SchemaFieldSpec(name=field, kinds=_STRING) for field in EA_ROFRS_FIELDS),
)
EA_ROFRS_RISK_BAND_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="ea-rofrs-risk-band",
    version=EA_ROFRS_TAXONOMY_VERSION,
    source_field="risk_band",
    normalization="strip_casefold",
    known_values=frozenset({"high", "medium", "low", "very low"}),
)
EA_ROFRS_FLOOD_SOURCE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="ea-rofrs-flood-source",
    version=EA_ROFRS_TAXONOMY_VERSION,
    source_field="flood_source",
    normalization="strip_casefold",
    known_values=frozenset({"rivers and sea", "rivers", "sea", "coastal"}),
)
EA_ROFRS_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    EA_ROFRS_RISK_BAND_TAXONOMY,
    EA_ROFRS_FLOOD_SOURCE_TAXONOMY,
)
