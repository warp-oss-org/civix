"""Source-field schema and taxonomy constants for FEMA NFHL fixture rows."""

from __future__ import annotations

from typing import Final

from civix.core.drift import JsonFieldKind, SchemaFieldSpec, SourceSchemaSpec, TaxonomySpec

FEMA_NFHL_SCHEMA_VERSION: Final[str] = "2026-05-03"
FEMA_NFHL_TAXONOMY_VERSION: Final[str] = "2026-05-03"

# Keep this wider than the mapper's normalized fields on purpose. The
# extra regulatory and technical attributes are fixture-backed schema
# sentinels for future NFHL mapping work; mapper tests pin the expected
# unmapped set so it cannot grow silently.
FEMA_NFHL_FLOOD_HAZARD_ZONES_OUT_FIELDS: Final[tuple[str, ...]] = (
    "OBJECTID",
    "DFIRM_ID",
    "FLD_AR_ID",
    "STUDY_TYP",
    "FLD_ZONE",
    "ZONE_SUBTY",
    "SFHA_TF",
    "STATIC_BFE",
    "V_DATUM",
    "DEPTH",
    "LEN_UNIT",
    "VELOCITY",
    "VEL_UNIT",
    "AR_REVERT",
    "AR_SUBTRV",
    "BFE_REVERT",
    "DEP_REVERT",
    "DUAL_ZONE",
    "SOURCE_CIT",
    "GFID",
    "GlobalID",
)

_STRING: Final[tuple[JsonFieldKind, ...]] = (JsonFieldKind.STRING,)
_NUMBER: Final[tuple[JsonFieldKind, ...]] = (JsonFieldKind.NUMBER,)

FEMA_NFHL_FLOOD_HAZARD_ZONES_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="fema-nfhl-flood-hazard-zones-raw",
    version=FEMA_NFHL_SCHEMA_VERSION,
    fields=(
        SchemaFieldSpec(name="OBJECTID", kinds=_NUMBER),
        SchemaFieldSpec(name="DFIRM_ID", kinds=_STRING),
        SchemaFieldSpec(name="FLD_AR_ID", kinds=_STRING),
        SchemaFieldSpec(name="STUDY_TYP", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="FLD_ZONE", kinds=_STRING),
        SchemaFieldSpec(name="ZONE_SUBTY", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="SFHA_TF", kinds=_STRING),
        SchemaFieldSpec(name="STATIC_BFE", kinds=_NUMBER, nullable=True),
        SchemaFieldSpec(name="V_DATUM", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="DEPTH", kinds=_NUMBER, nullable=True),
        SchemaFieldSpec(name="LEN_UNIT", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="VELOCITY", kinds=_NUMBER, nullable=True),
        SchemaFieldSpec(name="VEL_UNIT", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="AR_REVERT", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="AR_SUBTRV", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="BFE_REVERT", kinds=_NUMBER, nullable=True),
        SchemaFieldSpec(name="DEP_REVERT", kinds=_NUMBER, nullable=True),
        SchemaFieldSpec(name="DUAL_ZONE", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="SOURCE_CIT", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="GFID", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="GlobalID", kinds=_STRING, nullable=True),
    ),
)

# Fixture-backed baseline only. Broader production NFHL vocabularies
# should be added deliberately once a later sprint consumes wider samples.
FEMA_NFHL_ZONE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="fema-nfhl-zone",
    version=FEMA_NFHL_TAXONOMY_VERSION,
    source_field="FLD_ZONE",
    normalization="strip_casefold",
    known_values=frozenset({"ae", "x"}),
)
FEMA_NFHL_ZONE_SUBTYPE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="fema-nfhl-zone-subtype",
    version=FEMA_NFHL_TAXONOMY_VERSION,
    source_field="ZONE_SUBTY",
    normalization="strip_casefold",
    known_values=frozenset({"floodway"}),
)
FEMA_NFHL_SFHA_FLAG_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="fema-nfhl-sfha-flag",
    version=FEMA_NFHL_TAXONOMY_VERSION,
    source_field="SFHA_TF",
    normalization="strip_casefold",
    known_values=frozenset({"y", "n"}),
)
FEMA_NFHL_FLOOD_HAZARD_ZONES_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    FEMA_NFHL_ZONE_TAXONOMY,
    FEMA_NFHL_ZONE_SUBTYPE_TAXONOMY,
    FEMA_NFHL_SFHA_FLAG_TAXONOMY,
)
