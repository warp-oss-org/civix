"""Source-field schema and taxonomy constants for France TMJA road-traffic rows."""

from __future__ import annotations

from typing import Final

from civix.core.drift import JsonFieldKind, SchemaFieldSpec, SourceSchemaSpec, TaxonomySpec

_VERSION: Final[str] = "2025-08-18"
_STRING: Final[tuple[JsonFieldKind, ...]] = (JsonFieldKind.STRING,)

FR_TMJA_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="fr-tmja-rrnc-2024-raw",
    version=_VERSION,
    fields=(
        SchemaFieldSpec(name="dateReferentiel", kinds=_STRING),
        SchemaFieldSpec(name="route", kinds=_STRING),
        SchemaFieldSpec(name="longueur", kinds=_STRING),
        SchemaFieldSpec(name="prD", kinds=_STRING),
        SchemaFieldSpec(name="depPrD", kinds=_STRING),
        SchemaFieldSpec(name="concessionPrD", kinds=_STRING),
        SchemaFieldSpec(name="absD", kinds=_STRING),
        SchemaFieldSpec(name="cumulD", kinds=_STRING),
        SchemaFieldSpec(name="xD", kinds=_STRING),
        SchemaFieldSpec(name="yD", kinds=_STRING),
        SchemaFieldSpec(name="zD", kinds=_STRING),
        SchemaFieldSpec(name="prF", kinds=_STRING),
        SchemaFieldSpec(name="depPrF", kinds=_STRING),
        SchemaFieldSpec(name="concessionPrF", kinds=_STRING),
        SchemaFieldSpec(name="absF", kinds=_STRING),
        SchemaFieldSpec(name="cumulF", kinds=_STRING),
        SchemaFieldSpec(name="xF", kinds=_STRING),
        SchemaFieldSpec(name="yF", kinds=_STRING),
        SchemaFieldSpec(name="zF", kinds=_STRING),
        SchemaFieldSpec(name="cote", kinds=_STRING),
        SchemaFieldSpec(name="anneeMesureTrafic", kinds=_STRING),
        SchemaFieldSpec(name="typeComptageTrafic", kinds=_STRING),
        SchemaFieldSpec(name="typeComptageTrafic_lib", kinds=_STRING),
        SchemaFieldSpec(name="TMJA", kinds=_STRING),
        SchemaFieldSpec(name="ratio_PL", kinds=_STRING),
    ),
)

FR_TMJA_TYPE_COMPTAGE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="fr-tmja-type-comptage-trafic",
    version=_VERSION,
    source_field="typeComptageTrafic",
    normalization="exact",
    known_values=frozenset({"1"}),
)
FR_TMJA_TYPE_COMPTAGE_LABEL_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="fr-tmja-type-comptage-trafic-lib",
    version=_VERSION,
    source_field="typeComptageTrafic_lib",
    normalization="exact",
    known_values=frozenset({"Permanent horaire"}),
)
FR_TMJA_CONCESSION_PR_D_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="fr-tmja-concession-pr-d",
    version=_VERSION,
    source_field="concessionPrD",
    normalization="exact",
    known_values=frozenset({"C", "N"}),
)
FR_TMJA_CONCESSION_PR_F_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="fr-tmja-concession-pr-f",
    version=_VERSION,
    source_field="concessionPrF",
    normalization="exact",
    known_values=frozenset({"C", "N"}),
)
FR_TMJA_COTE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="fr-tmja-cote",
    version=_VERSION,
    source_field="cote",
    normalization="exact",
    known_values=frozenset({"I", "D", "G"}),
)

FR_TMJA_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    FR_TMJA_TYPE_COMPTAGE_TAXONOMY,
    FR_TMJA_TYPE_COMPTAGE_LABEL_TAXONOMY,
    FR_TMJA_CONCESSION_PR_D_TAXONOMY,
    FR_TMJA_CONCESSION_PR_F_TAXONOMY,
    FR_TMJA_COTE_TAXONOMY,
)
