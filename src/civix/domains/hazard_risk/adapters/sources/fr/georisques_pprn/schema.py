"""Source-field schema and taxonomy constants for Georisques GASPAR PPRN rows."""

from __future__ import annotations

from typing import Final

from civix.core.drift import JsonFieldKind, SchemaFieldSpec, SourceSchemaSpec, TaxonomySpec

GEORISQUES_PPRN_SCHEMA_VERSION: Final[str] = "2026-05-03"
GEORISQUES_PPRN_TAXONOMY_VERSION: Final[str] = "2026-05-03"

GEORISQUES_PPRN_FIELDS: Final[tuple[str, ...]] = (
    "CODE MODELE",
    "LIBELLE MODELE",
    # GASPAR publishes this header misspelled; keep the source spelling for drift checks.
    "CODE PROECEDURE",
    "LIBELLE PROCEDURE",
    "LIBELLE ORGANISME",
    "BASSIN RISQUE",
    "BASSINS HYDROGRAPHIQUES",
    "COURS EAU",
    "CODE INSEE REGION",
    "NOM REGION",
    "CODE INSEE DEPARTEMENT",
    "NOM DEPARTEMENT",
    "CODE INSEE COMMUNE",
    "NOM COMMUNE",
    "CODE RISQUE 1",
    "LIBELLE RISQUE 1",
    "CODE RISQUE 2",
    "LIBELLE RISQUE 2",
    # GASPAR repeats `CODE RISQUE 2` here; the adapter renames the second copy.
    "CODE RISQUE 3",
    "LIBELLE RISQUE 3",
    "PROCEDURE REVISANTE",
    "CODES REVISES",
    "PROCEDURE REVISEE",
    "CODES REVISANTS",
    "PROGRAMMATION_DEBUT",
    "PROGRAMMATION_FIN",
    "MONTAGE_DEBUT",
    "MONTAGE_FIN",
    "PRESCRIPTION",
    "ETUDES_HYDR_DEBUT",
    "ETUDES_HYDR_FIN",
    "CARTE_ALEAS_DEBUT",
    "CARTE_ALEAS_FIN",
    "CARTE_ENJEUX_DEBUT",
    "CARTE_ENJEUX_FIN",
    "ZONAGE_REGL_DEBUT",
    "ZONAGE_REGL_FIN",
    "REG_ET_NOTE_PRES_DEBUT",
    "REG_ET_NOTE_PRES_FIN",
    "CONCERTATION_DEBUT",
    "CONCERTATION_FIN",
    "CONSULTATION_DEBUT",
    "CONSULTATION_FIN",
    "CONSULT_SERV_DEBUT",
    "CONSULT_SERV_FIN",
    "ENQUETE_PUBL_DEBUT",
    "ENQUETE_PUBL_FIN",
    "ANNEX_PLU",
    "PROROGATION",
    "APPLIC_ANTIC",
    "DEPRESCRIPTION",
    "APPROBATION",
    "ANNULATION",
    "ABROGATION",
    "LIBELLE ETAT",
    "DATE ETAT",
    "LIBELLE SOUS-ETAT",
    "DATE SOUS-ETAT",
    "DATE DERNIERE MISE A JOUR",
)

_STRING: Final[tuple[JsonFieldKind, ...]] = (JsonFieldKind.STRING,)

GEORISQUES_PPRN_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="georisques-gaspar-pprn-raw",
    version=GEORISQUES_PPRN_SCHEMA_VERSION,
    fields=tuple(SchemaFieldSpec(name=field, kinds=_STRING) for field in GEORISQUES_PPRN_FIELDS),
)

GEORISQUES_PPRN_MODEL_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="georisques-pprn-model",
    version=GEORISQUES_PPRN_TAXONOMY_VERSION,
    source_field="CODE MODELE",
    normalization="exact",
    known_values=frozenset({"PPRN-I", "PPRN-Multi", "PPRN-IF", "PPRN-Mvt"}),
)
GEORISQUES_PPRN_RISK_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="georisques-pprn-risk",
    version=GEORISQUES_PPRN_TAXONOMY_VERSION,
    source_field="CODE RISQUE 3",
    normalization="exact",
    known_values=frozenset({"110", "123", "160"}),
)
GEORISQUES_PPRN_RISK_FAMILY_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="georisques-pprn-risk-family",
    version=GEORISQUES_PPRN_TAXONOMY_VERSION,
    source_field="CODE RISQUE 2",
    normalization="exact",
    known_values=frozenset({"11", "12", "16"}),
)
GEORISQUES_PPRN_STATE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="georisques-pprn-state",
    version=GEORISQUES_PPRN_TAXONOMY_VERSION,
    source_field="LIBELLE ETAT",
    normalization="strip_casefold",
    known_values=frozenset({"opposable", "prescrit", "caduque"}),
)
GEORISQUES_PPRN_SUBSTATE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="georisques-pprn-substate",
    version=GEORISQUES_PPRN_TAXONOMY_VERSION,
    source_field="LIBELLE SOUS-ETAT",
    normalization="strip_casefold",
    known_values=frozenset({"approuvé", "anticipé", "prescrit", "déprescrit", "abrogé"}),
)
GEORISQUES_PPRN_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    GEORISQUES_PPRN_MODEL_TAXONOMY,
    GEORISQUES_PPRN_RISK_FAMILY_TAXONOMY,
    GEORISQUES_PPRN_RISK_TAXONOMY,
    GEORISQUES_PPRN_STATE_TAXONOMY,
    GEORISQUES_PPRN_SUBSTATE_TAXONOMY,
)
