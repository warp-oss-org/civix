"""Source-field schema and taxonomy constants for France BAAC fixture rows."""

from __future__ import annotations

from typing import Final

from civix.core.drift import JsonFieldKind, SchemaFieldSpec, SourceSchemaSpec, TaxonomySpec
from civix.domains.transportation_safety.adapters.sources.fr.baac.adapter import BAAC_RELEASE

_STRING: Final[tuple[JsonFieldKind, ...]] = (JsonFieldKind.STRING,)

BAAC_CHARACTERISTICS_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="baac-caracteristiques-raw",
    version=BAAC_RELEASE,
    fields=(
        SchemaFieldSpec(name="Num_Acc", kinds=_STRING),
        SchemaFieldSpec(name="jour", kinds=_STRING),
        SchemaFieldSpec(name="mois", kinds=_STRING),
        SchemaFieldSpec(name="an", kinds=_STRING),
        SchemaFieldSpec(name="hrmn", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="lum", kinds=_STRING),
        SchemaFieldSpec(name="agg", kinds=_STRING),
        SchemaFieldSpec(name="int", kinds=_STRING),
        SchemaFieldSpec(name="atm", kinds=_STRING),
        SchemaFieldSpec(name="col", kinds=_STRING),
        SchemaFieldSpec(name="adr", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="lat", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="long", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="dep", kinds=_STRING),
        SchemaFieldSpec(name="com", kinds=_STRING),
    ),
)

BAAC_VEHICLES_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="baac-vehicules-raw",
    version=BAAC_RELEASE,
    fields=(
        SchemaFieldSpec(name="Num_Acc", kinds=_STRING),
        SchemaFieldSpec(name="id_vehicule", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="num_veh", kinds=_STRING),
        SchemaFieldSpec(name="senc", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="catv", kinds=_STRING),
        SchemaFieldSpec(name="obs", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="obsm", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="choc", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="manv", kinds=_STRING),
        SchemaFieldSpec(name="motor", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="occutc", kinds=_STRING, nullable=True),
    ),
)

BAAC_USERS_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="baac-usagers-raw",
    version=BAAC_RELEASE,
    fields=(
        SchemaFieldSpec(name="Num_Acc", kinds=_STRING),
        SchemaFieldSpec(name="id_usager", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="id_vehicule", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="num_veh", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="place", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="catu", kinds=_STRING),
        SchemaFieldSpec(name="grav", kinds=_STRING),
        SchemaFieldSpec(name="an_nais", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="secu1", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="secu2", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="secu3", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="locp", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="actp", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="etatp", kinds=_STRING, nullable=True),
    ),
)

BAAC_LIGHT_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="baac-lum",
    version=BAAC_RELEASE,
    source_field="lum",
    known_values=frozenset({"1", "5"}),
)
BAAC_WEATHER_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="baac-atm",
    version=BAAC_RELEASE,
    source_field="atm",
    known_values=frozenset({"1", "2"}),
)
BAAC_COLLISION_TYPE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="baac-col",
    version=BAAC_RELEASE,
    source_field="col",
    known_values=frozenset({"3", "6"}),
)
BAAC_INTERSECTION_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="baac-int",
    version=BAAC_RELEASE,
    source_field="int",
    known_values=frozenset({"1", "2", "3", "4", "5", "6", "7", "8", "9"}),
)
BAAC_VEHICLE_CATEGORY_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="baac-catv",
    version=BAAC_RELEASE,
    source_field="catv",
    known_values=frozenset({"1", "7", "30"}),
)
BAAC_VEHICLE_MANOEUVRE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="baac-manv",
    version=BAAC_RELEASE,
    source_field="manv",
    known_values=frozenset({"1", "13", "15"}),
)
BAAC_USER_CATEGORY_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="baac-catu",
    version=BAAC_RELEASE,
    source_field="catu",
    known_values=frozenset({"1", "2", "3"}),
)
BAAC_INJURY_SEVERITY_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="baac-grav",
    version=BAAC_RELEASE,
    source_field="grav",
    known_values=frozenset({"1", "2", "3", "4"}),
)
BAAC_SAFETY_EQUIPMENT_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="baac-secu1",
    version=BAAC_RELEASE,
    source_field="secu1",
    known_values=frozenset({"1", "2", "8"}),
)
BAAC_PEDESTRIAN_ACTION_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="baac-actp",
    version=BAAC_RELEASE,
    source_field="actp",
    known_values=frozenset({"0", "1", "3"}),
)
BAAC_POSITION_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="baac-place",
    version=BAAC_RELEASE,
    source_field="place",
    known_values=frozenset({"1", "2"}),
)

BAAC_CHARACTERISTICS_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    BAAC_LIGHT_TAXONOMY,
    BAAC_WEATHER_TAXONOMY,
    BAAC_COLLISION_TYPE_TAXONOMY,
    BAAC_INTERSECTION_TAXONOMY,
)
BAAC_VEHICLES_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    BAAC_VEHICLE_CATEGORY_TAXONOMY,
    BAAC_VEHICLE_MANOEUVRE_TAXONOMY,
)
BAAC_USERS_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    BAAC_USER_CATEGORY_TAXONOMY,
    BAAC_INJURY_SEVERITY_TAXONOMY,
    BAAC_SAFETY_EQUIPMENT_TAXONOMY,
    BAAC_PEDESTRIAN_ACTION_TAXONOMY,
    BAAC_POSITION_TAXONOMY,
)
BAAC_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    BAAC_CHARACTERISTICS_TAXONOMIES + BAAC_VEHICLES_TAXONOMIES + BAAC_USERS_TAXONOMIES
)
