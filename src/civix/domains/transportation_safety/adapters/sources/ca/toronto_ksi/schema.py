"""Source-field schema and taxonomy constants for Toronto KSI rows."""

from __future__ import annotations

from typing import Final

from civix.core.drift import JsonFieldKind, SchemaFieldSpec, SourceSchemaSpec, TaxonomySpec

TORONTO_KSI_SCHEMA_VERSION: Final[str] = "2026-05-01"

TORONTO_KSI_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="toronto-ksi-raw",
    version=TORONTO_KSI_SCHEMA_VERSION,
    fields=(
        SchemaFieldSpec(name="_id", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(
            name="collision_id",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
        ),
        SchemaFieldSpec(name="accdate", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="stname1", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="stname2", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="stname3", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(
            name="per_inv",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
            nullable=True,
        ),
        SchemaFieldSpec(name="acclass", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="accloc", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="traffictl", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="impactype", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="visible", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="light", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="rdsfcond", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="road_class", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="failtorem", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(
            name="longitude",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
            nullable=True,
        ),
        SchemaFieldSpec(
            name="latitude",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
            nullable=True,
        ),
        SchemaFieldSpec(name="wardname", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="division", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="neighbourhood", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(
            name="veh_no",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
            nullable=True,
        ),
        SchemaFieldSpec(name="vehtype", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="initdir", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(
            name="per_no",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
        ),
        SchemaFieldSpec(
            name="invage", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING), nullable=True
        ),
        SchemaFieldSpec(name="injury", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="safequip", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="drivact", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="drivcond", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="pedact", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="pedcond", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="manoeuvre", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="pedtype", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="cyclistype", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="cycact", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="cyccond", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="road_user", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(
            name="fatal_no",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
            nullable=True,
        ),
    ),
)

TORONTO_ACCLASS_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="toronto-ksi-acclass",
    version=TORONTO_KSI_SCHEMA_VERSION,
    source_field="acclass",
    normalization="strip_casefold",
    known_values=frozenset({"fatal", "non-fatal injury"}),
)

TORONTO_ROAD_USER_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="toronto-ksi-road-user",
    version=TORONTO_KSI_SCHEMA_VERSION,
    source_field="road_user",
    normalization="strip_casefold",
    known_values=frozenset({"cyclist", "driver", "motorcyclist", "passenger", "pedestrian"}),
)

TORONTO_VEHTYPE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="toronto-ksi-vehtype",
    version=TORONTO_KSI_SCHEMA_VERSION,
    source_field="vehtype",
    normalization="strip_casefold",
    known_values=frozenset({"automobile, station wagon", "bicycle", "motorcycle", "truck"}),
)

TORONTO_TRAFFIC_CONTROL_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="toronto-ksi-traffictl",
    version=TORONTO_KSI_SCHEMA_VERSION,
    source_field="traffictl",
    normalization="strip_casefold",
    known_values=frozenset({"no control", "traffic signal"}),
)

TORONTO_ACCIDENT_LOCATION_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="toronto-ksi-accloc",
    version=TORONTO_KSI_SCHEMA_VERSION,
    source_field="accloc",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "at intersection",
            "intersection",
            "intersection related",
            "mid-block",
            "non intersection",
            "non-intersection",
            "private drive",
        }
    ),
)

TORONTO_VISIBLE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="toronto-ksi-visible",
    version=TORONTO_KSI_SCHEMA_VERSION,
    source_field="visible",
    normalization="strip_casefold",
    known_values=frozenset({"clear", "fog", "rain", "snow"}),
)

TORONTO_LIGHT_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="toronto-ksi-light",
    version=TORONTO_KSI_SCHEMA_VERSION,
    source_field="light",
    normalization="strip_casefold",
    known_values=frozenset({"dark", "dark, artificial", "daylight"}),
)

TORONTO_ROAD_SURFACE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="toronto-ksi-rdsfcond",
    version=TORONTO_KSI_SCHEMA_VERSION,
    source_field="rdsfcond",
    normalization="strip_casefold",
    known_values=frozenset({"dry", "wet"}),
)

TORONTO_INJURY_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="toronto-ksi-injury",
    version=TORONTO_KSI_SCHEMA_VERSION,
    source_field="injury",
    normalization="strip_casefold",
    known_values=frozenset({"fatal", "major", "minor", "none"}),
)

TORONTO_KSI_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    TORONTO_ACCLASS_TAXONOMY,
    TORONTO_ROAD_USER_TAXONOMY,
    TORONTO_VEHTYPE_TAXONOMY,
    TORONTO_TRAFFIC_CONTROL_TAXONOMY,
    TORONTO_ACCIDENT_LOCATION_TAXONOMY,
    TORONTO_VISIBLE_TAXONOMY,
    TORONTO_LIGHT_TAXONOMY,
    TORONTO_ROAD_SURFACE_TAXONOMY,
    TORONTO_INJURY_TAXONOMY,
)
