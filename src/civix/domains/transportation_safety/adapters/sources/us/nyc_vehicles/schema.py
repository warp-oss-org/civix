"""Source-field schema and taxonomy constants for NYC vehicle rows."""

from __future__ import annotations

from typing import Final

from civix.core.drift import JsonFieldKind, SchemaFieldSpec, SourceSchemaSpec, TaxonomySpec

NYC_VEHICLES_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="nyc-motor-vehicle-collisions-vehicles-raw",
    version="2026-05-02",
    fields=(
        SchemaFieldSpec(name="unique_id", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="collision_id", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="crash_date", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="crash_time", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="vehicle_id", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="state_registration", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="vehicle_type", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="vehicle_make", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="vehicle_model", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="vehicle_year", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="travel_direction", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(
            name="vehicle_occupants",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
            nullable=True,
        ),
        SchemaFieldSpec(name="driver_sex", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(
            name="driver_license_status",
            kinds=(JsonFieldKind.STRING,),
            nullable=True,
        ),
        SchemaFieldSpec(
            name="driver_license_jurisdiction",
            kinds=(JsonFieldKind.STRING,),
            nullable=True,
        ),
        SchemaFieldSpec(name="pre_crash", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="point_of_impact", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="vehicle_damage", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="vehicle_damage_1", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="vehicle_damage_2", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="vehicle_damage_3", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(
            name="public_property_damage",
            kinds=(JsonFieldKind.STRING,),
            nullable=True,
        ),
        SchemaFieldSpec(
            name="public_property_damage_type",
            kinds=(JsonFieldKind.STRING,),
            nullable=True,
        ),
        SchemaFieldSpec(name="contributing_factor_1", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="contributing_factor_2", kinds=(JsonFieldKind.STRING,), nullable=True),
    ),
)

NYC_VEHICLE_TYPE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-vehicles-vehicle-type",
    version="2026-05-02",
    source_field="vehicle_type",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "4 dr sedan",
            "bike",
            "box truck",
            "bus",
            "motorcycle",
            "passenger vehicle",
            "pick-up truck",
            "sedan",
            "station wagon/sport utility vehicle",
            "taxi",
            "unknown",
            "van",
        }
    ),
)

NYC_TRAVEL_DIRECTION_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-vehicles-travel-direction",
    version="2026-05-02",
    source_field="travel_direction",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "-",
            "east",
            "n",
            "north",
            "northeast",
            "northwest",
            "s",
            "south",
            "southeast",
            "southwest",
            "u",
            "unknown",
            "w",
            "west",
        }
    ),
)

NYC_PRE_CRASH_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-vehicles-pre-crash",
    version="2026-05-02",
    source_field="pre_crash",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "backing",
            "changing lanes",
            "going straight ahead",
            "making left turn",
            "making right turn",
            "parked",
            "slowing or stopping",
            "stopped in traffic",
        }
    ),
)

NYC_VEHICLE_DAMAGE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-vehicles-vehicle-damage",
    version="2026-05-02",
    source_field="vehicle_damage",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "center back end",
            "center front end",
            "left front bumper",
            "left side doors",
            "no damage",
            "other",
            "right front bumper",
            "right rear bumper",
        }
    ),
)

NYC_VEHICLE_CONTRIBUTING_FACTOR_1_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-vehicles-contributing-factor-1",
    version="2026-05-02",
    source_field="contributing_factor_1",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "driver inattention/distraction",
            "failure to yield right-of-way",
            "following too closely",
            "other vehicular",
            "passing or lane usage improper",
            "unspecified",
        }
    ),
)

NYC_VEHICLE_CONTRIBUTING_FACTOR_2_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-vehicles-contributing-factor-2",
    version="2026-05-02",
    source_field="contributing_factor_2",
    normalization="strip_casefold",
    known_values=NYC_VEHICLE_CONTRIBUTING_FACTOR_1_TAXONOMY.known_values,
)

NYC_VEHICLES_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    NYC_VEHICLE_TYPE_TAXONOMY,
    NYC_TRAVEL_DIRECTION_TAXONOMY,
    NYC_PRE_CRASH_TAXONOMY,
    NYC_VEHICLE_DAMAGE_TAXONOMY,
    NYC_VEHICLE_CONTRIBUTING_FACTOR_1_TAXONOMY,
    NYC_VEHICLE_CONTRIBUTING_FACTOR_2_TAXONOMY,
)
