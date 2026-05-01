"""Source-field schema and taxonomy constants for Chicago vehicle rows."""

from __future__ import annotations

from typing import Final

from civix.core.drift import JsonFieldKind, SchemaFieldSpec, SourceSchemaSpec, TaxonomySpec

CHICAGO_VEHICLES_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="chicago-traffic-vehicles-raw",
    version="2026-05-01",
    fields=(
        SchemaFieldSpec(
            name="crash_unit_id",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
        ),
        SchemaFieldSpec(name="crash_record_id", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="crash_date", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(
            name="unit_no",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
        ),
        SchemaFieldSpec(name="unit_type", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(
            name="num_passengers",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
            nullable=True,
        ),
        SchemaFieldSpec(name="vehicle_id", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="vehicle_type", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(
            name="occupant_cnt",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
            nullable=True,
        ),
        SchemaFieldSpec(name="travel_direction", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="maneuver", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="first_contact_point", kinds=(JsonFieldKind.STRING,), nullable=True),
    ),
)

CHICAGO_UNIT_TYPE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="chicago-traffic-vehicles-unit-type",
    version="2026-05-01",
    source_field="unit_type",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "bicycle",
            "disabled vehicle",
            "driver",
            "driverless",
            "equestrian",
            "non-contact vehicle",
            "non-motor vehicle",
            "parked",
            "pedestrian",
        }
    ),
)

CHICAGO_VEHICLE_TYPE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="chicago-traffic-vehicles-vehicle-type",
    version="2026-05-01",
    source_field="vehicle_type",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "bus over 15 pass.",
            "bus up to 15 pass.",
            "motorcycle",
            "passenger",
            "pickup",
            "sport utility vehicle (suv)",
            "truck - single unit",
            "unknown/na",
        }
    ),
)

CHICAGO_TRAVEL_DIRECTION_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="chicago-traffic-vehicles-travel-direction",
    version="2026-05-01",
    source_field="travel_direction",
    normalization="strip_casefold",
    known_values=frozenset({"e", "n", "ne", "nw", "s", "se", "sw", "unknown", "w"}),
)

CHICAGO_MANEUVER_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="chicago-traffic-vehicles-maneuver",
    version="2026-05-01",
    source_field="maneuver",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "backing",
            "changing lanes",
            "passing/overtaking",
            "slowing/stopped in traffic",
            "straight ahead",
            "turning left",
            "turning right",
            "unknown/na",
        }
    ),
)

CHICAGO_FIRST_CONTACT_POINT_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="chicago-traffic-vehicles-first-contact-point",
    version="2026-05-01",
    source_field="first_contact_point",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "front",
            "front-left",
            "front-right",
            "none",
            "rear",
            "rear-left",
            "rear-right",
            "total (all areas)",
            "unknown",
        }
    ),
)

CHICAGO_VEHICLES_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    CHICAGO_UNIT_TYPE_TAXONOMY,
    CHICAGO_VEHICLE_TYPE_TAXONOMY,
    CHICAGO_TRAVEL_DIRECTION_TAXONOMY,
    CHICAGO_MANEUVER_TAXONOMY,
    CHICAGO_FIRST_CONTACT_POINT_TAXONOMY,
)
