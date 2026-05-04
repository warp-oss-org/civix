"""Source-field schema and taxonomy constants for STATS19 fixture rows.

Some collision taxonomies are watched for drift before the first mapper uses
them, because road class and road type are core STATS19 context fields.
"""

from __future__ import annotations

from typing import Final

from civix.core.drift import JsonFieldKind, SchemaFieldSpec, SourceSchemaSpec, TaxonomySpec
from civix.domains.transportation_safety.adapters.sources.gb.stats19.adapter import (
    STATS19_RELEASE,
)

_STRING: Final[tuple[JsonFieldKind, ...]] = (JsonFieldKind.STRING,)

STATS19_COLLISIONS_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="stats19-collisions-raw",
    version=STATS19_RELEASE,
    fields=(
        SchemaFieldSpec(name="accident_index", kinds=_STRING),
        SchemaFieldSpec(name="date", kinds=_STRING),
        SchemaFieldSpec(name="time", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="accident_severity", kinds=_STRING),
        SchemaFieldSpec(name="longitude", kinds=_STRING),
        SchemaFieldSpec(name="latitude", kinds=_STRING),
        SchemaFieldSpec(name="first_road_class", kinds=_STRING),
        SchemaFieldSpec(name="second_road_class", kinds=_STRING),
        SchemaFieldSpec(name="road_type", kinds=_STRING),
        SchemaFieldSpec(name="speed_limit", kinds=_STRING),
        SchemaFieldSpec(name="junction_detail", kinds=_STRING),
        SchemaFieldSpec(name="junction_control", kinds=_STRING),
        SchemaFieldSpec(name="light_conditions", kinds=_STRING),
        SchemaFieldSpec(name="weather_conditions", kinds=_STRING),
        SchemaFieldSpec(name="road_surface_conditions", kinds=_STRING),
        SchemaFieldSpec(name="number_of_vehicles", kinds=_STRING),
        SchemaFieldSpec(name="number_of_casualties", kinds=_STRING),
    ),
)

STATS19_VEHICLES_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="stats19-vehicles-raw",
    version=STATS19_RELEASE,
    fields=(
        SchemaFieldSpec(name="accident_index", kinds=_STRING),
        SchemaFieldSpec(name="vehicle_reference", kinds=_STRING),
        SchemaFieldSpec(name="vehicle_type", kinds=_STRING),
        SchemaFieldSpec(name="vehicle_manoeuvre", kinds=_STRING),
        SchemaFieldSpec(name="vehicle_direction_from", kinds=_STRING),
        SchemaFieldSpec(name="vehicle_direction_to", kinds=_STRING),
    ),
)

STATS19_CASUALTIES_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="stats19-casualties-raw",
    version=STATS19_RELEASE,
    fields=(
        SchemaFieldSpec(name="accident_index", kinds=_STRING),
        SchemaFieldSpec(name="vehicle_reference", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="casualty_reference", kinds=_STRING),
        SchemaFieldSpec(name="casualty_class", kinds=_STRING),
        SchemaFieldSpec(name="casualty_severity", kinds=_STRING),
        SchemaFieldSpec(name="casualty_type", kinds=_STRING),
        SchemaFieldSpec(name="age_of_casualty", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="age_band_of_casualty", kinds=_STRING, nullable=True),
    ),
)

STATS19_ACCIDENT_SEVERITY_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="stats19-accident-severity",
    version=STATS19_RELEASE,
    source_field="accident_severity",
    known_values=frozenset({"1", "2", "3"}),
)

STATS19_CASUALTY_SEVERITY_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="stats19-casualty-severity",
    version=STATS19_RELEASE,
    source_field="casualty_severity",
    known_values=frozenset({"1", "2", "3"}),
)

STATS19_CASUALTY_CLASS_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="stats19-casualty-class",
    version=STATS19_RELEASE,
    source_field="casualty_class",
    known_values=frozenset({"1", "2", "3"}),
)

STATS19_CASUALTY_TYPE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="stats19-casualty-type",
    version=STATS19_RELEASE,
    source_field="casualty_type",
    known_values=frozenset({"0", "1", "3", "4", "5", "8", "9", "11", "19", "90"}),
)

STATS19_VEHICLE_TYPE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="stats19-vehicle-type",
    version=STATS19_RELEASE,
    source_field="vehicle_type",
    known_values=frozenset({"1", "2", "3", "4", "5", "8", "9", "10", "11", "19", "90"}),
)

STATS19_VEHICLE_MANOEUVRE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="stats19-vehicle-manoeuvre",
    version=STATS19_RELEASE,
    source_field="vehicle_manoeuvre",
    known_values=frozenset({"1", "2", "3", "4", "5", "7", "9", "10", "13", "16", "18"}),
)

STATS19_VEHICLE_DIRECTION_TO_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="stats19-vehicle-direction-to",
    version=STATS19_RELEASE,
    source_field="vehicle_direction_to",
    known_values=frozenset({"N", "NE", "E", "SE", "S", "SW", "W", "NW"}),
)

STATS19_WEATHER_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="stats19-weather-conditions",
    version=STATS19_RELEASE,
    source_field="weather_conditions",
    known_values=frozenset({"1", "2", "3", "4", "5", "6", "7", "8", "9"}),
)

STATS19_LIGHTING_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="stats19-light-conditions",
    version=STATS19_RELEASE,
    source_field="light_conditions",
    known_values=frozenset({"1", "4", "5", "6", "7", "9"}),
)

STATS19_ROAD_SURFACE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="stats19-road-surface-conditions",
    version=STATS19_RELEASE,
    source_field="road_surface_conditions",
    known_values=frozenset({"1", "2", "3", "4", "5", "6", "7", "9"}),
)

STATS19_ROAD_TYPE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="stats19-road-type",
    version=STATS19_RELEASE,
    source_field="road_type",
    known_values=frozenset({"1", "2", "3", "6", "7", "9", "12"}),
)

STATS19_JUNCTION_DETAIL_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="stats19-junction-detail",
    version=STATS19_RELEASE,
    source_field="junction_detail",
    known_values=frozenset({"0", "1", "2", "3", "5", "6", "7", "8", "9", "99"}),
)

STATS19_JUNCTION_CONTROL_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="stats19-junction-control",
    version=STATS19_RELEASE,
    source_field="junction_control",
    known_values=frozenset({"0", "1", "2", "3", "4", "9"}),
)

STATS19_SPEED_LIMIT_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="stats19-speed-limit",
    version=STATS19_RELEASE,
    source_field="speed_limit",
    known_values=frozenset({"20", "30", "40", "50", "60", "70", "99", "-1"}),
)

STATS19_FIRST_ROAD_CLASS_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="stats19-first-road-class",
    version=STATS19_RELEASE,
    source_field="first_road_class",
    known_values=frozenset({"1", "2", "3", "4", "5", "6"}),
)

STATS19_SECOND_ROAD_CLASS_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="stats19-second-road-class",
    version=STATS19_RELEASE,
    source_field="second_road_class",
    known_values=frozenset({"-1", "1", "2", "3", "4", "5", "6"}),
)

STATS19_COLLISIONS_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    STATS19_ACCIDENT_SEVERITY_TAXONOMY,
    STATS19_WEATHER_TAXONOMY,
    STATS19_LIGHTING_TAXONOMY,
    STATS19_ROAD_SURFACE_TAXONOMY,
    STATS19_ROAD_TYPE_TAXONOMY,
    STATS19_JUNCTION_DETAIL_TAXONOMY,
    STATS19_JUNCTION_CONTROL_TAXONOMY,
    STATS19_SPEED_LIMIT_TAXONOMY,
    STATS19_FIRST_ROAD_CLASS_TAXONOMY,
    STATS19_SECOND_ROAD_CLASS_TAXONOMY,
)
STATS19_VEHICLES_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    STATS19_VEHICLE_TYPE_TAXONOMY,
    STATS19_VEHICLE_MANOEUVRE_TAXONOMY,
    STATS19_VEHICLE_DIRECTION_TO_TAXONOMY,
)
STATS19_CASUALTIES_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    STATS19_CASUALTY_SEVERITY_TAXONOMY,
    STATS19_CASUALTY_CLASS_TAXONOMY,
    STATS19_CASUALTY_TYPE_TAXONOMY,
)
STATS19_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    STATS19_COLLISIONS_TAXONOMIES + STATS19_VEHICLES_TAXONOMIES + STATS19_CASUALTIES_TAXONOMIES
)
