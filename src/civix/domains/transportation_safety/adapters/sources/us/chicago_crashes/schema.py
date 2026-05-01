"""Source-field schema and taxonomy constants for Chicago crash rows."""

from __future__ import annotations

from typing import Final

from civix.core.drift import JsonFieldKind, SchemaFieldSpec, SourceSchemaSpec, TaxonomySpec

CHICAGO_CRASHES_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="chicago-traffic-crashes-raw",
    version="2026-05-01",
    fields=(
        SchemaFieldSpec(name="crash_record_id", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="crash_date", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(
            name="posted_speed_limit",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
        ),
        SchemaFieldSpec(name="traffic_control_device", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="weather_condition", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="lighting_condition", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="roadway_surface_cond", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="road_defect", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(
            name="intersection_related_i",
            kinds=(JsonFieldKind.STRING,),
            nullable=True,
        ),
        SchemaFieldSpec(name="prim_contributory_cause", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="sec_contributory_cause", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(
            name="street_no",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
            nullable=True,
        ),
        SchemaFieldSpec(name="street_direction", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="street_name", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(
            name="num_units",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
        ),
        SchemaFieldSpec(name="most_severe_injury", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(
            name="injuries_total",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
        ),
        SchemaFieldSpec(
            name="injuries_fatal",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
        ),
        SchemaFieldSpec(
            name="injuries_incapacitating",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
        ),
        SchemaFieldSpec(
            name="injuries_non_incapacitating",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
        ),
        SchemaFieldSpec(
            name="injuries_reported_not_evident",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
        ),
        SchemaFieldSpec(
            name="injuries_no_indication",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
        ),
        SchemaFieldSpec(
            name="injuries_unknown",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
        ),
        SchemaFieldSpec(
            name="latitude",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
            nullable=True,
        ),
        SchemaFieldSpec(
            name="longitude",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
            nullable=True,
        ),
        SchemaFieldSpec(name="location", kinds=(JsonFieldKind.OBJECT,), nullable=True),
    ),
)

CHICAGO_WEATHER_CONDITION_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="chicago-traffic-crashes-weather-condition",
    version="2026-05-01",
    source_field="weather_condition",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "clear",
            "cloudy/overcast",
            "fog/smoke/haze",
            "freezing rain/drizzle",
            "other",
            "rain",
            "severe cross wind gate",
            "sleet/hail",
            "snow",
            "unknown",
        }
    ),
)

CHICAGO_LIGHTING_CONDITION_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="chicago-traffic-crashes-lighting-condition",
    version="2026-05-01",
    source_field="lighting_condition",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "darkness",
            "darkness, lighted road",
            "dawn",
            "daylight",
            "dusk",
            "unknown",
        }
    ),
)

CHICAGO_ROADWAY_SURFACE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="chicago-traffic-crashes-roadway-surface-cond",
    version="2026-05-01",
    source_field="roadway_surface_cond",
    normalization="strip_casefold",
    known_values=frozenset(
        {"dry", "ice", "other", "sand, mud, dirt", "snow or slush", "unknown", "wet"}
    ),
)

CHICAGO_ROAD_DEFECT_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="chicago-traffic-crashes-road-defect",
    version="2026-05-01",
    source_field="road_defect",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "debris on roadway",
            "no defects",
            "other",
            "rut, holes",
            "shoulder defect",
            "unknown",
            "worn surface",
        }
    ),
)

CHICAGO_TRAFFIC_CONTROL_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="chicago-traffic-crashes-traffic-control-device",
    version="2026-05-01",
    source_field="traffic_control_device",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "no controls",
            "other",
            "other regulatory sign",
            "stop sign/flashers",
            "traffic signal",
            "unknown",
            "yield",
        }
    ),
)

CHICAGO_MOST_SEVERE_INJURY_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="chicago-traffic-crashes-most-severe-injury",
    version="2026-05-01",
    source_field="most_severe_injury",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "fatal",
            "incapacitating injury",
            "no indication of injury",
            "nonincapacitating injury",
            "reported, not evident",
        }
    ),
)

CHICAGO_PRIMARY_CAUSE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="chicago-traffic-crashes-prim-contributory-cause",
    version="2026-05-01",
    source_field="prim_contributory_cause",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "disregarding traffic signals",
            "driving skills/knowledge/experience",
            "failing to reduce speed to avoid crash",
            "failing to yield right-of-way",
            "following too closely",
            "not applicable",
            "unable to determine",
        }
    ),
)

CHICAGO_SECONDARY_CAUSE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="chicago-traffic-crashes-sec-contributory-cause",
    version="2026-05-01",
    source_field="sec_contributory_cause",
    normalization="strip_casefold",
    known_values=CHICAGO_PRIMARY_CAUSE_TAXONOMY.known_values,
)

CHICAGO_CRASHES_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    CHICAGO_WEATHER_CONDITION_TAXONOMY,
    CHICAGO_LIGHTING_CONDITION_TAXONOMY,
    CHICAGO_ROADWAY_SURFACE_TAXONOMY,
    CHICAGO_ROAD_DEFECT_TAXONOMY,
    CHICAGO_TRAFFIC_CONTROL_TAXONOMY,
    CHICAGO_MOST_SEVERE_INJURY_TAXONOMY,
    CHICAGO_PRIMARY_CAUSE_TAXONOMY,
    CHICAGO_SECONDARY_CAUSE_TAXONOMY,
)
