"""Source-field schema and taxonomy constants for NYC bicycle/pedestrian counts."""

from __future__ import annotations

from typing import Final

from civix.core.drift import JsonFieldKind, SchemaFieldSpec, SourceSchemaSpec, TaxonomySpec

NYC_BICYCLE_PEDESTRIAN_COUNTS_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="nyc-bicycle-pedestrian-counts-raw",
    version="2026-05-02",
    fields=(
        SchemaFieldSpec(name="sensor_id", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="travelMode", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="direction", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="flowID", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="flowName", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="timestamp", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="granularity", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="counts", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="status", kinds=(JsonFieldKind.STRING,), nullable=True),
    ),
)

NYC_BICYCLE_PEDESTRIAN_SENSORS_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="nyc-bicycle-pedestrian-sensors-raw",
    version="2026-05-02",
    fields=(
        SchemaFieldSpec(name="sensor_id", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="sensor_name", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="longitude", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="latitude", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="direction", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="status", kinds=(JsonFieldKind.STRING,), nullable=True),
    ),
)

NYC_BICYCLE_PEDESTRIAN_TRAVEL_MODE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-bicycle-pedestrian-travel-mode",
    version="2026-05-02",
    source_field="travelMode",
    normalization="strip_casefold",
    known_values=frozenset({"bicycle", "pedestrian", "bike", "ped"}),
)

NYC_BICYCLE_PEDESTRIAN_DIRECTION_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-bicycle-pedestrian-direction",
    version="2026-05-02",
    source_field="direction",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "eastbound",
            "northbound",
            "southbound",
            "westbound",
            "bidirectional",
            "inbound",
            "outbound",
        }
    ),
)

NYC_BICYCLE_PEDESTRIAN_GRANULARITY_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-bicycle-pedestrian-granularity",
    version="2026-05-02",
    source_field="granularity",
    normalization="strip_casefold",
    known_values=frozenset({"15 minutes", "15-minute", "hourly", "daily"}),
)

NYC_BICYCLE_PEDESTRIAN_STATUS_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-bicycle-pedestrian-status",
    version="2026-05-02",
    source_field="status",
    normalization="strip_casefold",
    known_values=frozenset({"valid", "verified", "preliminary", "missing"}),
)

NYC_BICYCLE_PEDESTRIAN_COUNTS_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    NYC_BICYCLE_PEDESTRIAN_TRAVEL_MODE_TAXONOMY,
    NYC_BICYCLE_PEDESTRIAN_DIRECTION_TAXONOMY,
    NYC_BICYCLE_PEDESTRIAN_GRANULARITY_TAXONOMY,
    NYC_BICYCLE_PEDESTRIAN_STATUS_TAXONOMY,
)
NYC_BICYCLE_PEDESTRIAN_SENSORS_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    NYC_BICYCLE_PEDESTRIAN_DIRECTION_TAXONOMY,
    NYC_BICYCLE_PEDESTRIAN_STATUS_TAXONOMY,
)
