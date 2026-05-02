"""Source-field schema and taxonomy constants for NYC traffic volume counts."""

from __future__ import annotations

from typing import Final

from civix.core.drift import JsonFieldKind, SchemaFieldSpec, SourceSchemaSpec, TaxonomySpec

NYC_TRAFFIC_VOLUME_COUNTS_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="nyc-traffic-volume-counts-raw",
    version="2026-05-02",
    fields=(
        SchemaFieldSpec(name="RequestID", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="Boro", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="Yr", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="M", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="D", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="HH", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="MM", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="Vol", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="SegmentID", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="WktGeom", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="street", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="fromSt", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="toSt", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="Direction", kinds=(JsonFieldKind.STRING,), nullable=True),
    ),
)

NYC_TRAFFIC_VOLUME_BOROUGH_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-traffic-volume-counts-borough",
    version="2026-05-02",
    source_field="Boro",
    normalization="strip_casefold",
    known_values=frozenset({"bronx", "brooklyn", "manhattan", "queens", "staten island"}),
)

NYC_TRAFFIC_VOLUME_DIRECTION_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-traffic-volume-counts-direction",
    version="2026-05-02",
    source_field="Direction",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "eastbound",
            "e/b",
            "northbound",
            "n/b",
            "southbound",
            "s/b",
            "westbound",
            "w/b",
        }
    ),
)

NYC_TRAFFIC_VOLUME_COUNTS_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    NYC_TRAFFIC_VOLUME_BOROUGH_TAXONOMY,
    NYC_TRAFFIC_VOLUME_DIRECTION_TAXONOMY,
)
