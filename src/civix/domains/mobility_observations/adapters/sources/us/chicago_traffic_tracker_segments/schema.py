"""Source-field schema and taxonomy constants for Chicago Traffic Tracker segments."""

from __future__ import annotations

from typing import Final

from civix.core.drift import JsonFieldKind, SchemaFieldSpec, SourceSchemaSpec, TaxonomySpec

from .._chicago_common import CHICAGO_TRAFFIC_TRACKER_CAVEAT_TAXONOMY

_VERSION: Final[str] = "2026-05-02"

CHICAGO_TRAFFIC_TRACKER_SEGMENTS_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="chicago-traffic-tracker-segments-raw",
    version=_VERSION,
    fields=(
        SchemaFieldSpec(name="segmentid", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="street", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="_direction", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="_fromst", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="_tost", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(
            name="_length", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING), nullable=True
        ),
        SchemaFieldSpec(name="_strheading", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="_comments", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="_lif_lon", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="_lif_lat", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="_lit_lon", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="_lit_lat", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="_traffic", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="_last_updt", kinds=(JsonFieldKind.STRING,)),
    ),
)

CHICAGO_TRAFFIC_TRACKER_SEGMENTS_DIRECTION_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="chicago-traffic-tracker-segment-direction",
    version=_VERSION,
    source_field="_direction",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "n",
            "nb",
            "n/b",
            "northbound",
            "s",
            "sb",
            "s/b",
            "southbound",
            "e",
            "eb",
            "e/b",
            "eastbound",
            "w",
            "wb",
            "w/b",
            "westbound",
            "ne",
            "nw",
            "se",
            "sw",
        }
    ),
)

CHICAGO_TRAFFIC_TRACKER_SEGMENTS_STRHEADING_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="chicago-traffic-tracker-segment-strheading",
    version=_VERSION,
    source_field="_strheading",
    normalization="strip_casefold",
    known_values=frozenset({"n", "s", "e", "w", "ne", "nw", "se", "sw"}),
)

CHICAGO_TRAFFIC_TRACKER_SEGMENTS_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    CHICAGO_TRAFFIC_TRACKER_SEGMENTS_DIRECTION_TAXONOMY,
    CHICAGO_TRAFFIC_TRACKER_SEGMENTS_STRHEADING_TAXONOMY,
    CHICAGO_TRAFFIC_TRACKER_CAVEAT_TAXONOMY,
)
