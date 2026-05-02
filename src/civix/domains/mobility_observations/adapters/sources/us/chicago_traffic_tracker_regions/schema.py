"""Source-field schema and taxonomy constants for Chicago Traffic Tracker regions."""

from __future__ import annotations

from typing import Final

from civix.core.drift import JsonFieldKind, SchemaFieldSpec, SourceSchemaSpec, TaxonomySpec

from .._chicago_common import CHICAGO_TRAFFIC_TRACKER_CAVEAT_TAXONOMY

_VERSION: Final[str] = "2026-05-02"

CHICAGO_TRAFFIC_TRACKER_REGIONS_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="chicago-traffic-tracker-regions-raw",
    version=_VERSION,
    fields=(
        SchemaFieldSpec(name="_region_id", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="region", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="description", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="west", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="east", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="south", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="north", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="current_speed", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="_last_updt", kinds=(JsonFieldKind.STRING,)),
    ),
)

CHICAGO_TRAFFIC_TRACKER_REGIONS_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    CHICAGO_TRAFFIC_TRACKER_CAVEAT_TAXONOMY,
)
