"""Source-field schema and taxonomy constants for Toronto bicycle counters."""

from __future__ import annotations

from typing import Final

from civix.core.drift import JsonFieldKind, SchemaFieldSpec, SourceSchemaSpec, TaxonomySpec

_VERSION: Final[str] = "2026-05-02"

TORONTO_BICYCLE_COUNTER_LOCATIONS_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="toronto-bicycle-counter-locations-raw",
    version=_VERSION,
    fields=(
        SchemaFieldSpec(name="_id", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="location_dir_id", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="location_name", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="direction", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="linear_name_full", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="side_street", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="longitude", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="latitude", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="centreline_id", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="bin_size", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(
            name="latest_calibration_study", kinds=(JsonFieldKind.STRING,), nullable=True
        ),
        SchemaFieldSpec(name="first_active", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="last_active", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="date_decommissioned", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="technology", kinds=(JsonFieldKind.STRING,), nullable=True),
    ),
)

TORONTO_BICYCLE_COUNTER_15MIN_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="toronto-bicycle-counter-15min-raw",
    version=_VERSION,
    fields=(
        SchemaFieldSpec(name="_id", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="location_dir_id", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="datetime_bin", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="bin_volume", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
    ),
)

TORONTO_BICYCLE_COUNTER_DIRECTION_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="toronto-bicycle-counter-direction",
    version=_VERSION,
    source_field="direction",
    normalization="strip_casefold",
    known_values=frozenset({"eastbound", "northbound", "southbound", "westbound"}),
)
TORONTO_BICYCLE_COUNTER_BIN_SIZE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="toronto-bicycle-counter-bin-size",
    version=_VERSION,
    source_field="bin_size",
    normalization="strip_casefold",
    known_values=frozenset({"00:15:00", "01:00:00"}),
)
TORONTO_BICYCLE_COUNTER_TECHNOLOGY_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="toronto-bicycle-counter-technology",
    version=_VERSION,
    source_field="technology",
    normalization="strip_casefold",
    known_values=frozenset({"induction - other", "induction - loops", "radar"}),
)

TORONTO_BICYCLE_COUNTER_LOCATIONS_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    TORONTO_BICYCLE_COUNTER_DIRECTION_TAXONOMY,
    TORONTO_BICYCLE_COUNTER_BIN_SIZE_TAXONOMY,
    TORONTO_BICYCLE_COUNTER_TECHNOLOGY_TAXONOMY,
)
TORONTO_BICYCLE_COUNTER_15MIN_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = ()
