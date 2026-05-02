"""Source-field schema and taxonomy constants for Toronto TMC data."""

from __future__ import annotations

from typing import Final

from civix.core.drift import JsonFieldKind, SchemaFieldSpec, SourceSchemaSpec, TaxonomySpec

from .mapper import TMC_COUNT_COLUMNS

_VERSION: Final[str] = "2026-05-02"

TORONTO_TMC_SUMMARY_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="toronto-turning-movement-summary-raw",
    version=_VERSION,
    fields=(
        SchemaFieldSpec(name="_id", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="count_id", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="count_date", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="location_name", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="longitude", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="latitude", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="centreline_type", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="centreline_id", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(
            name="px", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING), nullable=True
        ),
        SchemaFieldSpec(name="count_duration", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="total_vehicle", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(name="total_bike", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(name="total_heavy_pct", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(name="total_pedestrian", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(name="am_peak_start", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="am_peak_vehicle", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(name="am_peak_bike", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(name="am_peak_heavy_pct", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(name="pm_peak_start", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="pm_peak_vehicle", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(name="pm_peak_bike", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(name="pm_peak_heavy_pct", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(name="n_appr_vehicle", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(name="n_appr_bike", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(name="n_appr_heavy_pct", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(name="e_appr_vehicle", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(name="e_appr_bike", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(name="e_appr_heavy_pct", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(name="s_appr_vehicle", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(name="s_appr_bike", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(name="s_appr_heavy_pct", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(name="w_appr_vehicle", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(name="w_appr_bike", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(name="w_appr_heavy_pct", kinds=(JsonFieldKind.NUMBER,), nullable=True),
    ),
)

TORONTO_TMC_RAW_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="toronto-turning-movement-raw-counts",
    version=_VERSION,
    fields=(
        SchemaFieldSpec(name="_id", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="count_id", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="count_date", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="location_name", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="longitude", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="latitude", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="centreline_type", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="centreline_id", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(
            name="px", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING), nullable=True
        ),
        SchemaFieldSpec(name="start_time", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="end_time", kinds=(JsonFieldKind.STRING,)),
        *(
            SchemaFieldSpec(
                name=column.source_field,
                kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
                nullable=True,
            )
            for column in TMC_COUNT_COLUMNS
        ),
    ),
)

TORONTO_TMC_CENTRELINE_TYPE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="toronto-turning-movement-centreline-type",
    version=_VERSION,
    source_field="centreline_type",
    normalization="strip_casefold",
    known_values=frozenset({"1", "2", "intersection", "midblock"}),
)
TORONTO_TMC_COUNT_DURATION_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="toronto-turning-movement-count-duration",
    version=_VERSION,
    source_field="count_duration",
    normalization="strip_casefold",
    known_values=frozenset({"14", "8r", "8s"}),
)

TORONTO_TMC_SUMMARY_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    TORONTO_TMC_CENTRELINE_TYPE_TAXONOMY,
    TORONTO_TMC_COUNT_DURATION_TAXONOMY,
)
TORONTO_TMC_RAW_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    TORONTO_TMC_CENTRELINE_TYPE_TAXONOMY,
)
