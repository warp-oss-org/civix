"""Source-field schema and taxonomy constants for GB DfT road-traffic-counts data."""

from __future__ import annotations

from typing import Final

from civix.core.drift import JsonFieldKind, SchemaFieldSpec, SourceSchemaSpec, TaxonomySpec

_VERSION: Final[str] = "2026-05-02"

GB_DFT_COUNT_POINTS_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="gb-dft-count-points-raw",
    version=_VERSION,
    fields=(
        SchemaFieldSpec(name="id", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="count_point_id", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="aadf_year", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="region_id", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="local_authority_id", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="road_name", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="road_category", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="road_type", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(
            name="start_junction_road_name",
            kinds=(JsonFieldKind.STRING,),
            nullable=True,
        ),
        SchemaFieldSpec(
            name="end_junction_road_name",
            kinds=(JsonFieldKind.STRING,),
            nullable=True,
        ),
        SchemaFieldSpec(name="easting", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="northing", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="latitude", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="longitude", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="link_length_km", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="link_length_miles", kinds=(JsonFieldKind.STRING,)),
    ),
)

# Schema includes vehicle-class fields the mapper does not emit (HGV per-axle
# sub-classes and `all_motor_vehicles`). They are deliberately listed so that
# their presence does not raise schema drift; the mapper skips them to avoid
# double-counting under naive aggregation.
GB_DFT_AADF_BY_DIRECTION_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="gb-dft-average-annual-daily-flow-by-direction-raw",
    version=_VERSION,
    fields=(
        SchemaFieldSpec(name="id", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="count_point_id", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="year", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="region_id", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="local_authority_id", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="road_name", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="road_category", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="road_type", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(
            name="start_junction_road_name",
            kinds=(JsonFieldKind.STRING,),
            nullable=True,
        ),
        SchemaFieldSpec(
            name="end_junction_road_name",
            kinds=(JsonFieldKind.STRING,),
            nullable=True,
        ),
        SchemaFieldSpec(name="easting", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="northing", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="latitude", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="longitude", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="link_length_km", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="link_length_miles", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="estimation_method", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="estimation_method_detailed", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="direction_of_travel", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="pedal_cycles", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="two_wheeled_motor_vehicles", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="cars_and_taxis", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="buses_and_coaches", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="lgvs", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="hgvs_2_rigid_axle", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="hgvs_3_rigid_axle", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="hgvs_4_or_more_rigid_axle", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="hgvs_3_or_4_articulated_axle", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="hgvs_5_articulated_axle", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="hgvs_6_articulated_axle", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="all_hgvs", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="all_motor_vehicles", kinds=(JsonFieldKind.NUMBER,)),
    ),
)

GB_DFT_DIRECTION_OF_TRAVEL_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="gb-dft-direction-of-travel",
    version=_VERSION,
    source_field="direction_of_travel",
    normalization="strip_casefold",
    known_values=frozenset({"n", "s", "e", "w", "c"}),
)
GB_DFT_ROAD_CATEGORY_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="gb-dft-road-category",
    version=_VERSION,
    source_field="road_category",
    normalization="exact",
    known_values=frozenset({"PA", "TA", "TM"}),
)
GB_DFT_ROAD_TYPE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="gb-dft-road-type",
    version=_VERSION,
    source_field="road_type",
    normalization="exact",
    known_values=frozenset({"Major"}),
)
GB_DFT_ESTIMATION_METHOD_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="gb-dft-estimation-method",
    version=_VERSION,
    source_field="estimation_method",
    normalization="exact",
    known_values=frozenset({"Counted", "Estimated"}),
)
GB_DFT_ESTIMATION_METHOD_DETAILED_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="gb-dft-estimation-method-detailed",
    version=_VERSION,
    source_field="estimation_method_detailed",
    normalization="exact",
    known_values=frozenset(
        {
            "Manual count",
            "Estimated using AADF from previous year on this link",
            "Estimated from nearby links",
        }
    ),
)

GB_DFT_COUNT_POINTS_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    GB_DFT_ROAD_CATEGORY_TAXONOMY,
    GB_DFT_ROAD_TYPE_TAXONOMY,
)
GB_DFT_AADF_BY_DIRECTION_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    GB_DFT_DIRECTION_OF_TRAVEL_TAXONOMY,
    GB_DFT_ROAD_CATEGORY_TAXONOMY,
    GB_DFT_ROAD_TYPE_TAXONOMY,
    GB_DFT_ESTIMATION_METHOD_TAXONOMY,
    GB_DFT_ESTIMATION_METHOD_DETAILED_TAXONOMY,
)
