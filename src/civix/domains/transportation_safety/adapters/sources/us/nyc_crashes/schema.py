"""Source-field schema and taxonomy constants for NYC crash rows."""

from __future__ import annotations

from typing import Final

from civix.core.drift import JsonFieldKind, SchemaFieldSpec, SourceSchemaSpec, TaxonomySpec

NYC_CRASHES_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="nyc-motor-vehicle-collisions-crashes-raw",
    version="2026-05-02",
    fields=(
        SchemaFieldSpec(name="crash_date", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="crash_time", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="borough", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="zip_code", kinds=(JsonFieldKind.STRING,), nullable=True),
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
        SchemaFieldSpec(name="on_street_name", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="off_street_name", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="cross_street_name", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(
            name="number_of_persons_injured",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
        ),
        SchemaFieldSpec(
            name="number_of_persons_killed",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
        ),
        SchemaFieldSpec(
            name="number_of_pedestrians_injured",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
        ),
        SchemaFieldSpec(
            name="number_of_pedestrians_killed",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
        ),
        SchemaFieldSpec(
            name="number_of_cyclist_injured",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
        ),
        SchemaFieldSpec(
            name="number_of_cyclist_killed",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
        ),
        SchemaFieldSpec(
            name="number_of_motorist_injured",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
        ),
        SchemaFieldSpec(
            name="number_of_motorist_killed",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
        ),
        SchemaFieldSpec(
            name="contributing_factor_vehicle_1",
            kinds=(JsonFieldKind.STRING,),
            nullable=True,
        ),
        SchemaFieldSpec(
            name="contributing_factor_vehicle_2",
            kinds=(JsonFieldKind.STRING,),
            nullable=True,
        ),
        SchemaFieldSpec(
            name="contributing_factor_vehicle_3",
            kinds=(JsonFieldKind.STRING,),
            nullable=True,
        ),
        SchemaFieldSpec(
            name="contributing_factor_vehicle_4",
            kinds=(JsonFieldKind.STRING,),
            nullable=True,
        ),
        SchemaFieldSpec(
            name="contributing_factor_vehicle_5",
            kinds=(JsonFieldKind.STRING,),
            nullable=True,
        ),
        SchemaFieldSpec(
            name="collision_id",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
        ),
        SchemaFieldSpec(name="vehicle_type_code1", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="vehicle_type_code2", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="vehicle_type_code_3", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="vehicle_type_code_4", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="vehicle_type_code_5", kinds=(JsonFieldKind.STRING,), nullable=True),
    ),
)

NYC_CRASH_BOROUGH_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-crashes-borough",
    version="2026-05-02",
    source_field="borough",
    normalization="strip_casefold",
    known_values=frozenset({"bronx", "brooklyn", "manhattan", "queens", "staten island"}),
)

NYC_CRASH_CONTRIBUTING_FACTOR_1_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-crashes-contributing-factor-vehicle-1",
    version="2026-05-02",
    source_field="contributing_factor_vehicle_1",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "driver inattention/distraction",
            "failure to yield right-of-way",
            "following too closely",
            "other vehicular",
            "passing or lane usage improper",
            "unspecified",
        }
    ),
)

NYC_CRASH_CONTRIBUTING_FACTOR_2_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-crashes-contributing-factor-vehicle-2",
    version="2026-05-02",
    source_field="contributing_factor_vehicle_2",
    normalization="strip_casefold",
    known_values=NYC_CRASH_CONTRIBUTING_FACTOR_1_TAXONOMY.known_values,
)

NYC_CRASH_CONTRIBUTING_FACTOR_3_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-crashes-contributing-factor-vehicle-3",
    version="2026-05-02",
    source_field="contributing_factor_vehicle_3",
    normalization="strip_casefold",
    known_values=NYC_CRASH_CONTRIBUTING_FACTOR_1_TAXONOMY.known_values,
)

NYC_CRASH_CONTRIBUTING_FACTOR_4_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-crashes-contributing-factor-vehicle-4",
    version="2026-05-02",
    source_field="contributing_factor_vehicle_4",
    normalization="strip_casefold",
    known_values=NYC_CRASH_CONTRIBUTING_FACTOR_1_TAXONOMY.known_values,
)

NYC_CRASH_CONTRIBUTING_FACTOR_5_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-crashes-contributing-factor-vehicle-5",
    version="2026-05-02",
    source_field="contributing_factor_vehicle_5",
    normalization="strip_casefold",
    known_values=NYC_CRASH_CONTRIBUTING_FACTOR_1_TAXONOMY.known_values,
)

NYC_CRASHES_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    NYC_CRASH_BOROUGH_TAXONOMY,
    NYC_CRASH_CONTRIBUTING_FACTOR_1_TAXONOMY,
    NYC_CRASH_CONTRIBUTING_FACTOR_2_TAXONOMY,
    NYC_CRASH_CONTRIBUTING_FACTOR_3_TAXONOMY,
    NYC_CRASH_CONTRIBUTING_FACTOR_4_TAXONOMY,
    NYC_CRASH_CONTRIBUTING_FACTOR_5_TAXONOMY,
)
