"""Source-field schema and taxonomy constants for NYC person rows."""

from __future__ import annotations

from typing import Final

from civix.core.drift import JsonFieldKind, SchemaFieldSpec, SourceSchemaSpec, TaxonomySpec

NYC_PERSONS_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="nyc-motor-vehicle-collisions-persons-raw",
    version="2026-05-02",
    fields=(
        SchemaFieldSpec(name="unique_id", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="collision_id", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="crash_date", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="crash_time", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="person_id", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="person_type", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="person_injury", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="vehicle_id", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(
            name="person_age",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
            nullable=True,
        ),
        SchemaFieldSpec(name="ejection", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="emotional_status", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="bodily_injury", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="position_in_vehicle", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="safety_equipment", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="ped_location", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="ped_action", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="complaint", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="ped_role", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="contributing_factor_1", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="contributing_factor_2", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="person_sex", kinds=(JsonFieldKind.STRING,), nullable=True),
    ),
)

NYC_PERSON_TYPE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-persons-person-type",
    version="2026-05-02",
    source_field="person_type",
    normalization="strip_casefold",
    known_values=frozenset({"bicyclist", "occupant", "other motorized", "pedestrian"}),
)

NYC_PERSON_INJURY_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-persons-person-injury",
    version="2026-05-02",
    source_field="person_injury",
    normalization="strip_casefold",
    known_values=frozenset({"injured", "killed", "unspecified"}),
)

NYC_PED_ROLE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-persons-ped-role",
    version="2026-05-02",
    source_field="ped_role",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "driver",
            "notified person",
            "owner",
            "passenger",
            "pedestrian",
            "policy holder",
            "registrant",
            "witness",
        }
    ),
)

NYC_SAFETY_EQUIPMENT_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-persons-safety-equipment",
    version="2026-05-02",
    source_field="safety_equipment",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "-",
            "air bag deployed",
            "child restraint only",
            "helmet (motorcycle only)",
            "helmet only (in-line skater/bicyclist)",
            "lap belt",
            "lap belt & harness",
            "none",
            "other",
            "unknown",
        }
    ),
)

NYC_POSITION_IN_VEHICLE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-persons-position-in-vehicle",
    version="2026-05-02",
    source_field="position_in_vehicle",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "does not apply",
            "driver",
            "front passenger, if two or more persons, including the driver, are in the front seat",
            "left rear passenger, or rear passenger on a bicycle, motorcycle, snowmobile",
            "right rear passenger or motorcycle sidecar passenger",
            "unknown",
        }
    ),
)

NYC_EJECTION_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-persons-ejection",
    version="2026-05-02",
    source_field="ejection",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "does not apply",
            "ejected",
            "not ejected",
            "partially ejected",
            "trapped",
            "unknown",
        }
    ),
)

NYC_PERSON_CONTRIBUTING_FACTOR_1_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-persons-contributing-factor-1",
    version="2026-05-02",
    source_field="contributing_factor_1",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "driver inattention/distraction",
            "failure to yield right-of-way",
            "pedestrian/bicyclist/other pedestrian error/confusion",
            "traffic control disregarded",
            "unspecified",
        }
    ),
)

NYC_PERSON_CONTRIBUTING_FACTOR_2_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-persons-contributing-factor-2",
    version="2026-05-02",
    source_field="contributing_factor_2",
    normalization="strip_casefold",
    known_values=NYC_PERSON_CONTRIBUTING_FACTOR_1_TAXONOMY.known_values,
)

NYC_PERSONS_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    NYC_PERSON_TYPE_TAXONOMY,
    NYC_PERSON_INJURY_TAXONOMY,
    NYC_PED_ROLE_TAXONOMY,
    NYC_SAFETY_EQUIPMENT_TAXONOMY,
    NYC_POSITION_IN_VEHICLE_TAXONOMY,
    NYC_EJECTION_TAXONOMY,
    NYC_PERSON_CONTRIBUTING_FACTOR_1_TAXONOMY,
    NYC_PERSON_CONTRIBUTING_FACTOR_2_TAXONOMY,
)
