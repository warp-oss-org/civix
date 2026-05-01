"""Source-field schema and taxonomy constants for Chicago person rows."""

from __future__ import annotations

from typing import Final

from civix.core.drift import JsonFieldKind, SchemaFieldSpec, SourceSchemaSpec, TaxonomySpec

CHICAGO_PEOPLE_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="chicago-traffic-people-raw",
    version="2026-05-01",
    fields=(
        SchemaFieldSpec(name="person_id", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="crash_record_id", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="vehicle_id", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="crash_date", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="person_type", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="seat_no", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(
            name="age",
            kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING),
            nullable=True,
        ),
        SchemaFieldSpec(name="safety_equipment", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="airbag_deployed", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="ejection", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="injury_classification", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="driver_action", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="driver_vision", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="physical_condition", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="sex", kinds=(JsonFieldKind.STRING,), nullable=True),
    ),
)

CHICAGO_PERSON_TYPE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="chicago-traffic-people-person-type",
    version="2026-05-01",
    source_field="person_type",
    normalization="strip_casefold",
    known_values=frozenset({"bicycle", "driver", "non-motor vehicle", "passenger", "pedestrian"}),
)

CHICAGO_INJURY_CLASSIFICATION_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="chicago-traffic-people-injury-classification",
    version="2026-05-01",
    source_field="injury_classification",
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

CHICAGO_SAFETY_EQUIPMENT_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="chicago-traffic-people-safety-equipment",
    version="2026-05-01",
    source_field="safety_equipment",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "helmet not used",
            "helmet used",
            "none present",
            "safety belt not used",
            "safety belt used",
            "usage unknown",
        }
    ),
)

CHICAGO_SEAT_NO_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="chicago-traffic-people-seat-no",
    version="2026-05-01",
    source_field="seat_no",
    normalization="strip_casefold",
    known_values=frozenset({"1", "2", "3", "4", "5", "6", "7", "8", "unknown"}),
)

CHICAGO_EJECTION_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="chicago-traffic-people-ejection",
    version="2026-05-01",
    source_field="ejection",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "not ejected",
            "partially ejected",
            "totally ejected",
            "trapped/extricated",
            "unknown",
        }
    ),
)

CHICAGO_PEOPLE_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    CHICAGO_PERSON_TYPE_TAXONOMY,
    CHICAGO_INJURY_CLASSIFICATION_TAXONOMY,
    CHICAGO_SAFETY_EQUIPMENT_TAXONOMY,
    CHICAGO_SEAT_NO_TAXONOMY,
    CHICAGO_EJECTION_TAXONOMY,
)
