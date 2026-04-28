"""Source-field schema and taxonomy constants for the Vancouver adapter and mapper.

`ADAPTER_CONSUMED_FIELDS` lists source fields the adapter surfaces via
`RawRecord` metadata rather than leaving in `raw_data`. The mapper
imports it when building `unmapped_source_fields` so fields the adapter
already accounted for are not double-listed.

`VANCOUVER_BUSINESS_LICENCES_SCHEMA` is the schema-drift baseline for
this dataset. `VANCOUVER_TAXONOMIES` is the taxonomy-drift baseline for
categorical fields whose full vocabulary is known. Both are versioned
by date and updated by PR when the source actually changes.

Lives in its own module so neither `adapter.py` nor `mapper.py` has to
import the other for facts about the source's field shape.
"""

from __future__ import annotations

from typing import Final

from civix.core.drift import (
    JsonFieldKind,
    SchemaFieldSpec,
    SourceSchemaSpec,
    TaxonomySpec,
)

ADAPTER_CONSUMED_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "licencersn",  # → record.source_record_id
        "extractdate",  # → record.source_updated_at
    }
)

VANCOUVER_BUSINESS_LICENCES_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="vancouver-business-licences-raw",
    version="2026-04-25",
    fields=(
        SchemaFieldSpec(name="folderyear", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="licencersn", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="licencenumber", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="licencerevisionnumber", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="businessname", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="businesstradename", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="status", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="issueddate", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="expireddate", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="businesstype", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="businesssubtype", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="unit", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="unittype", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="house", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="street", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="city", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="province", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="country", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="postalcode", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="localarea", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="numberofemployees", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(name="feepaid", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="extractdate", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="geom", kinds=(JsonFieldKind.OBJECT,), nullable=True),
        SchemaFieldSpec(name="geo_point_2d", kinds=(JsonFieldKind.OBJECT,), nullable=True),
    ),
)


VANCOUVER_STATUS_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="vancouver-business-licence-status",
    version="2026-04-25",
    source_field="status",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "issued",
            "active",
            "pending",
            "inactive",
            "expired",
            "cancelled",
            "gone out of business",
        }
    ),
)


VANCOUVER_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (VANCOUVER_STATUS_TAXONOMY,)
