"""Source-field schema and taxonomy constants for the Toronto adapter and mapper.

`ADAPTER_CONSUMED_FIELDS` lists source fields the adapter surfaces via
`RawRecord` metadata rather than treating purely as `raw_data`. The
mapper imports it when building `unmapped_source_fields` so fields the
adapter already accounted for are not double-listed.

`TORONTO_BUSINESS_LICENCES_SCHEMA` is the schema-drift baseline for this
dataset, derived from CKAN's datastore field metadata. The CKAN
transport's own `_id` row index is preserved in raw data so snapshots
remain faithful to CKAN's response shape.

`TORONTO_TAXONOMIES` is empty: Toronto's `Category` is open-vocabulary
across ~hundreds of values, and there is no source-direct status field
(status is derived in the mapper from `Cancel Date`). A partial spec
would fire false-positive drift findings on every category we have not
yet seen, so a category taxonomy is deferred until a future PR can
enumerate it from the live dataset rather than guess.

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
        "Licence No.",  # → record.source_record_id
    }
)

TORONTO_BUSINESS_LICENCES_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="toronto-business-licences-raw",
    version="2026-04-28",
    fields=(
        SchemaFieldSpec(name="_id", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="Category", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="Licence No.", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="Operating Name", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="Issued", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="Client Name", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="Business Phone", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="Business Phone Ext.", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="Licence Address Line 1", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="Licence Address Line 2", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="Licence Address Line 3", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="Ward", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(name="Conditions", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(
            name="Free Form Conditions Line 1",
            kinds=(JsonFieldKind.STRING,),
            nullable=True,
        ),
        SchemaFieldSpec(
            name="Free Form Conditions Line 2",
            kinds=(JsonFieldKind.STRING,),
            nullable=True,
        ),
        SchemaFieldSpec(name="Plate No.", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="Endorsements", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="Cancel Date", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="Last Record Update", kinds=(JsonFieldKind.STRING,)),
    ),
)


TORONTO_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = ()
