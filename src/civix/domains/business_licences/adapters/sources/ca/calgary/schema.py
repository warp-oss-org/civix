"""Source-field schema and taxonomy constants for Calgary business licences."""

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
        "getbusid",  # -> record.source_record_id
    }
)

CALGARY_BUSINESS_LICENCES_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="calgary-business-licences-raw",
    version="2026-04-29",
    fields=(
        SchemaFieldSpec(name="getbusid", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="tradename", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="homeoccind", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="address", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="comdistcd", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="comdistnm", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="licencetypes", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="first_iss_dt", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="exp_dt", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="jobstatusdesc", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="point", kinds=(JsonFieldKind.OBJECT,), nullable=True),
        SchemaFieldSpec(name="globalid", kinds=(JsonFieldKind.STRING,), nullable=True),
    ),
)


CALGARY_STATUS_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="calgary-business-licence-status",
    version="2026-04-29",
    source_field="jobstatusdesc",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "renewal licensed",
            "pending renewal",
            "licensed",
            "renewal invoiced",
            "move in progress",
            "renewal notification sent",
            "close in progress",
        }
    ),
)


CALGARY_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (CALGARY_STATUS_TAXONOMY,)
