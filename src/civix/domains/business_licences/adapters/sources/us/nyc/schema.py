"""Source-field schema and taxonomy constants for NYC DCWP licences."""

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
        "license_nbr",  # -> record.source_record_id
    }
)

NYC_BUSINESS_LICENCES_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="nyc-business-licences-raw",
    version="2026-04-30",
    fields=(
        SchemaFieldSpec(name="license_nbr", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="business_name", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="dba_trade_name", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="business_unique_id", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="business_category", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="license_type", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="license_status", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="license_creation_date", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="lic_expir_dd", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="detail", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="contact_phone", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="address_type", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="address_building", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="address_street_name", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(
            name="address_street_name_2",
            kinds=(JsonFieldKind.STRING,),
            nullable=True,
        ),
        SchemaFieldSpec(name="street3", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="unit_type", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="apt_suite", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="address_city", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="address_state", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="address_zip", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="address_borough", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="community_board", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="council_district", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="bin", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="bbl", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="nta", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(
            name="census_block_2010_",
            kinds=(JsonFieldKind.STRING,),
            nullable=True,
        ),
        SchemaFieldSpec(name="census_tract", kinds=(JsonFieldKind.STRING,), nullable=True),
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
    ),
)


NYC_LICENSE_STATUS_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-dcwp-license-status",
    version="2026-04-30",
    source_field="license_status",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "active",
            "expired",
            "surrendered",
            "failed to renew",
            "ready for renewal",
            "revoked",
            "voided",
            "suspended",
            "out of business",
            "close",
            "tol",
        }
    ),
)


NYC_LICENSE_TYPE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-dcwp-license-type",
    version="2026-04-30",
    source_field="license_type",
    normalization="strip_casefold",
    known_values=frozenset({"premises"}),
)


NYC_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    NYC_LICENSE_STATUS_TAXONOMY,
    NYC_LICENSE_TYPE_TAXONOMY,
)
