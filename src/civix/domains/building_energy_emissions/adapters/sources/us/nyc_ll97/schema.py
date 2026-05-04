"""Source-field schema and taxonomy specs for NYC LL97 CBL fixture rows."""

from __future__ import annotations

from typing import Final

from civix.core.drift import JsonFieldKind, SchemaFieldSpec, SourceSchemaSpec, TaxonomySpec

LL97_SCHEMA_VERSION: Final[str] = "2026-05-03"
LL97_TAXONOMY_VERSION: Final[str] = "2026-05-03"

# Canonical snake_case field names used throughout the slice. The adapter
# normalizes the workbook's header row (which has inconsistent leading and
# trailing whitespace) into these names; if DOB renames a column, the
# normalized form will diverge and surface as schema drift.
_IDENTITY_FIELDS: Final[tuple[str, ...]] = ("bbl", "bin")
_LL97_FIELDS: Final[tuple[str, ...]] = (
    "on_ll97_cbl",
    "ll97_compliance_pathway",
)
_OTHER_LAW_FIELDS: Final[tuple[str, ...]] = (
    "on_ll84_cbl",
    "required_to_report_water_data_from_dep",
    "on_ll88_cbl",
    "on_ll87",
)
_LOT_FIELDS: Final[tuple[str, ...]] = (
    "dof_bbl_address",
    "dof_bbl_zip_code",
    "dof_bbl_building_count",
    "dof_bbl_gross_square_footage",
)

LL97_RAW_FIELDS: Final[tuple[str, ...]] = (
    *_IDENTITY_FIELDS,
    *_LL97_FIELDS,
    *_OTHER_LAW_FIELDS,
    *_LOT_FIELDS,
)

# DOB header text the adapter expects to see in the workbook, mapped to the
# canonical field name. The keys are matched against headers after stripping
# surrounding whitespace and collapsing to lowercase snake_case; a DOB rename
# that survives that normalization fires schema drift via the missing-field
# check, not via this lookup.
LL97_HEADER_NORMALIZATION: Final[dict[str, str]] = {
    "bbl": "bbl",
    "bin": "bin",
    "on_ll97_cbl": "on_ll97_cbl",
    "ll97_compliance_pathway": "ll97_compliance_pathway",
    "on_ll84_cbl": "on_ll84_cbl",
    "required_to_report_water_data_from_dep": "required_to_report_water_data_from_dep",
    "on_ll88_cbl": "on_ll88_cbl",
    "on_ll87": "on_ll87",
    "dof_bbl_address": "dof_bbl_address",
    "dof_bbl_zip_code": "dof_bbl_zip_code",
    "dof_bbl_building_count": "dof_bbl_building_count",
    "dof_bbl_gross_square_footage": "dof_bbl_gross_square_footage",
}

_STRING: Final[tuple[JsonFieldKind, ...]] = (JsonFieldKind.STRING,)

# Every value is stringified by the adapter so heterogeneous spreadsheet cell
# types (int BBL, str pathway "0", str zip "10004-1940", None GSF) collapse to
# one shape downstream. The schema is uniformly STRING with nullable=True for
# fields the dataset legitimately omits per row.
LL97_RAW_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="nyc-ll97-cbl-raw",
    version=LL97_SCHEMA_VERSION,
    fields=(
        SchemaFieldSpec(name="bbl", kinds=_STRING),
        SchemaFieldSpec(name="bin", kinds=_STRING),
        SchemaFieldSpec(name="on_ll97_cbl", kinds=_STRING),
        SchemaFieldSpec(name="ll97_compliance_pathway", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="on_ll84_cbl", kinds=_STRING, nullable=True),
        SchemaFieldSpec(
            name="required_to_report_water_data_from_dep", kinds=_STRING, nullable=True
        ),
        SchemaFieldSpec(name="on_ll88_cbl", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="on_ll87", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="dof_bbl_address", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="dof_bbl_zip_code", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="dof_bbl_building_count", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="dof_bbl_gross_square_footage", kinds=_STRING, nullable=True),
    ),
)

_KNOWN_YES_NO: Final[frozenset[str]] = frozenset({"y", "n"})
_KNOWN_PATHWAYS: Final[frozenset[str]] = frozenset({"0", "1", "2", "3", "4"})

LL97_COVERED_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-ll97-on-cbl",
    version=LL97_TAXONOMY_VERSION,
    source_field="on_ll97_cbl",
    normalization="strip_casefold",
    known_values=_KNOWN_YES_NO,
)
LL97_PATHWAY_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="nyc-ll97-compliance-pathway",
    version=LL97_TAXONOMY_VERSION,
    source_field="ll97_compliance_pathway",
    normalization="exact",
    known_values=_KNOWN_PATHWAYS,
)

LL97_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    LL97_COVERED_TAXONOMY,
    LL97_PATHWAY_TAXONOMY,
)
