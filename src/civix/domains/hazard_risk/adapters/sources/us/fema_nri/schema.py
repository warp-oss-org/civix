"""Source-field schema and taxonomy constants for FEMA NRI fixture rows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from civix.core.drift import JsonFieldKind, SchemaFieldSpec, SourceSchemaSpec, TaxonomySpec

FEMA_NRI_SCHEMA_VERSION: Final[str] = "2026-05-03"
FEMA_NRI_TAXONOMY_VERSION: Final[str] = "2026-05-03"


@dataclass(frozen=True, slots=True)
class NriVersionMetadata:
    """Metadata implied by a known `NRI_VER` source value."""

    methodology_version: str
    publication_year: int
    publication_month: int


NRI_VERSION_METADATA: Final[dict[str, NriVersionMetadata]] = {
    "December 2025": NriVersionMetadata(
        methodology_version="1.20.0",
        publication_year=2025,
        publication_month=12,
    ),
}

NRI_HAZARD_PREFIXES: Final[tuple[str, ...]] = (
    "AVLN",
    "CFLD",
    "CWAV",
    "DRGT",
    "ERQK",
    "HAIL",
    "HRCN",
    "HWAV",
    "IFLD",
    "ISTM",
    "LNDS",
    "LTNG",
    "SWND",
    "TRND",
    "TSUN",
    "VLCN",
    "WFIR",
    "WNTW",
)

_AREA_FIELDS: Final[tuple[str, ...]] = (
    "NRI_ID",
    "STATE",
    "STATEABBRV",
    "STATEFIPS",
    "COUNTY",
    "COUNTYFIPS",
    "STCOFIPS",
    "TRACT",
    "TRACTFIPS",
)
_CORE_SCORE_FIELDS: Final[tuple[str, ...]] = (
    "RISK_SCORE",
    "RISK_RATNG",
    "RISK_SPCTL",
    "EAL_SCORE",
    "EAL_RATNG",
    "EAL_VALT",
    "SOVI_SCORE",
    "SOVI_RATNG",
    "RESL_SCORE",
    "RESL_RATNG",
    "NRI_VER",
)
FEMA_NRI_TRACTS_OUT_FIELDS: Final[tuple[str, ...]] = (
    *_AREA_FIELDS,
    *_CORE_SCORE_FIELDS,
    *(field for prefix in NRI_HAZARD_PREFIXES for field in (f"{prefix}_RISKS", f"{prefix}_RISKR")),
)

_STRING: Final[tuple[JsonFieldKind, ...]] = (JsonFieldKind.STRING,)
_NUMBER: Final[tuple[JsonFieldKind, ...]] = (JsonFieldKind.NUMBER,)

FEMA_NRI_TRACTS_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="fema-nri-census-tracts-raw",
    version=FEMA_NRI_SCHEMA_VERSION,
    fields=(
        *(SchemaFieldSpec(name=field, kinds=_STRING) for field in _AREA_FIELDS),
        SchemaFieldSpec(name="RISK_SCORE", kinds=_NUMBER, nullable=True),
        SchemaFieldSpec(name="RISK_RATNG", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="RISK_SPCTL", kinds=_NUMBER, nullable=True),
        SchemaFieldSpec(name="EAL_SCORE", kinds=_NUMBER, nullable=True),
        SchemaFieldSpec(name="EAL_RATNG", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="EAL_VALT", kinds=_NUMBER, nullable=True),
        SchemaFieldSpec(name="SOVI_SCORE", kinds=_NUMBER, nullable=True),
        SchemaFieldSpec(name="SOVI_RATNG", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="RESL_SCORE", kinds=_NUMBER, nullable=True),
        SchemaFieldSpec(name="RESL_RATNG", kinds=_STRING, nullable=True),
        SchemaFieldSpec(name="NRI_VER", kinds=_STRING),
        *(
            field_spec
            for prefix in NRI_HAZARD_PREFIXES
            for field_spec in (
                SchemaFieldSpec(name=f"{prefix}_RISKS", kinds=_NUMBER, nullable=True),
                SchemaFieldSpec(name=f"{prefix}_RISKR", kinds=_STRING, nullable=True),
            )
        ),
    ),
)

_KNOWN_RATINGS: Final[frozenset[str]] = frozenset(
    {
        "very high",
        "relatively high",
        "relatively moderate",
        "relatively low",
        "very low",
        "insufficient data",
        "not applicable",
    }
)

FEMA_NRI_VERSION_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="fema-nri-version",
    version=FEMA_NRI_TAXONOMY_VERSION,
    source_field="NRI_VER",
    normalization="exact",
    known_values=frozenset(NRI_VERSION_METADATA),
)
FEMA_NRI_RATING_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    *(
        TaxonomySpec(
            taxonomy_id=f"fema-nri-{field.lower().replace('_', '-')}",
            version=FEMA_NRI_TAXONOMY_VERSION,
            source_field=field,
            normalization="strip_casefold",
            known_values=_KNOWN_RATINGS,
        )
        for field in ("RISK_RATNG", "EAL_RATNG", "SOVI_RATNG", "RESL_RATNG")
    ),
    *(
        TaxonomySpec(
            taxonomy_id=f"fema-nri-{prefix.lower()}-risk-rating",
            version=FEMA_NRI_TAXONOMY_VERSION,
            source_field=f"{prefix}_RISKR",
            normalization="strip_casefold",
            known_values=_KNOWN_RATINGS,
        )
        for prefix in NRI_HAZARD_PREFIXES
    ),
)
FEMA_NRI_TRACTS_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    FEMA_NRI_VERSION_TAXONOMY,
    *FEMA_NRI_RATING_TAXONOMIES,
)
