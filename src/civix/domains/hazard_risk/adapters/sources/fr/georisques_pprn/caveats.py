"""Georisques GASPAR PPRN source caveat categories."""

from __future__ import annotations

from enum import StrEnum
from typing import Final

from civix.core.taxonomy.models.category import CategoryRef

GEORISQUES_PPRN_CAVEAT_TAXONOMY_ID: Final[str] = "georisques-pprn-source-caveats"
GEORISQUES_PPRN_CAVEAT_TAXONOMY_VERSION: Final[str] = "2026-05-03"
GEORISQUES_PPRN_METADATA_SOURCE_FIELD: Final[str] = "source.gaspar_metadata"


class GeorisquesPprnCaveat(StrEnum):
    """Known caveats for Georisques GASPAR PPRN records."""

    COMMUNE_GRAINED_PROCEDURES = "commune_grained_procedures"
    NO_PLAN_POLYGON_IN_CSV = "no_plan_polygon_in_csv"
    LEGAL_STATUS_FROM_GASPAR_LABELS = "legal_status_from_gaspar_labels"
    OPEN_LICENSE_AND_RETRIEVAL_CONTEXT = "open_license_and_retrieval_context"


_CAVEAT_LABELS: Final[dict[GeorisquesPprnCaveat, str]] = {
    GeorisquesPprnCaveat.COMMUNE_GRAINED_PROCEDURES: "Commune-Grained Procedure Rows",
    GeorisquesPprnCaveat.NO_PLAN_POLYGON_IN_CSV: "No Plan Polygon In GASPAR CSV",
    GeorisquesPprnCaveat.LEGAL_STATUS_FROM_GASPAR_LABELS: (
        "Legal Status Derived From GASPAR State Labels"
    ),
    GeorisquesPprnCaveat.OPEN_LICENSE_AND_RETRIEVAL_CONTEXT: (
        "Published Under Open Licence With Retrieval Context Required"
    ),
}


def georisques_pprn_caveat_categories(
    caveats: tuple[GeorisquesPprnCaveat, ...] = tuple(GeorisquesPprnCaveat),
) -> tuple[CategoryRef, ...]:
    """Build source caveat category references for Georisques PPRN records."""
    return tuple(_caveat_category(caveat) for caveat in caveats)


def _caveat_category(caveat: GeorisquesPprnCaveat) -> CategoryRef:
    return CategoryRef(
        code=caveat.value,
        label=_CAVEAT_LABELS[caveat],
        taxonomy_id=GEORISQUES_PPRN_CAVEAT_TAXONOMY_ID,
        taxonomy_version=GEORISQUES_PPRN_CAVEAT_TAXONOMY_VERSION,
    )
