"""FEMA NFHL source caveat categories."""

from __future__ import annotations

from enum import StrEnum
from typing import Final

from civix.core.taxonomy.models.category import CategoryRef

FEMA_NFHL_CAVEAT_TAXONOMY_ID: Final[str] = "fema-nfhl-source-caveats"
FEMA_NFHL_CAVEAT_TAXONOMY_VERSION: Final[str] = "2026-05-03"
FEMA_NFHL_METADATA_SOURCE_FIELD: Final[str] = "source.layer_metadata"


class FemaNfhlCaveat(StrEnum):
    """Known FEMA caveats for NFHL Flood Hazard Zone records."""

    EFFECTIVE_REGULATORY_LAYER = "effective_regulatory_layer"
    NOT_ALL_AREAS_HAVE_MODERNIZED_GIS = "not_all_areas_have_modernized_gis"
    EFFECTIVE_DATES_OUT_OF_SCOPE = "effective_dates_out_of_scope"
    CITE_SOURCE_AND_RETRIEVAL = "cite_source_and_retrieval"


_CAVEAT_LABELS: Final[dict[FemaNfhlCaveat, str]] = {
    FemaNfhlCaveat.EFFECTIVE_REGULATORY_LAYER: "Effective Regulatory NFHL Layer",
    FemaNfhlCaveat.NOT_ALL_AREAS_HAVE_MODERNIZED_GIS: ("Not All Areas Have Modernized GIS Data"),
    FemaNfhlCaveat.EFFECTIVE_DATES_OUT_OF_SCOPE: (
        "Effective Dates Live On FIRM Panel Data Outside This Slice"
    ),
    FemaNfhlCaveat.CITE_SOURCE_AND_RETRIEVAL: "Cite Source And Retrieval Context",
}


def fema_nfhl_caveat_categories(
    caveats: tuple[FemaNfhlCaveat, ...] = tuple(FemaNfhlCaveat),
) -> tuple[CategoryRef, ...]:
    """Build source caveat category references for FEMA NFHL records."""
    return tuple(_caveat_category(caveat) for caveat in caveats)


def _caveat_category(caveat: FemaNfhlCaveat) -> CategoryRef:
    return CategoryRef(
        code=caveat.value,
        label=_CAVEAT_LABELS[caveat],
        taxonomy_id=FEMA_NFHL_CAVEAT_TAXONOMY_ID,
        taxonomy_version=FEMA_NFHL_CAVEAT_TAXONOMY_VERSION,
    )
