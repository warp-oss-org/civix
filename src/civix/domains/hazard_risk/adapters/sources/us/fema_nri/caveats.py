"""FEMA NRI source caveat categories."""

from __future__ import annotations

from enum import StrEnum
from typing import Final

from civix.core.taxonomy.models.category import CategoryRef

FEMA_NRI_CAVEAT_TAXONOMY_ID: Final[str] = "fema-nri-source-caveats"
FEMA_NRI_CAVEAT_TAXONOMY_VERSION: Final[str] = "2026-05-03"
FEMA_NRI_METADATA_SOURCE_FIELD: Final[str] = "NRI_VER"


class FemaNriCaveat(StrEnum):
    """Known FEMA caveats for National Risk Index data."""

    PLANNING_DATA = "planning_data"
    NOT_LOCAL_RISK_ASSESSMENT = "not_local_risk_assessment"
    CITE_VERSION_AND_RETRIEVAL = "cite_version_and_retrieval"


_CAVEAT_LABELS: Final[dict[FemaNriCaveat, str]] = {
    FemaNriCaveat.PLANNING_DATA: "Planning Data",
    FemaNriCaveat.NOT_LOCAL_RISK_ASSESSMENT: "Not A Substitute For Local Risk Assessment",
    FemaNriCaveat.CITE_VERSION_AND_RETRIEVAL: "Cite Dataset Version And Retrieval Context",
}


def fema_nri_caveat_categories(
    caveats: tuple[FemaNriCaveat, ...] = tuple(FemaNriCaveat),
) -> tuple[CategoryRef, ...]:
    """Build source caveat category references for FEMA NRI records."""
    return tuple(_caveat_category(caveat) for caveat in caveats)


def _caveat_category(caveat: FemaNriCaveat) -> CategoryRef:
    return CategoryRef(
        code=caveat.value,
        label=_CAVEAT_LABELS[caveat],
        taxonomy_id=FEMA_NRI_CAVEAT_TAXONOMY_ID,
        taxonomy_version=FEMA_NRI_CAVEAT_TAXONOMY_VERSION,
    )
