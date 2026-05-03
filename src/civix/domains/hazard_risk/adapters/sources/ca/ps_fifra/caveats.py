"""Public Safety Canada FIFRA source caveats."""

from __future__ import annotations

from enum import StrEnum
from typing import Final

from civix.core.taxonomy.models.category import CategoryRef

PS_FIFRA_CAVEAT_TAXONOMY_ID: Final[str] = "ps-fifra-source-caveats"
PS_FIFRA_CAVEAT_TAXONOMY_VERSION: Final[str] = "2026-05-03"
PS_FIFRA_METADATA_SOURCE_FIELD: Final[str] = "methodology_url"


class PsFifraCaveat(StrEnum):
    """Known caveats for Federally Identified Flood Risk Areas."""

    SCREENING_TOOL = "screening_tool"
    COMPLEMENTS_LOCAL_MAPS = "complements_local_maps"
    PROVINCE_TERRITORY_OPT_IN = "province_territory_opt_in"


_CAVEAT_LABELS: Final[dict[PsFifraCaveat, str]] = {
    PsFifraCaveat.SCREENING_TOOL: "Screening Tool",
    PsFifraCaveat.COMPLEMENTS_LOCAL_MAPS: "Complements Local Flood Maps",
    PsFifraCaveat.PROVINCE_TERRITORY_OPT_IN: "Province And Territory Opt-In Context",
}


def ps_fifra_caveat_categories(
    caveats: tuple[PsFifraCaveat, ...] = tuple(PsFifraCaveat),
) -> tuple[CategoryRef, ...]:
    """Build source caveat category references for FIFRA records."""
    return tuple(_caveat_category(caveat) for caveat in caveats)


def _caveat_category(caveat: PsFifraCaveat) -> CategoryRef:
    return CategoryRef(
        code=caveat.value,
        label=_CAVEAT_LABELS[caveat],
        taxonomy_id=PS_FIFRA_CAVEAT_TAXONOMY_ID,
        taxonomy_version=PS_FIFRA_CAVEAT_TAXONOMY_VERSION,
    )
