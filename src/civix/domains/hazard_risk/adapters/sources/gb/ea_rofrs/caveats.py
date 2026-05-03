"""Environment Agency RoFRS source caveats."""

from __future__ import annotations

from enum import StrEnum
from typing import Final

from civix.core.taxonomy.models.category import CategoryRef

EA_ROFRS_CAVEAT_TAXONOMY_ID: Final[str] = "ea-rofrs-source-caveats"
EA_ROFRS_CAVEAT_TAXONOMY_VERSION: Final[str] = "2026-05-03"
EA_ROFRS_METADATA_SOURCE_FIELD: Final[str] = "product_url"


class EaRofrsCaveat(StrEnum):
    """Known caveats for Risk of Flooding from Rivers and Sea records."""

    PROBABILISTIC_PRODUCT = "probabilistic_product"
    NOT_PROPERTY_LEVEL = "not_property_level"
    DEFENCES_DO_NOT_REMOVE_RISK = "defences_do_not_remove_risk"


_CAVEAT_LABELS: Final[dict[EaRofrsCaveat, str]] = {
    EaRofrsCaveat.PROBABILISTIC_PRODUCT: "Probabilistic Product",
    EaRofrsCaveat.NOT_PROPERTY_LEVEL: "Not A Property-Level Assessment",
    EaRofrsCaveat.DEFENCES_DO_NOT_REMOVE_RISK: "Flood Defences Do Not Remove Risk",
}


def ea_rofrs_caveat_categories(
    caveats: tuple[EaRofrsCaveat, ...] = tuple(EaRofrsCaveat),
) -> tuple[CategoryRef, ...]:
    """Build source caveat category references for RoFRS records."""
    return tuple(_caveat_category(caveat) for caveat in caveats)


def _caveat_category(caveat: EaRofrsCaveat) -> CategoryRef:
    return CategoryRef(
        code=caveat.value,
        label=_CAVEAT_LABELS[caveat],
        taxonomy_id=EA_ROFRS_CAVEAT_TAXONOMY_ID,
        taxonomy_version=EA_ROFRS_CAVEAT_TAXONOMY_VERSION,
    )
