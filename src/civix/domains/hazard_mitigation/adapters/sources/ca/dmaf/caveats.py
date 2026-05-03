"""Source caveats for Canada DMAF project-list rows."""

from __future__ import annotations

from enum import StrEnum
from typing import Final

from civix.core.mapping.parsers import slugify
from civix.core.taxonomy.models.category import CategoryRef

CANADA_DMAF_TAXONOMY_VERSION: Final[str] = "2026-05-03"
CANADA_DMAF_CAVEAT_TAXONOMY_ID: Final[str] = "canada-dmaf-source-caveat"


class CanadaDmafCaveat(StrEnum):
    """Documented source semantics that should travel with mapped records."""

    FORECAST_CONSTRUCTION_DATES = "forecast construction dates are expected dates"
    TOTAL_ELIGIBLE_COST_LIFECYCLE = (
        "total eligible cost is not generally updated over the life of the project"
    )


def canada_dmaf_caveat_category(caveat: CanadaDmafCaveat) -> CategoryRef:
    return CategoryRef(
        code=slugify(caveat.name),
        label=caveat.value,
        taxonomy_id=CANADA_DMAF_CAVEAT_TAXONOMY_ID,
        taxonomy_version=CANADA_DMAF_TAXONOMY_VERSION,
    )
