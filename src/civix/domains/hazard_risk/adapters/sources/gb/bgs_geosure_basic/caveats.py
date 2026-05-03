"""BGS GeoSure Basic source caveats."""

from __future__ import annotations

from enum import StrEnum
from typing import Final

from civix.core.taxonomy.models.category import CategoryRef

BGS_GEOSURE_BASIC_CAVEAT_TAXONOMY_ID: Final[str] = "bgs-geosure-basic-source-caveats"
BGS_GEOSURE_BASIC_CAVEAT_TAXONOMY_VERSION: Final[str] = "2026-05-03"
BGS_GEOSURE_BASIC_METADATA_SOURCE_FIELD: Final[str] = "product_url"


class BgsGeosureBasicCaveat(StrEnum):
    """Known caveats for GeoSure Basic records."""

    GENERALIZED_GRID = "generalized_grid"
    COMBINED_GEOHAZARD_MODEL = "combined_geohazard_model"
    NOT_LOCAL_GROUND_INVESTIGATION = "not_local_ground_investigation"


_CAVEAT_LABELS: Final[dict[BgsGeosureBasicCaveat, str]] = {
    BgsGeosureBasicCaveat.GENERALIZED_GRID: "Generalized 5km Grid",
    BgsGeosureBasicCaveat.COMBINED_GEOHAZARD_MODEL: "Combined GeoHazard Model",
    BgsGeosureBasicCaveat.NOT_LOCAL_GROUND_INVESTIGATION: "Not A Local Ground Investigation",
}


def bgs_geosure_basic_caveat_categories(
    caveats: tuple[BgsGeosureBasicCaveat, ...] = tuple(BgsGeosureBasicCaveat),
) -> tuple[CategoryRef, ...]:
    """Build source caveat category references for GeoSure Basic records."""
    return tuple(_caveat_category(caveat) for caveat in caveats)


def _caveat_category(caveat: BgsGeosureBasicCaveat) -> CategoryRef:
    return CategoryRef(
        code=caveat.value,
        label=_CAVEAT_LABELS[caveat],
        taxonomy_id=BGS_GEOSURE_BASIC_CAVEAT_TAXONOMY_ID,
        taxonomy_version=BGS_GEOSURE_BASIC_CAVEAT_TAXONOMY_VERSION,
    )
