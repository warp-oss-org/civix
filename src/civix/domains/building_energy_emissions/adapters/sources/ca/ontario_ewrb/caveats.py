"""Ontario EWRB dataset-level source caveats."""

from __future__ import annotations

from enum import StrEnum
from typing import Final

from civix.core.taxonomy.models.category import CategoryRef

EWRB_CAVEAT_TAXONOMY_ID: Final[str] = "ontario-ewrb-source-caveats"
EWRB_CAVEAT_TAXONOMY_VERSION: Final[str] = "2026-05-03"
# Stable per-row anchor for dataset-level caveats. Every row carries an
# EWRB_ID, so the EWRB_ID column is documented as the source field for
# caveats that apply universally to a row by virtue of dataset identity.
EWRB_CAVEAT_SOURCE_FIELD: Final[str] = "ewrb_id"


class OntarioEwrbCaveat(StrEnum):
    """Dataset-level caveats published or implied by Ontario for EWRB rows."""

    OWNER_REPORTED_NOT_CLEANSED = "owner_reported_not_cleansed"
    SUPPRESSED_TOTAL_METRICS_AND_FLOOR_AREA = "suppressed_total_metrics_and_floor_area"
    PARTIAL_POSTAL_DISCLOSURE_FSA_ONLY = "partial_postal_disclosure_fsa_only"
    NRCAN_SOURCE_FACTOR_CHANGE_2023_08_28 = "nrcan_source_factor_change_2023_08_28"
    OPEN_GOVERNMENT_LICENCE_ONTARIO = "open_government_licence_ontario"


_CAVEAT_LABELS: Final[dict[OntarioEwrbCaveat, str]] = {
    OntarioEwrbCaveat.OWNER_REPORTED_NOT_CLEANSED: (
        "Data Reported By Owners Or Agents And Not Cleansed By Ontario"
    ),
    OntarioEwrbCaveat.SUPPRESSED_TOTAL_METRICS_AND_FLOOR_AREA: (
        "Total Energy, Water, GHG, And Gross Floor Area Withheld From Disclosure"
    ),
    OntarioEwrbCaveat.PARTIAL_POSTAL_DISCLOSURE_FSA_ONLY: (
        "Only The First Three Postal Code Characters (FSA) Are Disclosed"
    ),
    OntarioEwrbCaveat.NRCAN_SOURCE_FACTOR_CHANGE_2023_08_28: (
        "NRCan Source Factors Changed 2023-08-28; Submissions Flagged Per Row"
    ),
    OntarioEwrbCaveat.OPEN_GOVERNMENT_LICENCE_ONTARIO: (
        "Published Under The Open Government Licence - Ontario"
    ),
}


def ontario_ewrb_caveat_categories(
    caveats: tuple[OntarioEwrbCaveat, ...] = tuple(OntarioEwrbCaveat),
) -> tuple[CategoryRef, ...]:
    """Build source caveat category references for Ontario EWRB records."""
    return tuple(_caveat_category(caveat) for caveat in caveats)


def _caveat_category(caveat: OntarioEwrbCaveat) -> CategoryRef:
    return CategoryRef(
        code=caveat.value,
        label=_CAVEAT_LABELS[caveat],
        taxonomy_id=EWRB_CAVEAT_TAXONOMY_ID,
        taxonomy_version=EWRB_CAVEAT_TAXONOMY_VERSION,
    )
