"""NRCan Flood Susceptibility Index source caveats."""

from __future__ import annotations

from enum import StrEnum
from typing import Final

from civix.core.taxonomy.models.category import CategoryRef

NRCAN_FSI_CAVEAT_TAXONOMY_ID: Final[str] = "nrcan-fsi-source-caveats"
NRCAN_FSI_CAVEAT_TAXONOMY_VERSION: Final[str] = "2026-05-03"
NRCAN_FSI_METADATA_SOURCE_FIELD: Final[str] = "methodology_url"


class NrcanFsiCaveat(StrEnum):
    """Known NRCan caveats for Flood Susceptibility Index records."""

    SCREENING_DATA = "screening_data"
    NOT_SITE_LEVEL = "not_site_level"
    MACHINE_LEARNING_MODEL = "machine_learning_model"


_CAVEAT_LABELS: Final[dict[NrcanFsiCaveat, str]] = {
    NrcanFsiCaveat.SCREENING_DATA: "Screening Data",
    NrcanFsiCaveat.NOT_SITE_LEVEL: "Not Recommended For Site-Level Assessment",
    NrcanFsiCaveat.MACHINE_LEARNING_MODEL: "Machine Learning Susceptibility Model",
}


def nrcan_fsi_caveat_categories(
    caveats: tuple[NrcanFsiCaveat, ...] = tuple(NrcanFsiCaveat),
) -> tuple[CategoryRef, ...]:
    """Build source caveat category references for NRCan FSI records."""
    return tuple(_caveat_category(caveat) for caveat in caveats)


def _caveat_category(caveat: NrcanFsiCaveat) -> CategoryRef:
    return CategoryRef(
        code=caveat.value,
        label=_CAVEAT_LABELS[caveat],
        taxonomy_id=NRCAN_FSI_CAVEAT_TAXONOMY_ID,
        taxonomy_version=NRCAN_FSI_CAVEAT_TAXONOMY_VERSION,
    )
