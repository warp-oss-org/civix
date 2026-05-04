"""NYC LL97 CBL dataset-level source caveats."""

from __future__ import annotations

from enum import StrEnum
from typing import Final

from civix.core.taxonomy.models.category import CategoryRef

LL97_CAVEAT_TAXONOMY_ID: Final[str] = "nyc-ll97-source-caveats"
LL97_CAVEAT_TAXONOMY_VERSION: Final[str] = "2026-05-03"
# Stable per-row anchor for dataset-level caveats. Each row carries BBL+BIN,
# so the BIN column is documented as the source field for caveats that apply
# universally to a row by virtue of dataset identity.
LL97_CAVEAT_SOURCE_FIELD: Final[str] = "bin"


class NycLl97Caveat(StrEnum):
    """Dataset-level caveats published or implied by DOB for CBL rows."""

    PRELIMINARY_LIST_REFERENCE_ONLY = "preliminary_list_reference_only"
    OWNER_DISPUTABLE = "owner_disputable"
    ADDRESS_AND_GSF_BBL_LEVEL_NOT_BIN_LEVEL = "address_and_gsf_bbl_level_not_bin_level"
    NO_INDEPENDENT_AUDIT = "no_independent_audit"
    PATHWAY_CODE_PUBLISHED_WITHOUT_NUMERIC_LIMIT = "pathway_code_published_without_numeric_limit"


_CAVEAT_LABELS: Final[dict[NycLl97Caveat, str]] = {
    NycLl97Caveat.PRELIMINARY_LIST_REFERENCE_ONLY: (
        "DOB Publishes The Covered Buildings List As Reference Only"
    ),
    NycLl97Caveat.OWNER_DISPUTABLE: (
        "Owners May Dispute Inclusion Or Exclusion Through The DOB Helpdesk"
    ),
    NycLl97Caveat.ADDRESS_AND_GSF_BBL_LEVEL_NOT_BIN_LEVEL: (
        "DOF Address And Gross Square Footage Are Tax-Lot Totals, Not Per-Building"
    ),
    NycLl97Caveat.NO_INDEPENDENT_AUDIT: (
        "DOB Records Are Not Independently Audited Before Publication"
    ),
    NycLl97Caveat.PATHWAY_CODE_PUBLISHED_WITHOUT_NUMERIC_LIMIT: (
        "Compliance Pathway Code Published Without Per-Row Numeric Emissions Limit"
    ),
}


def nyc_ll97_caveat_categories(
    caveats: tuple[NycLl97Caveat, ...] = tuple(NycLl97Caveat),
) -> tuple[CategoryRef, ...]:
    """Build source caveat category references for NYC LL97 CBL records."""
    return tuple(_caveat_category(caveat) for caveat in caveats)


def _caveat_category(caveat: NycLl97Caveat) -> CategoryRef:
    return CategoryRef(
        code=caveat.value,
        label=_CAVEAT_LABELS[caveat],
        taxonomy_id=LL97_CAVEAT_TAXONOMY_ID,
        taxonomy_version=LL97_CAVEAT_TAXONOMY_VERSION,
    )
