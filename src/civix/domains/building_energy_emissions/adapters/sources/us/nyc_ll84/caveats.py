"""NYC LL84 dataset-level source caveats."""

from __future__ import annotations

from enum import StrEnum
from typing import Final

from civix.core.taxonomy.models.category import CategoryRef

LL84_CAVEAT_TAXONOMY_ID: Final[str] = "nyc-ll84-source-caveats"
LL84_CAVEAT_TAXONOMY_VERSION: Final[str] = "2026-05-03"
# Stable per-row anchor for dataset-level caveats. The caveats apply to
# every published row by virtue of dataset identity, so the source field
# is documented as the row's source key rather than any value-bearing
# column.
LL84_CAVEAT_SOURCE_FIELD: Final[str] = "property_id"


class NycLl84Caveat(StrEnum):
    """Dataset-level caveats published or implied by NYC for LL84 rows."""

    SOURCE_REPUBLISHED_FROM_ESPM = "source_republished_from_espm"
    SELF_REPORTED_NOT_AUDITED = "self_reported_not_audited"
    CLASSIFIED_LOCATIONS_REDACTED = "classified_locations_redacted"
    DOB_DATA_QUALITY_CHECKED_BUT_NOT_VALIDATED = "dob_data_quality_checked_but_not_validated"
    LICENSE_NOT_SPECIFIED = "license_not_specified"


_CAVEAT_LABELS: Final[dict[NycLl84Caveat, str]] = {
    NycLl84Caveat.SOURCE_REPUBLISHED_FROM_ESPM: (
        "Values Republished From ENERGY STAR Portfolio Manager"
    ),
    NycLl84Caveat.SELF_REPORTED_NOT_AUDITED: ("Owner Self-Reported And Not Independently Audited"),
    NycLl84Caveat.CLASSIFIED_LOCATIONS_REDACTED: (
        "Classified Public-Sector Locations Are Redacted In Address Fields"
    ),
    NycLl84Caveat.DOB_DATA_QUALITY_CHECKED_BUT_NOT_VALIDATED: (
        "DOB Data Quality Checker Run But Values Not Validated"
    ),
    NycLl84Caveat.LICENSE_NOT_SPECIFIED: "Data.gov Lists License Not Specified",
}


def nyc_ll84_caveat_categories(
    caveats: tuple[NycLl84Caveat, ...] = tuple(NycLl84Caveat),
) -> tuple[CategoryRef, ...]:
    """Build source caveat category references for NYC LL84 records."""
    return tuple(_caveat_category(caveat) for caveat in caveats)


def _caveat_category(caveat: NycLl84Caveat) -> CategoryRef:
    return CategoryRef(
        code=caveat.value,
        label=_CAVEAT_LABELS[caveat],
        taxonomy_id=LL84_CAVEAT_TAXONOMY_ID,
        taxonomy_version=LL84_CAVEAT_TAXONOMY_VERSION,
    )
