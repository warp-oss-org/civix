"""Source caveats for England FCERM scheme-allocation rows."""

from __future__ import annotations

from enum import StrEnum
from typing import Final

from civix.core.mapping.parsers import slugify
from civix.core.taxonomy.models.category import CategoryRef

ENGLAND_FCERM_TAXONOMY_VERSION: Final[str] = "2026-05-03"
ENGLAND_FCERM_CAVEAT_TAXONOMY_ID: Final[str] = "england-fcerm-source-caveat"
ENGLAND_FCERM_READ_ME_FIELD: Final[str] = "Read me"


class EnglandFcermCaveat(StrEnum):
    """Documented source semantics that should travel with mapped records."""

    SCHEME_LEVEL_LOCATION = (
        "investment programme data is only accurate at a scheme or community level "
        "and may not represent the actual FCERM scheme location"
    )
    COMPILED_INFORMATION = "investment programme data is accurate only to when compiled"
    FUNDING_AND_TIMELINES_CHANGE = "funding and timelines are liable to change"
    YEAR_ONLY_INVESTMENT = "scheme investment is for 2026 to 2027 only"
    LIVE_PROJECTS = "projects are live and subject to change"
    FUTURE_FUNDING_REVIEW = "funding for projects beyond 26/27 is subject to review"
    ROW_DERIVED_IDENTIFIERS = (
        "source record identifiers are derived from row content and row number because "
        "the workbook does not publish a stable scheme identifier"
    )


def england_fcerm_caveat_category(caveat: EnglandFcermCaveat) -> CategoryRef:
    return CategoryRef(
        code=slugify(caveat.name),
        label=caveat.value,
        taxonomy_id=ENGLAND_FCERM_CAVEAT_TAXONOMY_ID,
        taxonomy_version=ENGLAND_FCERM_TAXONOMY_VERSION,
    )


def england_fcerm_caveat_categories() -> tuple[CategoryRef, ...]:
    return tuple(england_fcerm_caveat_category(caveat) for caveat in EnglandFcermCaveat)
