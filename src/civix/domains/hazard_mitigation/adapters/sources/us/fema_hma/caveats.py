"""OpenFEMA HMA source caveat categories."""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Final

from civix.core.taxonomy.models.category import CategoryRef

OPENFEMA_HMA_CAVEAT_TAXONOMY_ID: Final[str] = "openfema-hma-source-caveats"
OPENFEMA_HMA_TAXONOMY_VERSION: Final[str] = "2026-05-02"
OPENFEMA_METADATA_DESCRIPTION_FIELD: Final[str] = "source.metadata.description"


class OpenFemaHmaCaveat(StrEnum):
    """Known caveats FEMA publishes for HMA datasets."""

    PII_REMOVED = "pii_removed"
    HUMAN_ERROR_POSSIBLE = "human_error_possible"
    MISSING_APPLICANT_DATA = "missing_applicant_data"
    NOT_OFFICIAL_FINANCIAL_REPORTING = "not_official_financial_reporting"


_CAVEAT_LABELS: Final[dict[OpenFemaHmaCaveat, str]] = {
    OpenFemaHmaCaveat.PII_REMOVED: "PII Removed",
    OpenFemaHmaCaveat.HUMAN_ERROR_POSSIBLE: "Human Error Possible",
    OpenFemaHmaCaveat.MISSING_APPLICANT_DATA: "Missing Applicant Or Subapplicant Data",
    OpenFemaHmaCaveat.NOT_OFFICIAL_FINANCIAL_REPORTING: (
        "Not Official Federal Financial Reporting"
    ),
}
_KNOWN_CAVEAT_SENTENCE_MAP: Final[dict[str, OpenFemaHmaCaveat]] = {
    (
        "Sensitive information, such as Personally Identifiable Information (PII), "
        "has been removed to protect privacy."
    ): OpenFemaHmaCaveat.PII_REMOVED,
    (
        "This dataset comes from the source system mentioned above and is subject to "
        "a small percentage of human error."
    ): OpenFemaHmaCaveat.HUMAN_ERROR_POSSIBLE,
    (
        "In some cases, data was not provided by the subapplicant, applicant, and/or "
        "entered into NEMIS Mitigation and eGrants."
    ): OpenFemaHmaCaveat.MISSING_APPLICANT_DATA,
    (
        "The financial information in this dataset is not derived from FEMA's official "
        "financial systems."
    ): OpenFemaHmaCaveat.NOT_OFFICIAL_FINANCIAL_REPORTING,
    (
        "Due to differences in reporting periods, status of obligations, and how "
        "business rules are applied, this financial information may differ slightly from "
        "official publication on public websites such as https://www.usaspending.gov."
    ): OpenFemaHmaCaveat.NOT_OFFICIAL_FINANCIAL_REPORTING,
    (
        "This dataset is not intended to be used for any official federal financial reporting."
    ): OpenFemaHmaCaveat.NOT_OFFICIAL_FINANCIAL_REPORTING,
}
_TERMS_MARKER: Final[str] = "FEMA's terms and conditions"


def openfema_hma_caveat_categories(
    caveats: tuple[OpenFemaHmaCaveat, ...] = tuple(OpenFemaHmaCaveat),
) -> tuple[CategoryRef, ...]:
    """Build source caveat category references for OpenFEMA HMA records."""
    return tuple(_caveat_category(caveat) for caveat in caveats)


def observed_openfema_hma_metadata_caveats(description: str) -> tuple[OpenFemaHmaCaveat, ...]:
    """Return known caveats from an OpenFEMA dataset description.

    This intentionally fails when the caveat paragraph contains a new sentence so that
    maintainers review and version the caveat category set instead of silently accepting it.
    """
    sentences = _caveat_sentences(description)
    observed: list[OpenFemaHmaCaveat] = []

    for sentence in sentences:
        caveat = _KNOWN_CAVEAT_SENTENCE_MAP.get(sentence)

        if caveat is None:
            raise ValueError(f"unrecognized OpenFEMA HMA caveat sentence: {sentence}")

        if caveat not in observed:
            observed.append(caveat)

    return tuple(observed)


def _caveat_category(caveat: OpenFemaHmaCaveat) -> CategoryRef:
    return CategoryRef(
        code=caveat.value,
        label=_CAVEAT_LABELS[caveat],
        taxonomy_id=OPENFEMA_HMA_CAVEAT_TAXONOMY_ID,
        taxonomy_version=OPENFEMA_HMA_TAXONOMY_VERSION,
    )


def _caveat_sentences(description: str) -> tuple[str, ...]:
    start = description.find("Sensitive information")

    if start < 0:
        raise ValueError("OpenFEMA HMA metadata description is missing caveat text")

    end = description.find(_TERMS_MARKER, start)
    caveat_text = description[start:] if end < 0 else description[start:end]
    sentences = tuple(
        sentence.strip()
        for sentence in re.split(r"(?<=[.!?])\s+", caveat_text.strip())
        if sentence.strip()
    )

    if not sentences:
        raise ValueError("OpenFEMA HMA metadata description is missing caveat sentences")

    return sentences
