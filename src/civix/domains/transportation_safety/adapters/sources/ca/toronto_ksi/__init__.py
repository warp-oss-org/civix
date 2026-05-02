"""Toronto KSI source package."""

from civix.domains.transportation_safety.adapters.sources.ca.toronto_ksi.adapter import (
    DEFAULT_BASE_URL,
    DEFAULT_PAGE_SIZE,
    SOURCE_ID,
    TORONTO_KSI_DATASET_CONFIG,
    TORONTO_KSI_DATASET_ID,
    TORONTO_KSI_JURISDICTION,
    TORONTO_KSI_SOURCE_RECORD_ID_FIELDS,
)
from civix.domains.transportation_safety.adapters.sources.ca.toronto_ksi.mapper import (
    TorontoKsiGroupedMapper,
    TorontoKsiGroupResult,
)
from civix.domains.transportation_safety.adapters.sources.ca.toronto_ksi.schema import (
    TORONTO_KSI_SCHEMA,
    TORONTO_KSI_TAXONOMIES,
)

__all__ = [
    "DEFAULT_BASE_URL",
    "DEFAULT_PAGE_SIZE",
    "SOURCE_ID",
    "TORONTO_KSI_DATASET_CONFIG",
    "TORONTO_KSI_DATASET_ID",
    "TORONTO_KSI_JURISDICTION",
    "TORONTO_KSI_SCHEMA",
    "TORONTO_KSI_SOURCE_RECORD_ID_FIELDS",
    "TORONTO_KSI_TAXONOMIES",
    "TorontoKsiGroupedMapper",
    "TorontoKsiGroupResult",
]
