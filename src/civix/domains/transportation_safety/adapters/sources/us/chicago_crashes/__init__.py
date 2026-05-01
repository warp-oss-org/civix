"""Chicago Traffic Crashes - Crashes source package."""

from civix.domains.transportation_safety.adapters.sources.us.chicago_crashes.adapter import (
    CHICAGO_CRASHES_DATASET_CONFIG,
    CHICAGO_CRASHES_DATASET_ID,
    CHICAGO_JURISDICTION,
    DEFAULT_BASE_URL,
    DEFAULT_PAGE_SIZE,
    SOCRATA_ORDER,
    SOURCE_ID,
)
from civix.domains.transportation_safety.adapters.sources.us.chicago_crashes.mapper import (
    MAPPER_ID,
    MAPPER_VERSION,
    ChicagoCrashesMapper,
)
from civix.domains.transportation_safety.adapters.sources.us.chicago_crashes.schema import (
    CHICAGO_CRASHES_SCHEMA,
    CHICAGO_CRASHES_TAXONOMIES,
)

__all__ = [
    "CHICAGO_CRASHES_DATASET_ID",
    "CHICAGO_CRASHES_DATASET_CONFIG",
    "CHICAGO_CRASHES_SCHEMA",
    "CHICAGO_CRASHES_TAXONOMIES",
    "CHICAGO_JURISDICTION",
    "DEFAULT_BASE_URL",
    "DEFAULT_PAGE_SIZE",
    "MAPPER_ID",
    "MAPPER_VERSION",
    "SOCRATA_ORDER",
    "SOURCE_ID",
    "ChicagoCrashesMapper",
]
