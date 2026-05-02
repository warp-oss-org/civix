"""NYC Motor Vehicle Collisions - Crashes source package."""

from civix.domains.transportation_safety.adapters.sources.us.nyc_crashes.adapter import (
    DEFAULT_BASE_URL,
    DEFAULT_PAGE_SIZE,
    NYC_COLLISIONS_RELEASE_CAVEATS,
    NYC_COLLISIONS_SOURCE_SCOPE,
    NYC_CRASHES_DATASET_CONFIG,
    NYC_CRASHES_DATASET_ID,
    NYC_JURISDICTION,
    SOCRATA_ORDER,
    SOURCE_ID,
)
from civix.domains.transportation_safety.adapters.sources.us.nyc_crashes.mapper import (
    MAPPER_ID,
    MAPPER_VERSION,
    NycCrashesMapper,
)
from civix.domains.transportation_safety.adapters.sources.us.nyc_crashes.schema import (
    NYC_CRASHES_SCHEMA,
    NYC_CRASHES_TAXONOMIES,
)

__all__ = [
    "DEFAULT_BASE_URL",
    "DEFAULT_PAGE_SIZE",
    "MAPPER_ID",
    "MAPPER_VERSION",
    "NYC_COLLISIONS_RELEASE_CAVEATS",
    "NYC_COLLISIONS_SOURCE_SCOPE",
    "NYC_CRASHES_DATASET_CONFIG",
    "NYC_CRASHES_DATASET_ID",
    "NYC_CRASHES_SCHEMA",
    "NYC_CRASHES_TAXONOMIES",
    "NYC_JURISDICTION",
    "NycCrashesMapper",
    "SOCRATA_ORDER",
    "SOURCE_ID",
]
