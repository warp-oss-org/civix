"""NYC Motor Vehicle Collisions - Person source package."""

from civix.domains.transportation_safety.adapters.sources.us.nyc_persons.adapter import (
    DEFAULT_BASE_URL,
    DEFAULT_PAGE_SIZE,
    NYC_JURISDICTION,
    NYC_PERSONS_DATASET_CONFIG,
    NYC_PERSONS_DATASET_ID,
    NYC_PERSONS_RELEASE_CAVEATS,
    NYC_PERSONS_SOURCE_SCOPE,
    SOCRATA_ORDER,
    SOURCE_ID,
)
from civix.domains.transportation_safety.adapters.sources.us.nyc_persons.mapper import (
    MAPPER_ID,
    MAPPER_VERSION,
    NycPersonsMapper,
)
from civix.domains.transportation_safety.adapters.sources.us.nyc_persons.schema import (
    NYC_PERSONS_SCHEMA,
    NYC_PERSONS_TAXONOMIES,
)

__all__ = [
    "DEFAULT_BASE_URL",
    "DEFAULT_PAGE_SIZE",
    "MAPPER_ID",
    "MAPPER_VERSION",
    "NYC_JURISDICTION",
    "NYC_PERSONS_DATASET_CONFIG",
    "NYC_PERSONS_DATASET_ID",
    "NYC_PERSONS_RELEASE_CAVEATS",
    "NYC_PERSONS_SCHEMA",
    "NYC_PERSONS_SOURCE_SCOPE",
    "NYC_PERSONS_TAXONOMIES",
    "NycPersonsMapper",
    "SOCRATA_ORDER",
    "SOURCE_ID",
]
