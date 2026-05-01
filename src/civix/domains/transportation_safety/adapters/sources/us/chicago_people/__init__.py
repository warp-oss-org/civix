"""Chicago Traffic Crashes - People source package."""

from civix.domains.transportation_safety.adapters.sources.us.chicago_people.adapter import (
    CHICAGO_JURISDICTION,
    CHICAGO_PEOPLE_DATASET_CONFIG,
    CHICAGO_PEOPLE_DATASET_ID,
    DEFAULT_BASE_URL,
    DEFAULT_PAGE_SIZE,
    SOCRATA_ORDER,
    SOURCE_ID,
)
from civix.domains.transportation_safety.adapters.sources.us.chicago_people.mapper import (
    MAPPER_ID,
    MAPPER_VERSION,
    ChicagoPeopleMapper,
)
from civix.domains.transportation_safety.adapters.sources.us.chicago_people.schema import (
    CHICAGO_PEOPLE_SCHEMA,
    CHICAGO_PEOPLE_TAXONOMIES,
)

__all__ = [
    "CHICAGO_JURISDICTION",
    "CHICAGO_PEOPLE_DATASET_CONFIG",
    "CHICAGO_PEOPLE_DATASET_ID",
    "CHICAGO_PEOPLE_SCHEMA",
    "CHICAGO_PEOPLE_TAXONOMIES",
    "DEFAULT_BASE_URL",
    "DEFAULT_PAGE_SIZE",
    "MAPPER_ID",
    "MAPPER_VERSION",
    "SOCRATA_ORDER",
    "SOURCE_ID",
    "ChicagoPeopleMapper",
]
