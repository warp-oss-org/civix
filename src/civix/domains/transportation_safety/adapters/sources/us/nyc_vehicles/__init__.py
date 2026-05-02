"""NYC Motor Vehicle Collisions - Vehicles source package."""

from civix.domains.transportation_safety.adapters.sources.us.nyc_vehicles.adapter import (
    DEFAULT_BASE_URL,
    DEFAULT_PAGE_SIZE,
    NYC_JURISDICTION,
    NYC_VEHICLES_DATASET_CONFIG,
    NYC_VEHICLES_DATASET_ID,
    NYC_VEHICLES_RELEASE_CAVEATS,
    NYC_VEHICLES_SOURCE_SCOPE,
    SOCRATA_ORDER,
    SOURCE_ID,
    NycVehiclesAdapter,
)
from civix.domains.transportation_safety.adapters.sources.us.nyc_vehicles.mapper import (
    MAPPER_ID,
    MAPPER_VERSION,
    NycVehiclesMapper,
)
from civix.domains.transportation_safety.adapters.sources.us.nyc_vehicles.schema import (
    NYC_VEHICLES_SCHEMA,
    NYC_VEHICLES_TAXONOMIES,
)

__all__ = [
    "DEFAULT_BASE_URL",
    "DEFAULT_PAGE_SIZE",
    "MAPPER_ID",
    "MAPPER_VERSION",
    "NYC_JURISDICTION",
    "NYC_VEHICLES_DATASET_CONFIG",
    "NYC_VEHICLES_DATASET_ID",
    "NYC_VEHICLES_RELEASE_CAVEATS",
    "NYC_VEHICLES_SCHEMA",
    "NYC_VEHICLES_SOURCE_SCOPE",
    "NYC_VEHICLES_TAXONOMIES",
    "NycVehiclesAdapter",
    "NycVehiclesMapper",
    "SOCRATA_ORDER",
    "SOURCE_ID",
]
