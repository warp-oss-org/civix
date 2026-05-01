"""Chicago Traffic Crashes - Vehicles source package."""

from civix.domains.transportation_safety.adapters.sources.us.chicago_vehicles.adapter import (
    CHICAGO_JURISDICTION,
    CHICAGO_VEHICLES_DATASET_CONFIG,
    CHICAGO_VEHICLES_DATASET_ID,
    DEFAULT_BASE_URL,
    DEFAULT_PAGE_SIZE,
    SOCRATA_ORDER,
    SOURCE_ID,
)
from civix.domains.transportation_safety.adapters.sources.us.chicago_vehicles.mapper import (
    MAPPER_ID,
    MAPPER_VERSION,
    ChicagoVehiclesMapper,
)
from civix.domains.transportation_safety.adapters.sources.us.chicago_vehicles.schema import (
    CHICAGO_VEHICLES_SCHEMA,
    CHICAGO_VEHICLES_TAXONOMIES,
)

__all__ = [
    "CHICAGO_JURISDICTION",
    "CHICAGO_VEHICLES_DATASET_CONFIG",
    "CHICAGO_VEHICLES_DATASET_ID",
    "CHICAGO_VEHICLES_SCHEMA",
    "CHICAGO_VEHICLES_TAXONOMIES",
    "DEFAULT_BASE_URL",
    "DEFAULT_PAGE_SIZE",
    "MAPPER_ID",
    "MAPPER_VERSION",
    "SOCRATA_ORDER",
    "SOURCE_ID",
    "ChicagoVehiclesMapper",
]
