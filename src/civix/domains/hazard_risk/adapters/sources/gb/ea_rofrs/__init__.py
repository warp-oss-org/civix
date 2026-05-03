"""Environment Agency RoFRS source slice."""

from civix.domains.hazard_risk.adapters.sources.gb.ea_rofrs.adapter import (
    EA_ROFRS_DATASET_ID,
    EA_ROFRS_DATASET_PAGE_URL,
    EA_ROFRS_SOURCE_SCOPE,
    GB_ENGLAND_JURISDICTION,
    SOURCE_ID,
    EaRofrsAdapter,
    EaRofrsFetchConfig,
)
from civix.domains.hazard_risk.adapters.sources.gb.ea_rofrs.caveats import (
    EA_ROFRS_CAVEAT_TAXONOMY_ID,
    EA_ROFRS_CAVEAT_TAXONOMY_VERSION,
    EA_ROFRS_METADATA_SOURCE_FIELD,
    EaRofrsCaveat,
    ea_rofrs_caveat_categories,
)
from civix.domains.hazard_risk.adapters.sources.gb.ea_rofrs.mapper import (
    ZONE_MAPPER_ID,
    ZONE_MAPPER_VERSION,
    EaRofrsZoneMapper,
)
from civix.domains.hazard_risk.adapters.sources.gb.ea_rofrs.schema import (
    EA_ROFRS_FIELDS,
    EA_ROFRS_SCHEMA,
    EA_ROFRS_SCHEMA_VERSION,
    EA_ROFRS_TAXONOMIES,
    EA_ROFRS_TAXONOMY_VERSION,
)

__all__ = [
    "EA_ROFRS_CAVEAT_TAXONOMY_ID",
    "EA_ROFRS_CAVEAT_TAXONOMY_VERSION",
    "EA_ROFRS_DATASET_ID",
    "EA_ROFRS_FIELDS",
    "EA_ROFRS_DATASET_PAGE_URL",
    "EA_ROFRS_METADATA_SOURCE_FIELD",
    "EA_ROFRS_SCHEMA",
    "EA_ROFRS_SCHEMA_VERSION",
    "EA_ROFRS_SOURCE_SCOPE",
    "EA_ROFRS_TAXONOMIES",
    "EA_ROFRS_TAXONOMY_VERSION",
    "GB_ENGLAND_JURISDICTION",
    "SOURCE_ID",
    "ZONE_MAPPER_ID",
    "ZONE_MAPPER_VERSION",
    "EaRofrsAdapter",
    "EaRofrsCaveat",
    "EaRofrsFetchConfig",
    "EaRofrsZoneMapper",
    "ea_rofrs_caveat_categories",
]
