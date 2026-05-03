"""Public Safety Canada FIFRA source slice."""

from civix.domains.hazard_risk.adapters.sources.ca.ps_fifra.adapter import (
    CA_JURISDICTION,
    PS_FIFRA_DATASET_ID,
    PS_FIFRA_DATASET_PAGE_URL,
    PS_FIFRA_SOURCE_SCOPE,
    SOURCE_ID,
    PsFifraAdapter,
    PsFifraFetchConfig,
)
from civix.domains.hazard_risk.adapters.sources.ca.ps_fifra.caveats import (
    PS_FIFRA_CAVEAT_TAXONOMY_ID,
    PS_FIFRA_CAVEAT_TAXONOMY_VERSION,
    PS_FIFRA_METADATA_SOURCE_FIELD,
    PsFifraCaveat,
    ps_fifra_caveat_categories,
)
from civix.domains.hazard_risk.adapters.sources.ca.ps_fifra.mapper import (
    ZONE_MAPPER_ID,
    ZONE_MAPPER_VERSION,
    PsFifraZoneMapper,
)
from civix.domains.hazard_risk.adapters.sources.ca.ps_fifra.schema import (
    PS_FIFRA_FIELDS,
    PS_FIFRA_SCHEMA,
    PS_FIFRA_SCHEMA_VERSION,
    PS_FIFRA_TAXONOMIES,
    PS_FIFRA_TAXONOMY_VERSION,
)

__all__ = [
    "CA_JURISDICTION",
    "PS_FIFRA_CAVEAT_TAXONOMY_ID",
    "PS_FIFRA_CAVEAT_TAXONOMY_VERSION",
    "PS_FIFRA_DATASET_ID",
    "PS_FIFRA_FIELDS",
    "PS_FIFRA_DATASET_PAGE_URL",
    "PS_FIFRA_METADATA_SOURCE_FIELD",
    "PS_FIFRA_SCHEMA",
    "PS_FIFRA_SCHEMA_VERSION",
    "PS_FIFRA_SOURCE_SCOPE",
    "PS_FIFRA_TAXONOMIES",
    "PS_FIFRA_TAXONOMY_VERSION",
    "SOURCE_ID",
    "ZONE_MAPPER_ID",
    "ZONE_MAPPER_VERSION",
    "PsFifraAdapter",
    "PsFifraCaveat",
    "PsFifraFetchConfig",
    "PsFifraZoneMapper",
    "ps_fifra_caveat_categories",
]
