"""Natural Resources Canada Flood Susceptibility Index source slice."""

from civix.domains.hazard_risk.adapters.sources.ca.nrcan_fsi.adapter import (
    CA_JURISDICTION,
    NRCAN_FSI_DATASET_ID,
    NRCAN_FSI_DATASET_PAGE_URL,
    NRCAN_FSI_SOURCE_SCOPE,
    SOURCE_ID,
    NrcanFsiAdapter,
    NrcanFsiFetchConfig,
)
from civix.domains.hazard_risk.adapters.sources.ca.nrcan_fsi.caveats import (
    NRCAN_FSI_CAVEAT_TAXONOMY_ID,
    NRCAN_FSI_CAVEAT_TAXONOMY_VERSION,
    NRCAN_FSI_METADATA_SOURCE_FIELD,
    NrcanFsiCaveat,
    nrcan_fsi_caveat_categories,
)
from civix.domains.hazard_risk.adapters.sources.ca.nrcan_fsi.mapper import (
    AREA_MAPPER_ID,
    AREA_MAPPER_VERSION,
    METHODOLOGY_LABEL,
    SCORES_MAPPER_ID,
    SCORES_MAPPER_VERSION,
    NrcanFsiAreaMapper,
    NrcanFsiScoresMapper,
)
from civix.domains.hazard_risk.adapters.sources.ca.nrcan_fsi.schema import (
    NRCAN_FSI_FIELDS,
    NRCAN_FSI_SCHEMA,
    NRCAN_FSI_SCHEMA_VERSION,
    NRCAN_FSI_TAXONOMIES,
    NRCAN_FSI_TAXONOMY_VERSION,
)

__all__ = [
    "AREA_MAPPER_ID",
    "AREA_MAPPER_VERSION",
    "CA_JURISDICTION",
    "METHODOLOGY_LABEL",
    "NRCAN_FSI_CAVEAT_TAXONOMY_ID",
    "NRCAN_FSI_CAVEAT_TAXONOMY_VERSION",
    "NRCAN_FSI_DATASET_ID",
    "NRCAN_FSI_FIELDS",
    "NRCAN_FSI_DATASET_PAGE_URL",
    "NRCAN_FSI_METADATA_SOURCE_FIELD",
    "NRCAN_FSI_SCHEMA",
    "NRCAN_FSI_SCHEMA_VERSION",
    "NRCAN_FSI_SOURCE_SCOPE",
    "NRCAN_FSI_TAXONOMIES",
    "NRCAN_FSI_TAXONOMY_VERSION",
    "SCORES_MAPPER_ID",
    "SCORES_MAPPER_VERSION",
    "SOURCE_ID",
    "NrcanFsiAdapter",
    "NrcanFsiAreaMapper",
    "NrcanFsiCaveat",
    "NrcanFsiFetchConfig",
    "NrcanFsiScoresMapper",
    "nrcan_fsi_caveat_categories",
]
