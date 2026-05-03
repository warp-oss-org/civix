"""British Geological Survey GeoSure Basic source slice."""

from civix.domains.hazard_risk.adapters.sources.gb.bgs_geosure_basic.adapter import (
    BGS_GEOSURE_BASIC_DATASET_ID,
    BGS_GEOSURE_BASIC_DATASET_PAGE_URL,
    BGS_GEOSURE_BASIC_SOURCE_SCOPE,
    GB_JURISDICTION,
    SOURCE_ID,
    BgsGeosureBasicAdapter,
    BgsGeosureBasicFetchConfig,
)
from civix.domains.hazard_risk.adapters.sources.gb.bgs_geosure_basic.caveats import (
    BGS_GEOSURE_BASIC_CAVEAT_TAXONOMY_ID,
    BGS_GEOSURE_BASIC_CAVEAT_TAXONOMY_VERSION,
    BGS_GEOSURE_BASIC_METADATA_SOURCE_FIELD,
    BgsGeosureBasicCaveat,
    bgs_geosure_basic_caveat_categories,
)
from civix.domains.hazard_risk.adapters.sources.gb.bgs_geosure_basic.mapper import (
    AREA_MAPPER_ID,
    AREA_MAPPER_VERSION,
    METHODOLOGY_LABEL,
    SCORES_MAPPER_ID,
    SCORES_MAPPER_VERSION,
    BgsGeosureBasicAreaMapper,
    BgsGeosureBasicScoresMapper,
)
from civix.domains.hazard_risk.adapters.sources.gb.bgs_geosure_basic.schema import (
    BGS_GEOSURE_BASIC_FIELDS,
    BGS_GEOSURE_BASIC_SCHEMA,
    BGS_GEOSURE_BASIC_SCHEMA_VERSION,
    BGS_GEOSURE_BASIC_TAXONOMIES,
    BGS_GEOSURE_BASIC_TAXONOMY_VERSION,
)

__all__ = [
    "AREA_MAPPER_ID",
    "AREA_MAPPER_VERSION",
    "BGS_GEOSURE_BASIC_CAVEAT_TAXONOMY_ID",
    "BGS_GEOSURE_BASIC_CAVEAT_TAXONOMY_VERSION",
    "BGS_GEOSURE_BASIC_DATASET_ID",
    "BGS_GEOSURE_BASIC_FIELDS",
    "BGS_GEOSURE_BASIC_DATASET_PAGE_URL",
    "BGS_GEOSURE_BASIC_METADATA_SOURCE_FIELD",
    "BGS_GEOSURE_BASIC_SCHEMA",
    "BGS_GEOSURE_BASIC_SCHEMA_VERSION",
    "BGS_GEOSURE_BASIC_SOURCE_SCOPE",
    "BGS_GEOSURE_BASIC_TAXONOMIES",
    "BGS_GEOSURE_BASIC_TAXONOMY_VERSION",
    "GB_JURISDICTION",
    "METHODOLOGY_LABEL",
    "SCORES_MAPPER_ID",
    "SCORES_MAPPER_VERSION",
    "SOURCE_ID",
    "BgsGeosureBasicAdapter",
    "BgsGeosureBasicAreaMapper",
    "BgsGeosureBasicCaveat",
    "BgsGeosureBasicFetchConfig",
    "BgsGeosureBasicScoresMapper",
    "bgs_geosure_basic_caveat_categories",
]
