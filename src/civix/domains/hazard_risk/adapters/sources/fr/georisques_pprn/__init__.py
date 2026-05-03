"""France Georisques GASPAR PPRN source slice."""

from civix.domains.hazard_risk.adapters.sources.fr.georisques_pprn.adapter import (
    FR_JURISDICTION,
    GEORISQUES_PPRN_CSV_URL,
    GEORISQUES_PPRN_DATASET_ID,
    GEORISQUES_PPRN_SOURCE_SCOPE,
    SOURCE_ID,
    GeorisquesPprnAdapter,
    GeorisquesPprnFetchConfig,
)
from civix.domains.hazard_risk.adapters.sources.fr.georisques_pprn.caveats import (
    GEORISQUES_PPRN_CAVEAT_TAXONOMY_ID,
    GEORISQUES_PPRN_CAVEAT_TAXONOMY_VERSION,
    GEORISQUES_PPRN_METADATA_SOURCE_FIELD,
    GeorisquesPprnCaveat,
    georisques_pprn_caveat_categories,
)
from civix.domains.hazard_risk.adapters.sources.fr.georisques_pprn.mapper import (
    AREA_MAPPER_ID,
    AREA_MAPPER_VERSION,
    ZONE_MAPPER_ID,
    ZONE_MAPPER_VERSION,
    GeorisquesPprnAreaMapper,
    GeorisquesPprnZoneMapper,
)
from civix.domains.hazard_risk.adapters.sources.fr.georisques_pprn.schema import (
    GEORISQUES_PPRN_FIELDS,
    GEORISQUES_PPRN_MODEL_TAXONOMY,
    GEORISQUES_PPRN_RISK_FAMILY_TAXONOMY,
    GEORISQUES_PPRN_RISK_TAXONOMY,
    GEORISQUES_PPRN_SCHEMA,
    GEORISQUES_PPRN_SCHEMA_VERSION,
    GEORISQUES_PPRN_STATE_TAXONOMY,
    GEORISQUES_PPRN_SUBSTATE_TAXONOMY,
    GEORISQUES_PPRN_TAXONOMIES,
    GEORISQUES_PPRN_TAXONOMY_VERSION,
)

__all__ = [
    "AREA_MAPPER_ID",
    "AREA_MAPPER_VERSION",
    "FR_JURISDICTION",
    "GEORISQUES_PPRN_CAVEAT_TAXONOMY_ID",
    "GEORISQUES_PPRN_CAVEAT_TAXONOMY_VERSION",
    "GEORISQUES_PPRN_CSV_URL",
    "GEORISQUES_PPRN_DATASET_ID",
    "GEORISQUES_PPRN_FIELDS",
    "GEORISQUES_PPRN_METADATA_SOURCE_FIELD",
    "GEORISQUES_PPRN_MODEL_TAXONOMY",
    "GEORISQUES_PPRN_RISK_FAMILY_TAXONOMY",
    "GEORISQUES_PPRN_RISK_TAXONOMY",
    "GEORISQUES_PPRN_SCHEMA",
    "GEORISQUES_PPRN_SCHEMA_VERSION",
    "GEORISQUES_PPRN_SOURCE_SCOPE",
    "GEORISQUES_PPRN_STATE_TAXONOMY",
    "GEORISQUES_PPRN_SUBSTATE_TAXONOMY",
    "GEORISQUES_PPRN_TAXONOMIES",
    "GEORISQUES_PPRN_TAXONOMY_VERSION",
    "SOURCE_ID",
    "ZONE_MAPPER_ID",
    "ZONE_MAPPER_VERSION",
    "GeorisquesPprnAdapter",
    "GeorisquesPprnAreaMapper",
    "GeorisquesPprnCaveat",
    "GeorisquesPprnFetchConfig",
    "GeorisquesPprnZoneMapper",
    "georisques_pprn_caveat_categories",
]
