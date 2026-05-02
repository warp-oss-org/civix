"""NYC Automated Traffic Volume Counts source constants."""

from __future__ import annotations

from typing import Final

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, SourceId
from civix.infra.sources.socrata import DEFAULT_PAGE_SIZE as SOCRATA_DEFAULT_PAGE_SIZE
from civix.infra.sources.socrata import SOCRATA_DEFAULT_ORDER, SocrataDatasetConfig

DEFAULT_BASE_URL: Final[str] = "https://data.cityofnewyork.us/resource/"
DEFAULT_PAGE_SIZE: Final[int] = SOCRATA_DEFAULT_PAGE_SIZE
SOURCE_ID: Final[SourceId] = SourceId("nyc-open-data")
NYC_TRAFFIC_VOLUME_COUNTS_DATASET_ID: Final[DatasetId] = DatasetId("7ym2-wayt")
NYC_JURISDICTION: Final[Jurisdiction] = Jurisdiction(
    country="US",
    region="NY",
    locality="New York City",
)
SOCRATA_ORDER: Final[str] = SOCRATA_DEFAULT_ORDER

NYC_TRAFFIC_VOLUME_COUNTS_SOURCE_SCOPE: Final[str] = (
    "NYC DOT automated traffic volume counts published through NYC Open Data."
)
NYC_TRAFFIC_VOLUME_COUNTS_RELEASE_CAVEATS: Final[tuple[str, ...]] = (
    "Rows are short-duration source-published traffic counts and must not be treated as "
    "annualized volume.",
    "Segment geometry and street labels are source metadata preserved for site mapping and drift.",
)

_SOURCE_RECORD_ID_FIELD: Final[str] = "RequestID"

NYC_TRAFFIC_VOLUME_COUNTS_DATASET_CONFIG: Final[SocrataDatasetConfig] = SocrataDatasetConfig(
    source_id=SOURCE_ID,
    dataset_id=NYC_TRAFFIC_VOLUME_COUNTS_DATASET_ID,
    jurisdiction=NYC_JURISDICTION,
    base_url=DEFAULT_BASE_URL,
    source_record_id_field=_SOURCE_RECORD_ID_FIELD,
)
