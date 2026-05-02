"""NYC DOT Traffic Speeds source constants."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, SourceId
from civix.core.ports.models.adapter import FetchResult
from civix.infra.sources.socrata import DEFAULT_PAGE_SIZE as SOCRATA_DEFAULT_PAGE_SIZE
from civix.infra.sources.socrata import (
    SOCRATA_DEFAULT_ORDER,
    SocrataDatasetConfig,
    SocrataFetchConfig,
    SocrataSourceAdapter,
)

DEFAULT_BASE_URL: Final[str] = "https://data.cityofnewyork.us/resource/"
DEFAULT_PAGE_SIZE: Final[int] = SOCRATA_DEFAULT_PAGE_SIZE
SOURCE_ID: Final[SourceId] = SourceId("nyc-open-data")
NYC_TRAFFIC_SPEEDS_DATASET_ID: Final[DatasetId] = DatasetId("i4gi-tjb9")
NYC_JURISDICTION: Final[Jurisdiction] = Jurisdiction(
    country="US",
    region="NY",
    locality="New York City",
)
SOCRATA_ORDER: Final[str] = SOCRATA_DEFAULT_ORDER

NYC_TRAFFIC_SPEEDS_SOURCE_SCOPE: Final[str] = (
    "NYC DOT traffic speed and travel-time observations by traffic link, published through "
    "NYC Open Data."
)
NYC_TRAFFIC_SPEEDS_RELEASE_CAVEATS: Final[tuple[str, ...]] = (
    "SPEED and TRAVEL_TIME are separate source metrics carried by one normalized observation.",
    "Congestion metrics are not modeled by this NYC slice.",
)

_SOURCE_RECORD_ID_FIELD: Final[str] = "ID"

NYC_TRAFFIC_SPEEDS_DATASET_CONFIG: Final[SocrataDatasetConfig] = SocrataDatasetConfig(
    source_id=SOURCE_ID,
    dataset_id=NYC_TRAFFIC_SPEEDS_DATASET_ID,
    jurisdiction=NYC_JURISDICTION,
    base_url=DEFAULT_BASE_URL,
    source_record_id_field=_SOURCE_RECORD_ID_FIELD,
)


@dataclass(frozen=True, slots=True)
class NycTrafficSpeedsAdapter:
    """Fetches NYC traffic speed rows via the shared Socrata adapter."""

    fetch_config: SocrataFetchConfig

    @property
    def source_id(self) -> SourceId:
        return NYC_TRAFFIC_SPEEDS_DATASET_CONFIG.source_id

    @property
    def dataset_id(self) -> DatasetId:
        return NYC_TRAFFIC_SPEEDS_DATASET_CONFIG.dataset_id

    @property
    def jurisdiction(self) -> Jurisdiction:
        return NYC_TRAFFIC_SPEEDS_DATASET_CONFIG.jurisdiction

    async def fetch(self) -> FetchResult:
        adapter = SocrataSourceAdapter(
            dataset=NYC_TRAFFIC_SPEEDS_DATASET_CONFIG,
            fetch_config=self.fetch_config,
        )

        return await adapter.fetch()
