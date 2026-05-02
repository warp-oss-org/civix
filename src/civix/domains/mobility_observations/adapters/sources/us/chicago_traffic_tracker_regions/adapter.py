"""Chicago Traffic Tracker — Congestion Estimates by Regions source constants."""

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

DEFAULT_BASE_URL: Final[str] = "https://data.cityofchicago.org/resource/"
DEFAULT_PAGE_SIZE: Final[int] = SOCRATA_DEFAULT_PAGE_SIZE
SOURCE_ID: Final[SourceId] = SourceId("chicago-data-portal")
CHICAGO_TRAFFIC_TRACKER_REGIONS_DATASET_ID: Final[DatasetId] = DatasetId("t2qc-9pjd")
CHICAGO_JURISDICTION: Final[Jurisdiction] = Jurisdiction(
    country="US",
    region="IL",
    locality="Chicago",
)
SOCRATA_ORDER: Final[str] = SOCRATA_DEFAULT_ORDER

CHICAGO_TRAFFIC_TRACKER_REGIONS_SOURCE_SCOPE: Final[str] = (
    "Chicago Traffic Tracker live congestion estimates rolled up by region, derived from "
    "anonymized bus-GPS speed data and published through the Chicago Data Portal."
)
CHICAGO_TRAFFIC_TRACKER_REGIONS_RELEASE_CAVEATS: Final[tuple[str, ...]] = (
    "Estimates are regional rollups of bus-GPS speeds, not segment-level observations.",
    "_last_updt is the dataset refresh timestamp, not an observation interval.",
)

_SOURCE_RECORD_ID_FIELD: Final[str] = "_region_id"

CHICAGO_TRAFFIC_TRACKER_REGIONS_DATASET_CONFIG: Final[SocrataDatasetConfig] = SocrataDatasetConfig(
    source_id=SOURCE_ID,
    dataset_id=CHICAGO_TRAFFIC_TRACKER_REGIONS_DATASET_ID,
    jurisdiction=CHICAGO_JURISDICTION,
    base_url=DEFAULT_BASE_URL,
    source_record_id_field=_SOURCE_RECORD_ID_FIELD,
)


@dataclass(frozen=True, slots=True)
class ChicagoTrafficTrackerRegionsAdapter:
    """Fetches Chicago Traffic Tracker region rows via the shared Socrata adapter."""

    fetch_config: SocrataFetchConfig

    @property
    def source_id(self) -> SourceId:
        return CHICAGO_TRAFFIC_TRACKER_REGIONS_DATASET_CONFIG.source_id

    @property
    def dataset_id(self) -> DatasetId:
        return CHICAGO_TRAFFIC_TRACKER_REGIONS_DATASET_CONFIG.dataset_id

    @property
    def jurisdiction(self) -> Jurisdiction:
        return CHICAGO_TRAFFIC_TRACKER_REGIONS_DATASET_CONFIG.jurisdiction

    async def fetch(self) -> FetchResult:
        adapter = SocrataSourceAdapter(
            dataset=CHICAGO_TRAFFIC_TRACKER_REGIONS_DATASET_CONFIG,
            fetch_config=self.fetch_config,
        )

        return await adapter.fetch()
