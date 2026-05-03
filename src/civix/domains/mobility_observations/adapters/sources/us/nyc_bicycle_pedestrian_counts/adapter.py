"""NYC Bicycle and Pedestrian Counts source constants.

This package intentionally owns two co-published NYC Open Data datasets:
count observations and companion sensor metadata. That deviates from the
usual one-dataset-per-package convention because `sensor_id` is the
source-published join key and both datasets are needed to preserve site
provenance for the count observations.
"""

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
NYC_BICYCLE_PEDESTRIAN_COUNTS_DATASET_ID: Final[DatasetId] = DatasetId("ct66-47at")
NYC_BICYCLE_PEDESTRIAN_SENSORS_DATASET_ID: Final[DatasetId] = DatasetId("6up2-gnw8")
NYC_JURISDICTION: Final[Jurisdiction] = Jurisdiction(
    country="US",
    region="NY",
    locality="New York City",
)
SOCRATA_ORDER: Final[str] = SOCRATA_DEFAULT_ORDER

NYC_BICYCLE_PEDESTRIAN_SOURCE_SCOPE: Final[str] = (
    "NYC DOT bicycle and pedestrian count observations with companion sensor metadata, "
    "published through NYC Open Data."
)
NYC_BICYCLE_PEDESTRIAN_RELEASE_CAVEATS: Final[tuple[str, ...]] = (
    "Count data can have lapses from transmission, weather, connection, equipment, or "
    "vandalism issues.",
    "Sensor metadata is mapped as site context and count observations retain their own row "
    "provenance.",
)

_SENSOR_SOURCE_RECORD_ID_FIELD: Final[str] = "sensor_id"

NYC_BICYCLE_PEDESTRIAN_COUNTS_DATASET_CONFIG: Final[SocrataDatasetConfig] = SocrataDatasetConfig(
    source_id=SOURCE_ID,
    dataset_id=NYC_BICYCLE_PEDESTRIAN_COUNTS_DATASET_ID,
    jurisdiction=NYC_JURISDICTION,
    base_url=DEFAULT_BASE_URL,
    source_record_id_fields=(),
)
NYC_BICYCLE_PEDESTRIAN_SENSORS_DATASET_CONFIG: Final[SocrataDatasetConfig] = SocrataDatasetConfig(
    source_id=SOURCE_ID,
    dataset_id=NYC_BICYCLE_PEDESTRIAN_SENSORS_DATASET_ID,
    jurisdiction=NYC_JURISDICTION,
    base_url=DEFAULT_BASE_URL,
    source_record_id_fields=(_SENSOR_SOURCE_RECORD_ID_FIELD,),
)


@dataclass(frozen=True, slots=True)
class NycBicyclePedestrianCountsAdapter:
    """Fetches NYC bicycle/pedestrian count rows via the shared Socrata adapter."""

    fetch_config: SocrataFetchConfig

    @property
    def source_id(self) -> SourceId:
        return NYC_BICYCLE_PEDESTRIAN_COUNTS_DATASET_CONFIG.source_id

    @property
    def dataset_id(self) -> DatasetId:
        return NYC_BICYCLE_PEDESTRIAN_COUNTS_DATASET_CONFIG.dataset_id

    @property
    def jurisdiction(self) -> Jurisdiction:
        return NYC_BICYCLE_PEDESTRIAN_COUNTS_DATASET_CONFIG.jurisdiction

    async def fetch(self) -> FetchResult:
        adapter = SocrataSourceAdapter(
            dataset=NYC_BICYCLE_PEDESTRIAN_COUNTS_DATASET_CONFIG,
            fetch_config=self.fetch_config,
        )

        return await adapter.fetch()


@dataclass(frozen=True, slots=True)
class NycBicyclePedestrianSensorsAdapter:
    """Fetches NYC bicycle/pedestrian sensor rows via the shared Socrata adapter."""

    fetch_config: SocrataFetchConfig

    @property
    def source_id(self) -> SourceId:
        return NYC_BICYCLE_PEDESTRIAN_SENSORS_DATASET_CONFIG.source_id

    @property
    def dataset_id(self) -> DatasetId:
        return NYC_BICYCLE_PEDESTRIAN_SENSORS_DATASET_CONFIG.dataset_id

    @property
    def jurisdiction(self) -> Jurisdiction:
        return NYC_BICYCLE_PEDESTRIAN_SENSORS_DATASET_CONFIG.jurisdiction

    async def fetch(self) -> FetchResult:
        adapter = SocrataSourceAdapter(
            dataset=NYC_BICYCLE_PEDESTRIAN_SENSORS_DATASET_CONFIG,
            fetch_config=self.fetch_config,
        )

        return await adapter.fetch()
