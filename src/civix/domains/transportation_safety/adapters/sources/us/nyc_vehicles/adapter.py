"""NYC Motor Vehicle Collisions - Vehicles source constants."""

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
NYC_VEHICLES_DATASET_ID: Final[DatasetId] = DatasetId("bm4k-52h4")
NYC_JURISDICTION: Final[Jurisdiction] = Jurisdiction(
    country="US", region="NY", locality="New York City"
)
SOCRATA_ORDER: Final[str] = SOCRATA_DEFAULT_ORDER

NYC_VEHICLES_SOURCE_SCOPE: Final[str] = (
    "Vehicle-level records for motor vehicles involved in NYC police-reported "
    "collisions, published through NYC Open Data."
)
NYC_VEHICLES_RELEASE_CAVEATS: Final[tuple[str, ...]] = (
    "The vehicle table goes back to April 2016 when crash reporting switched to an "
    "electronic system.",
    "NYC Open Data records are preliminary and subject to change when MV-104AN forms "
    "are amended based on revised crash details.",
    "Vehicle make, model, year, registration, and driver licence fields are preserved "
    "raw and unmapped in the transportation-safety domain.",
)

_SOURCE_RECORD_ID_FIELD: Final[str] = "unique_id"

NYC_VEHICLES_DATASET_CONFIG: Final[SocrataDatasetConfig] = SocrataDatasetConfig(
    source_id=SOURCE_ID,
    dataset_id=NYC_VEHICLES_DATASET_ID,
    jurisdiction=NYC_JURISDICTION,
    base_url=DEFAULT_BASE_URL,
    source_record_id_field=_SOURCE_RECORD_ID_FIELD,
)


@dataclass(frozen=True, slots=True)
class NycVehiclesAdapter:
    """Fetches NYC crash vehicle rows via the shared Socrata adapter."""

    fetch_config: SocrataFetchConfig

    @property
    def source_id(self) -> SourceId:
        return NYC_VEHICLES_DATASET_CONFIG.source_id

    @property
    def dataset_id(self) -> DatasetId:
        return NYC_VEHICLES_DATASET_CONFIG.dataset_id

    @property
    def jurisdiction(self) -> Jurisdiction:
        return NYC_VEHICLES_DATASET_CONFIG.jurisdiction

    async def fetch(self) -> FetchResult:
        adapter = SocrataSourceAdapter(
            dataset=NYC_VEHICLES_DATASET_CONFIG,
            fetch_config=self.fetch_config,
        )

        return await adapter.fetch()
