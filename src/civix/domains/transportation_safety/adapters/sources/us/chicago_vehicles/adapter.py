"""Chicago Traffic Crashes - Vehicles source adapter."""

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
CHICAGO_VEHICLES_DATASET_ID: Final[DatasetId] = DatasetId("68nd-jvt3")
CHICAGO_JURISDICTION: Final[Jurisdiction] = Jurisdiction(
    country="US", region="IL", locality="Chicago"
)
SOCRATA_ORDER: Final[str] = SOCRATA_DEFAULT_ORDER

_SOURCE_RECORD_ID_FIELD: Final[str] = "crash_unit_id"

CHICAGO_VEHICLES_DATASET_CONFIG: Final[SocrataDatasetConfig] = SocrataDatasetConfig(
    source_id=SOURCE_ID,
    dataset_id=CHICAGO_VEHICLES_DATASET_ID,
    jurisdiction=CHICAGO_JURISDICTION,
    base_url=DEFAULT_BASE_URL,
    source_record_id_fields=(_SOURCE_RECORD_ID_FIELD,),
)


@dataclass(frozen=True, slots=True)
class ChicagoVehiclesAdapter:
    """Fetches Chicago crash vehicle rows via the shared Socrata adapter."""

    fetch_config: SocrataFetchConfig

    @property
    def source_id(self) -> SourceId:
        return CHICAGO_VEHICLES_DATASET_CONFIG.source_id

    @property
    def dataset_id(self) -> DatasetId:
        return CHICAGO_VEHICLES_DATASET_CONFIG.dataset_id

    @property
    def jurisdiction(self) -> Jurisdiction:
        return CHICAGO_VEHICLES_DATASET_CONFIG.jurisdiction

    async def fetch(self) -> FetchResult:
        adapter = SocrataSourceAdapter(
            dataset=CHICAGO_VEHICLES_DATASET_CONFIG,
            fetch_config=self.fetch_config,
        )

        return await adapter.fetch()
