"""Toronto KSI source adapter configuration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, SourceId
from civix.core.ports.models.adapter import FetchResult
from civix.infra.sources.ckan import (
    DEFAULT_BASE_URL as CKAN_DEFAULT_BASE_URL,
)
from civix.infra.sources.ckan import (
    DEFAULT_PAGE_SIZE as CKAN_DEFAULT_PAGE_SIZE,
)
from civix.infra.sources.ckan import (
    CkanDatasetConfig,
    CkanFetchConfig,
    CkanSourceAdapter,
)

DEFAULT_BASE_URL: Final[str] = CKAN_DEFAULT_BASE_URL
DEFAULT_PAGE_SIZE: Final[int] = CKAN_DEFAULT_PAGE_SIZE
SOURCE_ID: Final[SourceId] = SourceId("toronto-open-data")
TORONTO_KSI_DATASET_ID: Final[DatasetId] = DatasetId(
    "motor-vehicle-collisions-involving-killed-or-seriously-injured-persons"
)
TORONTO_KSI_JURISDICTION: Final[Jurisdiction] = Jurisdiction(
    country="CA",
    region="ON",
    locality="Toronto",
)
TORONTO_KSI_SOURCE_RECORD_ID_FIELDS: Final[tuple[str, ...]] = ("collision_id", "per_no")

TORONTO_KSI_DATASET_CONFIG: Final[CkanDatasetConfig] = CkanDatasetConfig(
    source_id=SOURCE_ID,
    dataset_id=TORONTO_KSI_DATASET_ID,
    jurisdiction=TORONTO_KSI_JURISDICTION,
    source_record_id_fields=TORONTO_KSI_SOURCE_RECORD_ID_FIELDS,
)


@dataclass(frozen=True, slots=True)
class TorontoKsiAdapter:
    """Fetches Toronto KSI rows via the shared CKAN adapter."""

    fetch_config: CkanFetchConfig

    @property
    def source_id(self) -> SourceId:
        return TORONTO_KSI_DATASET_CONFIG.source_id

    @property
    def dataset_id(self) -> DatasetId:
        return TORONTO_KSI_DATASET_CONFIG.dataset_id

    @property
    def jurisdiction(self) -> Jurisdiction:
        return TORONTO_KSI_DATASET_CONFIG.jurisdiction

    async def fetch(self) -> FetchResult:
        adapter = CkanSourceAdapter(
            dataset=TORONTO_KSI_DATASET_CONFIG,
            fetch_config=self.fetch_config,
        )

        return await adapter.fetch()
