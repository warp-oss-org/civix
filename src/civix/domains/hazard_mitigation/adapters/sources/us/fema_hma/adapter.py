"""FEMA Hazard Mitigation Assistance source adapters."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, SourceId
from civix.core.ports.models.adapter import FetchResult
from civix.infra.sources.openfema import (
    DEFAULT_BASE_URL,
    OpenFemaDatasetConfig,
    OpenFemaFetchConfig,
    OpenFemaSourceAdapter,
)

SOURCE_ID: Final[SourceId] = SourceId("openfema")
US_JURISDICTION: Final[Jurisdiction] = Jurisdiction(country="US")
FEMA_HMA_PROJECTS_DATASET_ID: Final[DatasetId] = DatasetId("HazardMitigationAssistanceProjects")
FEMA_HMA_TRANSACTIONS_DATASET_ID: Final[DatasetId] = DatasetId(
    "HazardMitigationAssistanceProjectsFinancialTransactions"
)
FEMA_HMA_PROJECTS_VERSION: Final[str] = "v4"
FEMA_HMA_TRANSACTIONS_VERSION: Final[str] = "v1"
FEMA_HMA_PROJECTS_ORDER: Final[str] = "projectIdentifier"
FEMA_HMA_TRANSACTIONS_ORDER: Final[str] = "projectIdentifier,transactionIdentifier"
FEMA_HMA_SOURCE_SCOPE: Final[str] = (
    "FEMA Hazard Mitigation Assistance projects and project financial transactions "
    "published through OpenFEMA."
)

FEMA_HMA_PROJECTS_DATASET_CONFIG: Final[OpenFemaDatasetConfig] = OpenFemaDatasetConfig(
    source_id=SOURCE_ID,
    dataset_id=FEMA_HMA_PROJECTS_DATASET_ID,
    jurisdiction=US_JURISDICTION,
    version=FEMA_HMA_PROJECTS_VERSION,
    entity=str(FEMA_HMA_PROJECTS_DATASET_ID),
    source_record_id_fields=("projectIdentifier",),
    base_url=DEFAULT_BASE_URL,
)
FEMA_HMA_TRANSACTIONS_DATASET_CONFIG: Final[OpenFemaDatasetConfig] = OpenFemaDatasetConfig(
    source_id=SOURCE_ID,
    dataset_id=FEMA_HMA_TRANSACTIONS_DATASET_ID,
    jurisdiction=US_JURISDICTION,
    version=FEMA_HMA_TRANSACTIONS_VERSION,
    entity=str(FEMA_HMA_TRANSACTIONS_DATASET_ID),
    source_record_id_fields=("projectIdentifier", "transactionIdentifier"),
    base_url=DEFAULT_BASE_URL,
)


@dataclass(frozen=True, slots=True)
class FemaHmaProjectsAdapter:
    """Fetches FEMA HMA project rows through the shared OpenFEMA adapter."""

    fetch_config: OpenFemaFetchConfig

    @property
    def source_id(self) -> SourceId:
        return FEMA_HMA_PROJECTS_DATASET_CONFIG.source_id

    @property
    def dataset_id(self) -> DatasetId:
        return FEMA_HMA_PROJECTS_DATASET_CONFIG.dataset_id

    @property
    def jurisdiction(self) -> Jurisdiction:
        return FEMA_HMA_PROJECTS_DATASET_CONFIG.jurisdiction

    async def fetch(self) -> FetchResult:
        adapter = OpenFemaSourceAdapter(
            dataset=FEMA_HMA_PROJECTS_DATASET_CONFIG,
            fetch_config=self.fetch_config,
        )

        return await adapter.fetch()


@dataclass(frozen=True, slots=True)
class FemaHmaTransactionsAdapter:
    """Fetches FEMA HMA financial transaction rows through OpenFEMA."""

    fetch_config: OpenFemaFetchConfig

    @property
    def source_id(self) -> SourceId:
        return FEMA_HMA_TRANSACTIONS_DATASET_CONFIG.source_id

    @property
    def dataset_id(self) -> DatasetId:
        return FEMA_HMA_TRANSACTIONS_DATASET_CONFIG.dataset_id

    @property
    def jurisdiction(self) -> Jurisdiction:
        return FEMA_HMA_TRANSACTIONS_DATASET_CONFIG.jurisdiction

    async def fetch(self) -> FetchResult:
        adapter = OpenFemaSourceAdapter(
            dataset=FEMA_HMA_TRANSACTIONS_DATASET_CONFIG,
            fetch_config=self.fetch_config,
        )

        return await adapter.fetch()
