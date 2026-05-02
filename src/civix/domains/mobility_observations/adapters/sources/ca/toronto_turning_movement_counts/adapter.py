"""Toronto turning-movement-count source adapter configuration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, SourceId
from civix.core.ports.models.adapter import FetchResult
from civix.infra.sources.ckan import DEFAULT_BASE_URL as CKAN_DEFAULT_BASE_URL
from civix.infra.sources.ckan import DEFAULT_PAGE_SIZE as CKAN_DEFAULT_PAGE_SIZE
from civix.infra.sources.ckan import CkanDatasetConfig, CkanFetchConfig, CkanSourceAdapter

DEFAULT_BASE_URL: Final[str] = CKAN_DEFAULT_BASE_URL
DEFAULT_PAGE_SIZE: Final[int] = CKAN_DEFAULT_PAGE_SIZE
SOURCE_ID: Final[SourceId] = SourceId("toronto-open-data")
TORONTO_TMC_DATASET_ID: Final[DatasetId] = DatasetId(
    "traffic-volumes-at-intersections-for-all-modes"
)
TORONTO_TMC_SUMMARY_RESOURCE_NAME: Final[str] = "tmc_summary_data"
TORONTO_TMC_RAW_RESOURCE_NAME: Final[str] = "tmc_raw_data_2020_2029"
TORONTO_TMC_JURISDICTION: Final[Jurisdiction] = Jurisdiction(
    country="CA",
    region="ON",
    locality="Toronto",
)

TORONTO_TMC_SOURCE_SCOPE: Final[str] = (
    "City of Toronto multimodal turning movement counts. Sprint 3 uses the current "
    "datastore-backed summary table and 2020-2029 raw 15-minute table. The raw table is "
    "year-bounded and should be reviewed when Toronto publishes a successor decade resource; "
    "the older traffic signal vehicle/pedestrian volume spreadsheet target is intentionally "
    "deferred."
)
TORONTO_TMC_RELEASE_CAVEATS: Final[tuple[str, ...]] = (
    "Counts are collected ad hoc and do not provide comprehensive temporal or geographic coverage.",
    "Older counts used manual collection and current counts use video technology; the source does "
    "not expose row-level collection method in the selected datastore resources.",
    "Toronto notes possible overcounting, undercounting, equipment errors, and post-September-2023 "
    "bicycle count semantics that may include crosswalk-area bicycle movement.",
    "CKAN metadata checked on 2026-05-02 reported License not specified.",
)

TORONTO_TMC_SUMMARY_DATASET_CONFIG: Final[CkanDatasetConfig] = CkanDatasetConfig(
    source_id=SOURCE_ID,
    dataset_id=TORONTO_TMC_DATASET_ID,
    jurisdiction=TORONTO_TMC_JURISDICTION,
    source_record_id_fields=("count_id",),
    resource_name=TORONTO_TMC_SUMMARY_RESOURCE_NAME,
)
TORONTO_TMC_RAW_DATASET_CONFIG: Final[CkanDatasetConfig] = CkanDatasetConfig(
    source_id=SOURCE_ID,
    dataset_id=TORONTO_TMC_DATASET_ID,
    jurisdiction=TORONTO_TMC_JURISDICTION,
    source_record_id_fields=("count_id", "start_time"),
    resource_name=TORONTO_TMC_RAW_RESOURCE_NAME,
)


@dataclass(frozen=True, slots=True)
class TorontoTmcSummaryAdapter:
    """Fetches Toronto TMC summary rows via the shared CKAN adapter."""

    fetch_config: CkanFetchConfig

    @property
    def source_id(self) -> SourceId:
        return TORONTO_TMC_SUMMARY_DATASET_CONFIG.source_id

    @property
    def dataset_id(self) -> DatasetId:
        return TORONTO_TMC_SUMMARY_DATASET_CONFIG.dataset_id

    @property
    def jurisdiction(self) -> Jurisdiction:
        return TORONTO_TMC_SUMMARY_DATASET_CONFIG.jurisdiction

    async def fetch(self) -> FetchResult:
        adapter = CkanSourceAdapter(
            dataset=TORONTO_TMC_SUMMARY_DATASET_CONFIG,
            fetch_config=self.fetch_config,
        )

        return await adapter.fetch()


@dataclass(frozen=True, slots=True)
class TorontoTmcRawCountsAdapter:
    """Fetches Toronto TMC 15-minute raw count rows via the shared CKAN adapter."""

    fetch_config: CkanFetchConfig

    @property
    def source_id(self) -> SourceId:
        return TORONTO_TMC_RAW_DATASET_CONFIG.source_id

    @property
    def dataset_id(self) -> DatasetId:
        return TORONTO_TMC_RAW_DATASET_CONFIG.dataset_id

    @property
    def jurisdiction(self) -> Jurisdiction:
        return TORONTO_TMC_RAW_DATASET_CONFIG.jurisdiction

    async def fetch(self) -> FetchResult:
        adapter = CkanSourceAdapter(
            dataset=TORONTO_TMC_RAW_DATASET_CONFIG,
            fetch_config=self.fetch_config,
        )

        return await adapter.fetch()
