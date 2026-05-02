"""Toronto permanent bicycle counter source adapter configuration."""

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
TORONTO_BICYCLE_COUNTERS_DATASET_ID: Final[DatasetId] = DatasetId("permanent-bicycle-counters")
TORONTO_BICYCLE_COUNTER_LOCATIONS_RESOURCE_NAME: Final[str] = "cycling_permanent_counts_locations"
TORONTO_BICYCLE_COUNTER_15MIN_RESOURCE_NAME: Final[str] = "cycling_permanent_counts_15min_2025_2026"
TORONTO_BICYCLE_COUNTERS_JURISDICTION: Final[Jurisdiction] = Jurisdiction(
    country="CA",
    region="ON",
    locality="Toronto",
)

TORONTO_BICYCLE_COUNTERS_SOURCE_SCOPE: Final[str] = (
    "City of Toronto permanent bicycle counter locations and current 15-minute count bins. "
    "The 15-minute resource name is year-bounded and should be reviewed when Toronto publishes "
    "a successor current-period table."
)
TORONTO_BICYCLE_COUNTERS_RELEASE_CAVEATS: Final[tuple[str, ...]] = (
    "Counts include bicycles and other micromobility devices such as e-bikes and scooters.",
    "Only riders passing within the detector zone in the designated bicycle lane are counted.",
    "Construction, parked vehicles, snowbanks, and other blockages can produce zero-volume bins.",
    "Detector calibration may be updated retroactively.",
    "CKAN metadata checked on 2026-05-02 reported License not specified.",
)

TORONTO_BICYCLE_COUNTER_LOCATIONS_DATASET_CONFIG: Final[CkanDatasetConfig] = CkanDatasetConfig(
    source_id=SOURCE_ID,
    dataset_id=TORONTO_BICYCLE_COUNTERS_DATASET_ID,
    jurisdiction=TORONTO_BICYCLE_COUNTERS_JURISDICTION,
    source_record_id_fields=("location_dir_id",),
    resource_name=TORONTO_BICYCLE_COUNTER_LOCATIONS_RESOURCE_NAME,
)
TORONTO_BICYCLE_COUNTER_15MIN_DATASET_CONFIG: Final[CkanDatasetConfig] = CkanDatasetConfig(
    source_id=SOURCE_ID,
    dataset_id=TORONTO_BICYCLE_COUNTERS_DATASET_ID,
    jurisdiction=TORONTO_BICYCLE_COUNTERS_JURISDICTION,
    source_record_id_fields=("location_dir_id", "datetime_bin"),
    resource_name=TORONTO_BICYCLE_COUNTER_15MIN_RESOURCE_NAME,
)


@dataclass(frozen=True, slots=True)
class TorontoBicycleCounterLocationsAdapter:
    """Fetches Toronto permanent bicycle counter location rows via CKAN."""

    fetch_config: CkanFetchConfig

    @property
    def source_id(self) -> SourceId:
        return TORONTO_BICYCLE_COUNTER_LOCATIONS_DATASET_CONFIG.source_id

    @property
    def dataset_id(self) -> DatasetId:
        return TORONTO_BICYCLE_COUNTER_LOCATIONS_DATASET_CONFIG.dataset_id

    @property
    def jurisdiction(self) -> Jurisdiction:
        return TORONTO_BICYCLE_COUNTER_LOCATIONS_DATASET_CONFIG.jurisdiction

    async def fetch(self) -> FetchResult:
        adapter = CkanSourceAdapter(
            dataset=TORONTO_BICYCLE_COUNTER_LOCATIONS_DATASET_CONFIG,
            fetch_config=self.fetch_config,
        )

        return await adapter.fetch()


@dataclass(frozen=True, slots=True)
class TorontoBicycleCounter15MinAdapter:
    """Fetches Toronto permanent bicycle counter 15-minute rows via CKAN."""

    fetch_config: CkanFetchConfig

    @property
    def source_id(self) -> SourceId:
        return TORONTO_BICYCLE_COUNTER_15MIN_DATASET_CONFIG.source_id

    @property
    def dataset_id(self) -> DatasetId:
        return TORONTO_BICYCLE_COUNTER_15MIN_DATASET_CONFIG.dataset_id

    @property
    def jurisdiction(self) -> Jurisdiction:
        return TORONTO_BICYCLE_COUNTER_15MIN_DATASET_CONFIG.jurisdiction

    async def fetch(self) -> FetchResult:
        adapter = CkanSourceAdapter(
            dataset=TORONTO_BICYCLE_COUNTER_15MIN_DATASET_CONFIG,
            fetch_config=self.fetch_config,
        )

        return await adapter.fetch()
