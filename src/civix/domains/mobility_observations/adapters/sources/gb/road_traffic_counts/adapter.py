"""Great Britain DfT road-traffic-counts source adapter configuration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, SourceId
from civix.core.ports.models.adapter import FetchResult

from ._client import (
    DEFAULT_BASE_URL as DFT_DEFAULT_BASE_URL,
)
from ._client import (
    DEFAULT_PAGE_SIZE as DFT_DEFAULT_PAGE_SIZE,
)
from ._client import (
    DftDatasetConfig,
    DftFetchConfig,
    DftSourceAdapter,
)

DEFAULT_BASE_URL: Final[str] = DFT_DEFAULT_BASE_URL
DEFAULT_PAGE_SIZE: Final[int] = DFT_DEFAULT_PAGE_SIZE
SOURCE_ID: Final[SourceId] = SourceId("gb-dft-road-traffic")
GB_DFT_COUNT_POINTS_DATASET_ID: Final[DatasetId] = DatasetId("dft-count-points")
GB_DFT_AADF_BY_DIRECTION_DATASET_ID: Final[DatasetId] = DatasetId(
    "dft-average-annual-daily-flow-by-direction"
)
GB_DFT_COUNT_POINTS_ENDPOINT: Final[str] = "count-points"
GB_DFT_AADF_BY_DIRECTION_ENDPOINT: Final[str] = "average-annual-daily-flow-by-direction"
GB_DFT_JURISDICTION: Final[Jurisdiction] = Jurisdiction(country="GB")

GB_DFT_SOURCE_SCOPE: Final[str] = (
    "Department for Transport (DfT) road-traffic-counts API. Sprint 5 covers two endpoints: "
    "count-points for site identity, and average-annual-daily-flow-by-direction for AADF "
    "count observations fanned out per vehicle class. The raw-counts endpoint and minor-road "
    "coverage are intentionally deferred to follow-up work."
)
GB_DFT_RELEASE_CAVEATS: Final[tuple[str, ...]] = (
    "Published under the UK Open Government Licence v3.0; preserve attribution downstream.",
    "AADF rows are annual-average daily flows, not raw daily counts; do not aggregate as if "
    "they were observed days.",
    "DfT vehicle-class columns are DfT-specific (e.g. LGVs are vans, not light trucks); "
    "mapping to TravelMode loses some category structure that lives in source caveats.",
    "The all_motor_vehicles column is a published sum of class columns; emitting it alongside "
    "class-specific observations would double-count under naive SUM aggregation. The mapper "
    "intentionally skips it.",
)


@dataclass(frozen=True, slots=True)
class GbDftCountPointsAdapter:
    """Fetches DfT count-point site rows."""

    fetch_config: DftFetchConfig
    base_url: str = DEFAULT_BASE_URL

    @property
    def _dataset(self) -> DftDatasetConfig:
        return DftDatasetConfig(
            source_id=SOURCE_ID,
            dataset_id=GB_DFT_COUNT_POINTS_DATASET_ID,
            jurisdiction=GB_DFT_JURISDICTION,
            endpoint=GB_DFT_COUNT_POINTS_ENDPOINT,
            base_url=self.base_url,
        )

    @property
    def source_id(self) -> SourceId:
        return SOURCE_ID

    @property
    def dataset_id(self) -> DatasetId:
        return GB_DFT_COUNT_POINTS_DATASET_ID

    @property
    def jurisdiction(self) -> Jurisdiction:
        return GB_DFT_JURISDICTION

    async def fetch(self) -> FetchResult:
        adapter = DftSourceAdapter(dataset=self._dataset, fetch_config=self.fetch_config)

        return await adapter.fetch()


@dataclass(frozen=True, slots=True)
class GbDftAadfByDirectionAdapter:
    """Fetches DfT average-annual-daily-flow-by-direction count rows."""

    fetch_config: DftFetchConfig
    base_url: str = DEFAULT_BASE_URL

    @property
    def _dataset(self) -> DftDatasetConfig:
        return DftDatasetConfig(
            source_id=SOURCE_ID,
            dataset_id=GB_DFT_AADF_BY_DIRECTION_DATASET_ID,
            jurisdiction=GB_DFT_JURISDICTION,
            endpoint=GB_DFT_AADF_BY_DIRECTION_ENDPOINT,
            base_url=self.base_url,
        )

    @property
    def source_id(self) -> SourceId:
        return SOURCE_ID

    @property
    def dataset_id(self) -> DatasetId:
        return GB_DFT_AADF_BY_DIRECTION_DATASET_ID

    @property
    def jurisdiction(self) -> Jurisdiction:
        return GB_DFT_JURISDICTION

    async def fetch(self) -> FetchResult:
        adapter = DftSourceAdapter(dataset=self._dataset, fetch_config=self.fetch_config)

        return await adapter.fetch()
