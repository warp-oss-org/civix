"""NYC LL84 annual benchmarking source adapter."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final

import httpx

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, SourceId
from civix.core.ports.models.adapter import FetchResult
from civix.core.temporal import Clock, utc_now
from civix.domains.building_energy_emissions.adapters.sources.us.nyc_ll84.schema import (
    LL84_OUT_FIELDS,
)
from civix.infra.sources.socrata import (
    SocrataDatasetConfig,
    SocrataFetchConfig,
    SocrataSourceAdapter,
)

SOURCE_ID: Final[SourceId] = SourceId("nyc-open-data")
LL84_DATASET_ID: Final[DatasetId] = DatasetId("5zyy-y8am")
NYC_JURISDICTION: Final[Jurisdiction] = Jurisdiction(
    country="US", region="NY", locality="New York City"
)
_LL84_BASE_URL: Final[str] = "https://data.cityofnewyork.us/resource/"
LL84_BASE_URL: Final[str] = f"{_LL84_BASE_URL}{LL84_DATASET_ID}.json"
LL84_DEFAULT_ORDER: Final[str] = "property_id, report_year"
LL84_SOURCE_SCOPE: Final[str] = (
    "NYC Local Law 84 annual building energy and water benchmarking rows for "
    "calendar years 2022 and later, published through NYC Open Data."
)
DEFAULT_PAGE_SIZE: Final[int] = 1000
LL84_SOURCE_RECORD_ID_FIELDS: Final[tuple[str, str]] = ("property_id", "report_year")

LL84_DATASET_CONFIG: Final[SocrataDatasetConfig] = SocrataDatasetConfig(
    source_id=SOURCE_ID,
    dataset_id=LL84_DATASET_ID,
    jurisdiction=NYC_JURISDICTION,
    base_url=_LL84_BASE_URL,
    source_record_id_fields=LL84_SOURCE_RECORD_ID_FIELDS,
)


@dataclass(frozen=True, slots=True)
class NycLl84FetchConfig:
    """Runtime fetch options for one NYC LL84 query snapshot."""

    client: httpx.AsyncClient
    clock: Clock = field(default=utc_now)
    page_size: int = DEFAULT_PAGE_SIZE
    app_token: str | None = None
    where: str = "1=1"
    select: tuple[str, ...] = LL84_OUT_FIELDS
    order: str = LL84_DEFAULT_ORDER

    def __post_init__(self) -> None:
        if self.page_size <= 0:
            raise ValueError("page_size must be greater than zero")

        if not self.select:
            raise ValueError("select must contain at least one source field")


@dataclass(frozen=True, slots=True)
class NycLl84Adapter:
    """Fetches NYC LL84 benchmarking rows via the shared Socrata adapter."""

    fetch_config: NycLl84FetchConfig

    @property
    def source_id(self) -> SourceId:
        return SOURCE_ID

    @property
    def dataset_id(self) -> DatasetId:
        return LL84_DATASET_ID

    @property
    def jurisdiction(self) -> Jurisdiction:
        return NYC_JURISDICTION

    async def fetch(self) -> FetchResult:
        return await SocrataSourceAdapter(
            dataset=LL84_DATASET_CONFIG,
            fetch_config=SocrataFetchConfig(
                client=self.fetch_config.client,
                clock=self.fetch_config.clock,
                page_size=self.fetch_config.page_size,
                app_token=self.fetch_config.app_token,
                where=self.fetch_config.where,
                order=self.fetch_config.order,
                select=self.fetch_config.select,
            ),
        ).fetch()
