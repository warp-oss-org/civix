"""NYC DCWP premises business-licences source adapter.

Fetches NYC Open Data's Socrata SODA "Issued Licenses" dataset,
restricted to DCWP premises licenses with `license_type = 'Premises'`.
The filter is applied to both the count probe and record pages so raw
snapshots are reproducibly scoped to business-location licences.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final

import httpx

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, SourceId
from civix.core.ports.models.adapter import FetchResult
from civix.core.temporal import Clock, utc_now
from civix.infra.sources.socrata import (
    DEFAULT_PAGE_SIZE as SOCRATA_DEFAULT_PAGE_SIZE,
)
from civix.infra.sources.socrata import (
    SocrataDatasetConfig,
    SocrataFetchConfig,
    SocrataSourceAdapter,
)

DEFAULT_BASE_URL: Final[str] = "https://data.cityofnewyork.us/resource/"
SOURCE_ID: Final[SourceId] = SourceId("nyc-open-data")
DEFAULT_PAGE_SIZE: Final[int] = SOCRATA_DEFAULT_PAGE_SIZE
PREMISES_FILTER: Final[str] = "license_type = 'Premises'"

_SOURCE_RECORD_ID_FIELD: Final[str] = "license_nbr"


@dataclass(frozen=True, slots=True)
class NycBusinessLicencesAdapter:
    """Fetches NYC DCWP premises business licences via the shared Socrata adapter."""

    dataset_id: DatasetId
    jurisdiction: Jurisdiction
    client: httpx.AsyncClient
    clock: Clock = field(default=utc_now)
    base_url: str = DEFAULT_BASE_URL
    page_size: int = DEFAULT_PAGE_SIZE
    app_token: str | None = None

    def __post_init__(self) -> None:
        if self.page_size <= 0:
            raise ValueError("page_size must be greater than zero")

    @property
    def source_id(self) -> SourceId:
        return SOURCE_ID

    async def fetch(self) -> FetchResult:
        return await SocrataSourceAdapter(
            dataset=SocrataDatasetConfig(
                source_id=SOURCE_ID,
                dataset_id=self.dataset_id,
                jurisdiction=self.jurisdiction,
                base_url=self.base_url,
                source_record_id_fields=(_SOURCE_RECORD_ID_FIELD,),
            ),
            fetch_config=SocrataFetchConfig(
                client=self.client,
                clock=self.clock,
                page_size=self.page_size,
                app_token=self.app_token,
                where=PREMISES_FILTER,
            ),
        ).fetch()
