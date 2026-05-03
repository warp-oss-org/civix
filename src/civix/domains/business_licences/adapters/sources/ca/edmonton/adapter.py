"""Edmonton business-licences source adapter.

Fetches Edmonton's Socrata SODA "City of Edmonton - Business Licences"
dataset via the shared Socrata source adapter. Socrata-computed region
fields are stripped before records enter Civix snapshots.
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

DEFAULT_BASE_URL: Final[str] = "https://data.edmonton.ca/resource/"
SOURCE_ID: Final[SourceId] = SourceId("edmonton-open-data")
DEFAULT_PAGE_SIZE: Final[int] = SOCRATA_DEFAULT_PAGE_SIZE

_SOURCE_RECORD_ID_FIELD: Final[str] = "externalid"


@dataclass(frozen=True, slots=True)
class EdmontonBusinessLicencesAdapter:
    """Fetches Edmonton's business-licences dataset via the shared Socrata adapter."""

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
            ),
        ).fetch()
