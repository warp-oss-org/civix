"""Environment Agency Risk of Flooding from Rivers and Sea source adapter."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final

import httpx

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, SourceId
from civix.core.ports.models.adapter import FetchResult
from civix.core.temporal import Clock, utc_now
from civix.domains.hazard_risk.adapters.sources._json import (
    JsonArraySourceSpec,
    fetch_json_array_source,
)

SOURCE_ID: Final[SourceId] = SourceId("environment-agency")
EA_ROFRS_DATASET_ID: Final[DatasetId] = DatasetId("risk_of_flooding_from_rivers_and_sea")
GB_ENGLAND_JURISDICTION: Final[Jurisdiction] = Jurisdiction(country="GB", region="England")
EA_ROFRS_DATASET_PAGE_URL: Final[str] = (
    "https://www.data.gov.uk/dataset/943d2bbb-aa08-45d1-96cb-42556cd01d94/"
    "risk-of-flooding-from-rivers-and-sea2"
)
EA_ROFRS_SOURCE_SCOPE: Final[str] = (
    "Environment Agency Risk of Flooding from Rivers and Sea fixture-shaped extract for England. "
    "The default source URL is the official dataset page; callers should provide a "
    "fixture-shaped JSON extract URL until a stable machine endpoint is confirmed."
)

_SOURCE_SPEC: Final[JsonArraySourceSpec] = JsonArraySourceSpec(
    source_id=SOURCE_ID,
    dataset_id=EA_ROFRS_DATASET_ID,
    jurisdiction=GB_ENGLAND_JURISDICTION,
    source_label="Environment Agency RoFRS",
    id_fields=("risk_area_id",),
)


@dataclass(frozen=True, slots=True)
class EaRofrsFetchConfig:
    """Runtime fetch options for an Environment Agency RoFRS JSON extract."""

    client: httpx.AsyncClient
    clock: Clock = field(default=utc_now)
    source_url: str = EA_ROFRS_DATASET_PAGE_URL


@dataclass(frozen=True, slots=True)
class EaRofrsAdapter:
    """Fetches RoFRS fixture-shaped JSON rows."""

    fetch_config: EaRofrsFetchConfig

    @property
    def source_id(self) -> SourceId:
        return SOURCE_ID

    @property
    def dataset_id(self) -> DatasetId:
        return EA_ROFRS_DATASET_ID

    @property
    def jurisdiction(self) -> Jurisdiction:
        return GB_ENGLAND_JURISDICTION

    async def fetch(self) -> FetchResult:
        return await fetch_json_array_source(
            client=self.fetch_config.client,
            fetched_at=self.fetch_config.clock(),
            source_url=self.fetch_config.source_url,
            spec=_SOURCE_SPEC,
            fetch_params={"source_family": "ea-rofrs"},
        )
