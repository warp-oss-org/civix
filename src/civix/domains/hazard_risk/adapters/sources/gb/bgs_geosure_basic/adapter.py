"""British Geological Survey GeoSure Basic source adapter."""

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

SOURCE_ID: Final[SourceId] = SourceId("british-geological-survey")
BGS_GEOSURE_BASIC_DATASET_ID: Final[DatasetId] = DatasetId("geosure_basic")
GB_JURISDICTION: Final[Jurisdiction] = Jurisdiction(country="GB")
BGS_GEOSURE_BASIC_DATASET_PAGE_URL: Final[str] = (
    "https://www.data.gov.uk/dataset/b6c7976d-a0c3-4bfe-b4a0-d68a47d07102/geosure-basic-version-8"
)
BGS_GEOSURE_BASIC_SOURCE_SCOPE: Final[str] = (
    "British Geological Survey GeoSure Basic fixture-shaped extract. Records are "
    "generalized Great Britain geohazard susceptibility ratings. The default source "
    "URL is the official dataset page; callers should provide a fixture-shaped JSON "
    "extract URL until a stable machine endpoint is confirmed."
)

_SOURCE_SPEC: Final[JsonArraySourceSpec] = JsonArraySourceSpec(
    source_id=SOURCE_ID,
    dataset_id=BGS_GEOSURE_BASIC_DATASET_ID,
    jurisdiction=GB_JURISDICTION,
    source_label="BGS GeoSure Basic",
    id_fields=("hex_id",),
)


@dataclass(frozen=True, slots=True)
class BgsGeosureBasicFetchConfig:
    """Runtime fetch options for a GeoSure Basic JSON extract."""

    client: httpx.AsyncClient
    clock: Clock = field(default=utc_now)
    source_url: str = BGS_GEOSURE_BASIC_DATASET_PAGE_URL


@dataclass(frozen=True, slots=True)
class BgsGeosureBasicAdapter:
    """Fetches GeoSure Basic fixture-shaped JSON rows."""

    fetch_config: BgsGeosureBasicFetchConfig

    @property
    def source_id(self) -> SourceId:
        return SOURCE_ID

    @property
    def dataset_id(self) -> DatasetId:
        return BGS_GEOSURE_BASIC_DATASET_ID

    @property
    def jurisdiction(self) -> Jurisdiction:
        return GB_JURISDICTION

    async def fetch(self) -> FetchResult:
        return await fetch_json_array_source(
            client=self.fetch_config.client,
            fetched_at=self.fetch_config.clock(),
            source_url=self.fetch_config.source_url,
            spec=_SOURCE_SPEC,
            fetch_params={"source_family": "bgs-geosure-basic"},
        )
