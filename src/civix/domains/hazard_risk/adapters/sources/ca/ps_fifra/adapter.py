"""Public Safety Canada Federally Identified Flood Risk Areas source adapter."""

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

SOURCE_ID: Final[SourceId] = SourceId("public-safety-canada")
PS_FIFRA_DATASET_ID: Final[DatasetId] = DatasetId("federally_identified_flood_risk_areas")
CA_JURISDICTION: Final[Jurisdiction] = Jurisdiction(country="CA")
PS_FIFRA_DATASET_PAGE_URL: Final[str] = (
    "https://www.publicsafety.gc.ca/cnt/mrgnc-mngmnt/dsstr-prvntn-mtgtn/"
    "ntrl-hzrd-rsk-ssssmnt/index-en.aspx"
)
PS_FIFRA_SOURCE_SCOPE: Final[str] = (
    "Public Safety Canada Federally Identified Flood Risk Areas fixture-shaped extract. "
    "The default source URL is the official dataset page; callers should provide a "
    "fixture-shaped JSON extract URL until a stable machine endpoint is confirmed. "
    "These records are Canada-wide flood screening areas, not local regulatory maps."
)

_SOURCE_SPEC: Final[JsonArraySourceSpec] = JsonArraySourceSpec(
    source_id=SOURCE_ID,
    dataset_id=PS_FIFRA_DATASET_ID,
    jurisdiction=CA_JURISDICTION,
    source_label="Public Safety Canada FIFRA",
    id_fields=("area_id",),
)


@dataclass(frozen=True, slots=True)
class PsFifraFetchConfig:
    """Runtime fetch options for a FIFRA JSON extract."""

    client: httpx.AsyncClient
    clock: Clock = field(default=utc_now)
    source_url: str = PS_FIFRA_DATASET_PAGE_URL


@dataclass(frozen=True, slots=True)
class PsFifraAdapter:
    """Fetches FIFRA fixture-shaped JSON rows."""

    fetch_config: PsFifraFetchConfig

    @property
    def source_id(self) -> SourceId:
        return SOURCE_ID

    @property
    def dataset_id(self) -> DatasetId:
        return PS_FIFRA_DATASET_ID

    @property
    def jurisdiction(self) -> Jurisdiction:
        return CA_JURISDICTION

    async def fetch(self) -> FetchResult:
        return await fetch_json_array_source(
            client=self.fetch_config.client,
            fetched_at=self.fetch_config.clock(),
            source_url=self.fetch_config.source_url,
            spec=_SOURCE_SPEC,
            fetch_params={"source_family": "ps-fifra"},
        )
