"""Natural Resources Canada Flood Susceptibility Index source adapter."""

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

SOURCE_ID: Final[SourceId] = SourceId("nrcan-open-maps")
NRCAN_FSI_DATASET_ID: Final[DatasetId] = DatasetId("flood_susceptibility_index")
CA_JURISDICTION: Final[Jurisdiction] = Jurisdiction(country="CA")
NRCAN_FSI_DATASET_PAGE_URL: Final[str] = (
    "https://open.canada.ca/data/en/dataset/df106e11-4cee-425d-bd38-7e51ac674128"
)
NRCAN_FSI_SOURCE_SCOPE: Final[str] = (
    "Natural Resources Canada Flood Susceptibility Index fixture-shaped extract. "
    "Callers must supply a JSON extract URL via NrcanFsiFetchConfig.source_url; "
    "NRCAN_FSI_DATASET_PAGE_URL points at the dataset landing page (HTML) and is "
    "exported only for traceability until a stable machine endpoint is confirmed. "
    "The source is a national screening layer, not a site-level flood-risk assessment."
)

_SOURCE_SPEC: Final[JsonArraySourceSpec] = JsonArraySourceSpec(
    source_id=SOURCE_ID,
    dataset_id=NRCAN_FSI_DATASET_ID,
    jurisdiction=CA_JURISDICTION,
    source_label="NRCan Flood Susceptibility Index",
    id_fields=("cell_id",),
)


@dataclass(frozen=True, slots=True)
class NrcanFsiFetchConfig:
    """Runtime fetch options for an NRCan FSI JSON extract.

    `source_url` is required: NRCan publishes the FSI as a dataset
    landing page rather than a stable machine endpoint, so callers must
    point at a JSON extract they have staged. `NRCAN_FSI_DATASET_PAGE_URL`
    is exported for traceability, not as a default.
    """

    client: httpx.AsyncClient
    source_url: str
    clock: Clock = field(default=utc_now)


@dataclass(frozen=True, slots=True)
class NrcanFsiAdapter:
    """Fetches NRCan FSI fixture-shaped JSON rows."""

    fetch_config: NrcanFsiFetchConfig

    @property
    def source_id(self) -> SourceId:
        return SOURCE_ID

    @property
    def dataset_id(self) -> DatasetId:
        return NRCAN_FSI_DATASET_ID

    @property
    def jurisdiction(self) -> Jurisdiction:
        return CA_JURISDICTION

    async def fetch(self) -> FetchResult:
        return await fetch_json_array_source(
            client=self.fetch_config.client,
            fetched_at=self.fetch_config.clock(),
            source_url=self.fetch_config.source_url,
            spec=_SOURCE_SPEC,
            fetch_params={"source_family": "nrcan-fsi"},
        )
