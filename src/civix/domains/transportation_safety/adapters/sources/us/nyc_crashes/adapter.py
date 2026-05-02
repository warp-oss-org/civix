"""NYC Motor Vehicle Collisions - Crashes source constants."""

from __future__ import annotations

from typing import Final

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, SourceId
from civix.infra.sources.socrata import DEFAULT_PAGE_SIZE as SOCRATA_DEFAULT_PAGE_SIZE
from civix.infra.sources.socrata import (
    SOCRATA_DEFAULT_ORDER,
    SocrataDatasetConfig,
)

DEFAULT_BASE_URL: Final[str] = "https://data.cityofnewyork.us/resource/"
DEFAULT_PAGE_SIZE: Final[int] = SOCRATA_DEFAULT_PAGE_SIZE
SOURCE_ID: Final[SourceId] = SourceId("nyc-open-data")
NYC_CRASHES_DATASET_ID: Final[DatasetId] = DatasetId("h9gi-nx95")
NYC_JURISDICTION: Final[Jurisdiction] = Jurisdiction(
    country="US", region="NY", locality="New York City"
)
SOCRATA_ORDER: Final[str] = SOCRATA_DEFAULT_ORDER

NYC_COLLISIONS_SOURCE_SCOPE: Final[str] = (
    "Police-reported motor vehicle collisions in New York City from NYPD MV-104AN "
    "reports, published through NYC Open Data."
)
NYC_COLLISIONS_RELEASE_CAVEATS: Final[tuple[str, ...]] = (
    "MV-104AN reports are required for collisions where someone is injured or killed, "
    "or where there is at least $1000 in property damage.",
    "NYC Open Data records are preliminary and subject to change when MV-104AN forms "
    "are amended based on revised crash details.",
    "Crash-row aggregate injury and fatality counts must not overwrite person-level "
    "outcome records from the person table.",
    "Crash-row mode-specific injury/fatality counts and vehicle type code fields are "
    "observed for drift but remain unmapped in S6 because the current domain model has "
    "no mode-specific aggregate slots and the vehicles table is the canonical vehicle "
    "type source.",
)

_SOURCE_RECORD_ID_FIELD: Final[str] = "collision_id"

NYC_CRASHES_DATASET_CONFIG: Final[SocrataDatasetConfig] = SocrataDatasetConfig(
    source_id=SOURCE_ID,
    dataset_id=NYC_CRASHES_DATASET_ID,
    jurisdiction=NYC_JURISDICTION,
    base_url=DEFAULT_BASE_URL,
    source_record_id_field=_SOURCE_RECORD_ID_FIELD,
)
