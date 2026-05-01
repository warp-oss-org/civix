"""Chicago Traffic Crashes - Vehicles source adapter."""

from __future__ import annotations

from typing import Final

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, SourceId
from civix.infra.sources.socrata import DEFAULT_PAGE_SIZE as SOCRATA_DEFAULT_PAGE_SIZE
from civix.infra.sources.socrata import (
    SOCRATA_DEFAULT_ORDER,
    SocrataDatasetConfig,
)

DEFAULT_BASE_URL: Final[str] = "https://data.cityofchicago.org/resource/"
DEFAULT_PAGE_SIZE: Final[int] = SOCRATA_DEFAULT_PAGE_SIZE
SOURCE_ID: Final[SourceId] = SourceId("chicago-data-portal")
CHICAGO_VEHICLES_DATASET_ID: Final[DatasetId] = DatasetId("68nd-jvt3")
CHICAGO_JURISDICTION: Final[Jurisdiction] = Jurisdiction(
    country="US", region="IL", locality="Chicago"
)
SOCRATA_ORDER: Final[str] = SOCRATA_DEFAULT_ORDER

_SOURCE_RECORD_ID_FIELD: Final[str] = "crash_unit_id"

CHICAGO_VEHICLES_DATASET_CONFIG: Final[SocrataDatasetConfig] = SocrataDatasetConfig(
    source_id=SOURCE_ID,
    dataset_id=CHICAGO_VEHICLES_DATASET_ID,
    jurisdiction=CHICAGO_JURISDICTION,
    base_url=DEFAULT_BASE_URL,
    source_record_id_field=_SOURCE_RECORD_ID_FIELD,
)
