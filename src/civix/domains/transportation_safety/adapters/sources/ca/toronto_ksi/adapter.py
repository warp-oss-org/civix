"""Toronto KSI source adapter configuration."""

from __future__ import annotations

from typing import Final

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, SourceId
from civix.infra.sources.ckan import (
    DEFAULT_BASE_URL as CKAN_DEFAULT_BASE_URL,
)
from civix.infra.sources.ckan import (
    DEFAULT_PAGE_SIZE as CKAN_DEFAULT_PAGE_SIZE,
)
from civix.infra.sources.ckan import (
    CkanDatasetConfig,
)

DEFAULT_BASE_URL: Final[str] = CKAN_DEFAULT_BASE_URL
DEFAULT_PAGE_SIZE: Final[int] = CKAN_DEFAULT_PAGE_SIZE
SOURCE_ID: Final[SourceId] = SourceId("toronto-open-data")
TORONTO_KSI_DATASET_ID: Final[DatasetId] = DatasetId(
    "motor-vehicle-collisions-involving-killed-or-seriously-injured-persons"
)
TORONTO_KSI_JURISDICTION: Final[Jurisdiction] = Jurisdiction(
    country="CA",
    region="ON",
    locality="Toronto",
)
TORONTO_KSI_SOURCE_RECORD_ID_FIELDS: Final[tuple[str, ...]] = ("collision_id", "per_no")

TORONTO_KSI_DATASET_CONFIG: Final[CkanDatasetConfig] = CkanDatasetConfig(
    source_id=SOURCE_ID,
    dataset_id=TORONTO_KSI_DATASET_ID,
    jurisdiction=TORONTO_KSI_JURISDICTION,
    source_record_id_fields=TORONTO_KSI_SOURCE_RECORD_ID_FIELDS,
)
