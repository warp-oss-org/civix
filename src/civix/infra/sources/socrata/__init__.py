"""Reusable Socrata SODA source acquisition helpers."""

from civix.infra.sources.socrata.client import (
    DEFAULT_PAGE_SIZE,
    SOCRATA_COMPUTED_REGION_PREFIX,
    SOCRATA_COUNT_FIELD,
    SOCRATA_DEFAULT_ORDER,
    SocrataDatasetConfig,
    SocrataFetchConfig,
    SocrataSourceAdapter,
    fetch_socrata_dataset,
)

__all__ = [
    "DEFAULT_PAGE_SIZE",
    "SOCRATA_COMPUTED_REGION_PREFIX",
    "SOCRATA_COUNT_FIELD",
    "SOCRATA_DEFAULT_ORDER",
    "SocrataDatasetConfig",
    "SocrataFetchConfig",
    "SocrataSourceAdapter",
    "fetch_socrata_dataset",
]
