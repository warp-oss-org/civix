"""Reusable OpenFEMA source acquisition helpers."""

from civix.infra.sources.openfema.client import (
    DEFAULT_BASE_URL,
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
    OpenFemaDatasetConfig,
    OpenFemaFetchConfig,
    OpenFemaSourceAdapter,
    fetch_openfema_dataset,
)

__all__ = [
    "DEFAULT_BASE_URL",
    "DEFAULT_PAGE_SIZE",
    "MAX_PAGE_SIZE",
    "OpenFemaDatasetConfig",
    "OpenFemaFetchConfig",
    "OpenFemaSourceAdapter",
    "fetch_openfema_dataset",
]
