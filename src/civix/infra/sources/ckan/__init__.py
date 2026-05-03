"""Reusable CKAN datastore source acquisition helpers."""

from civix.infra.sources.ckan.client import (
    DEFAULT_BASE_URL,
    DEFAULT_PAGE_SIZE,
    CkanDatasetConfig,
    CkanFetchConfig,
    CkanSourceAdapter,
    CkanStaticJsonResource,
    fetch_ckan_dataset,
    fetch_ckan_static_json_resource,
)

__all__ = [
    "DEFAULT_BASE_URL",
    "DEFAULT_PAGE_SIZE",
    "CkanDatasetConfig",
    "CkanFetchConfig",
    "CkanSourceAdapter",
    "CkanStaticJsonResource",
    "fetch_ckan_dataset",
    "fetch_ckan_static_json_resource",
]
