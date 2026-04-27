"""Source adapter contract and HTTP plumbing."""

from civix.core.adapters.errors import FetchError
from civix.core.adapters.http import (
    DEFAULT_RETRIES,
    DEFAULT_TIMEOUT_SECONDS,
    default_http_client,
    default_user_agent,
)
from civix.core.adapters.models import FetchResult, SourceAdapter

__all__ = [
    "DEFAULT_RETRIES",
    "DEFAULT_TIMEOUT_SECONDS",
    "FetchError",
    "FetchResult",
    "SourceAdapter",
    "default_http_client",
    "default_user_agent",
]
