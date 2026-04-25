"""Source adapter contract and HTTP plumbing."""

from civix.core.sources.http import (
    DEFAULT_RETRIES,
    DEFAULT_TIMEOUT_SECONDS,
    default_http_client,
    default_user_agent,
)
from civix.core.sources.models import Clock, FetchResult, SourceAdapter

__all__ = [
    "DEFAULT_RETRIES",
    "DEFAULT_TIMEOUT_SECONDS",
    "Clock",
    "FetchResult",
    "SourceAdapter",
    "default_http_client",
    "default_user_agent",
]
