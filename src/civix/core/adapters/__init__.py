"""Source adapter contract.

HTTP transport helpers live in `civix.infra.http`; this package exposes
only the contract types every adapter implementation satisfies.
"""

from civix.core.adapters.errors import FetchError
from civix.core.adapters.models import FetchResult, SourceAdapter

__all__ = [
    "FetchError",
    "FetchResult",
    "SourceAdapter",
]
