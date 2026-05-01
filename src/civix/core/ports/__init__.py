"""Source adapter contract.

HTTP transport helpers live in `civix.infra.http`; this package exposes
only the contract types every adapter implementation satisfies.
"""

from civix.core.ports.errors import FetchError

__all__ = ["FetchError"]
