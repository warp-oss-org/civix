"""HTTP client factory for source adapters.

Adapters take an `httpx.AsyncClient` via dependency injection. This
module provides the canonical pre-configured client civix-the-library
recommends; CLI/notebook callers and tests can use it directly, or
construct their own.

The factory deliberately exposes httpx's own type. We don't wrap it in
a custom Protocol — httpx is a stable, well-shaped dependency and a
wrapper would only add indirection.
"""

from __future__ import annotations

import httpx

from civix import __version__

DEFAULT_TIMEOUT_SECONDS: float = 30.0
DEFAULT_RETRIES: int = 3


def default_user_agent() -> str:
    """User-Agent string identifying civix to civic data portals."""
    return f"civix/{__version__}"


def default_http_client(
    *,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    user_agent: str | None = None,
    retries: int = DEFAULT_RETRIES,
) -> httpx.AsyncClient:
    """Construct an `httpx.AsyncClient` pre-configured for civic data fetches.

    Use as an async context manager so connections are properly closed:

        async with default_http_client() as client:
            adapter = MyAdapter(client=client, ...)
            result = await adapter.fetch()

    Retries apply to transient network failures (connection errors,
    timeouts) at the transport layer; HTTP-level error responses are
    not retried.
    """
    return httpx.AsyncClient(
        timeout=timeout,
        headers={"User-Agent": user_agent or default_user_agent()},
        transport=httpx.AsyncHTTPTransport(retries=retries),
    )
