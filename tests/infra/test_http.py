"""Tests for the default HTTP client factory used by source adapters."""

from __future__ import annotations

import httpx
import respx

from civix import __version__
from civix.infra.http import (
    DEFAULT_RETRIES,
    DEFAULT_TIMEOUT_SECONDS,
    default_http_client,
    default_user_agent,
)


class TestDefaultHttpClient:
    def test_returns_async_client(self) -> None:
        client = default_http_client()

        assert isinstance(client, httpx.AsyncClient)

    def test_timeout_default(self) -> None:
        client = default_http_client()

        assert client.timeout == httpx.Timeout(DEFAULT_TIMEOUT_SECONDS)

    def test_timeout_override(self) -> None:
        client = default_http_client(timeout=5.0)

        assert client.timeout == httpx.Timeout(5.0)

    def test_user_agent_default(self) -> None:
        client = default_http_client()

        assert client.headers["User-Agent"] == f"civix/{__version__}"

    def test_user_agent_override(self) -> None:
        client = default_http_client(user_agent="civix-test/0.0.0")

        assert client.headers["User-Agent"] == "civix-test/0.0.0"

    def test_default_user_agent_helper(self) -> None:
        assert default_user_agent() == f"civix/{__version__}"

    def test_default_retries_constant(self) -> None:
        assert DEFAULT_RETRIES == 3

    async def test_client_makes_requests_through_respx_stub(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            route = respx_mock.get("https://opendata.vancouver.ca/ping").mock(
                return_value=httpx.Response(200, json={"ok": True})
            )

            async with default_http_client() as client:
                response = await client.get("https://opendata.vancouver.ca/ping")

            assert route.called
            assert response.status_code == 200
            assert response.json() == {"ok": True}

    async def test_user_agent_sent_on_wire(self) -> None:
        async with respx.mock(assert_all_called=True) as respx_mock:
            route = respx_mock.get("https://opendata.vancouver.ca/ping").mock(
                return_value=httpx.Response(200, json={})
            )

            async with default_http_client() as client:
                await client.get("https://opendata.vancouver.ca/ping")

            sent_request = route.calls.last.request

            assert sent_request.headers["User-Agent"] == f"civix/{__version__}"
