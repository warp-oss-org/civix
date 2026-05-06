"""Minimal async Civix SDK client."""

from __future__ import annotations

from typing import TYPE_CHECKING

import httpx

from civix.core.pipeline import PipelineResult, run
from civix.core.temporal import Clock, utc_now
from civix.infra.http import default_http_client
from civix.sdk.models import CivixRuntime, DatasetProduct

if TYPE_CHECKING:
    from civix.sdk.datasets import DatasetsNamespace


class Civix:
    """Thin async facade over configured dataset products and the core pipeline."""

    def __init__(
        self,
        *,
        http_client: httpx.AsyncClient | None = None,
        clock: Clock = utc_now,
    ) -> None:
        self._owns_http_client = http_client is None
        self._http_client = http_client or default_http_client()
        self._runtime = CivixRuntime(http_client=self._http_client, clock=clock)
        self._closed = False

        # Avoid importing every source adapter during a plain `import civix`.
        from civix.sdk.datasets import DATASETS

        self.datasets: DatasetsNamespace = DATASETS

    async def __aenter__(self) -> Civix:
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._closed:
            return

        self._closed = True

        if self._owns_http_client:
            await self._http_client.aclose()

    async def fetch[TNorm](self, dataset: DatasetProduct[TNorm]) -> PipelineResult[TNorm]:
        if self._closed:
            raise RuntimeError("Civix client is closed")

        adapter = dataset.create_adapter(self._runtime)
        mapper = dataset.create_mapper()

        return await run(adapter, mapper)
