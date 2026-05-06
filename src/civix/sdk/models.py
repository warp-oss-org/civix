"""Typed SDK dataset products and runtime dependencies."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import httpx

from civix.core.mapping.models.mapper import Mapper
from civix.core.ports.models.adapter import SourceAdapter
from civix.core.temporal import Clock


@dataclass(frozen=True, slots=True)
class CivixRuntime:
    """Runtime dependencies shared by SDK-created adapters."""

    http_client: httpx.AsyncClient
    clock: Clock


@dataclass(frozen=True, slots=True)
class DatasetProduct[TNorm]:
    """A selectable SDK dataset backed by one adapter and one mapper."""

    country: str
    domain: str
    model: str
    slug: str
    adapter_factory: Callable[[CivixRuntime], SourceAdapter]
    mapper_factory: Callable[[], Mapper[TNorm]]

    @property
    def path(self) -> str:
        return f"{self.country}.{self.domain}.{self.model}.{self.slug}"

    def create_adapter(self, runtime: CivixRuntime) -> SourceAdapter:
        return self.adapter_factory(runtime)

    def create_mapper(self) -> Mapper[TNorm]:
        return self.mapper_factory()
