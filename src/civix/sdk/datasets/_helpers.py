"""Shared helpers for SDK dataset product declarations."""

from __future__ import annotations

from collections.abc import Callable

from civix.core.mapping.models.mapper import Mapper
from civix.core.ports.models.adapter import SourceAdapter
from civix.infra.sources.ckan import CkanFetchConfig
from civix.infra.sources.socrata import SocrataFetchConfig
from civix.sdk.models import CivixRuntime, DatasetProduct

AdapterFactory = Callable[[CivixRuntime], SourceAdapter]


def product[TNorm](
    *,
    country: str,
    domain: str,
    model: str,
    slug: str,
    adapter_factory: AdapterFactory,
    mapper_factory: Callable[[], Mapper[TNorm]],
) -> DatasetProduct[TNorm]:
    return DatasetProduct[TNorm](
        country=country,
        domain=domain,
        model=model,
        slug=slug,
        adapter_factory=adapter_factory,
        mapper_factory=mapper_factory,
    )


def socrata(runtime: CivixRuntime) -> SocrataFetchConfig:
    return SocrataFetchConfig(client=runtime.http_client, clock=runtime.clock)


def ckan(runtime: CivixRuntime) -> CkanFetchConfig:
    return CkanFetchConfig(client=runtime.http_client, clock=runtime.clock)
