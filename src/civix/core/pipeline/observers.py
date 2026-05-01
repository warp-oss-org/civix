"""Wire drift observers into the streaming pipeline.

`attach_observers` returns a `PipelineResult` whose record iterator
side-effects each observer with the raw record before yielding the
paired result downstream. A single iteration over the wrapped result
feeds every observer once and produces no extra reads from the source.

Partial iteration produces a partial observation: an exporter that
breaks out of the loop early leaves observers in a half-finalized state.
That is intentional — `finalize` always reflects what was observed, not
what was promised.
"""

from __future__ import annotations

from collections.abc import AsyncIterable, Sequence
from typing import Protocol, runtime_checkable

from civix.core.pipeline.runner import PipelineRecord, PipelineResult
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot


@runtime_checkable
class _HasObserve(Protocol):
    """Minimal observer shape this module needs.

    The full `DriftObserver` Protocol (which also requires `finalize`)
    lives in `core/drift/observers.py`. This package does not depend on
    `core/drift/` — `attach_observers` only invokes `observe`, so it
    declares only that half of the contract.
    """

    def observe(self, record: RawRecord) -> None: ...

    def finalize(self, snapshot: SourceSnapshot) -> object: ...


def attach_observers[TNorm](
    result: PipelineResult[TNorm],
    observers: Sequence[_HasObserve],
) -> PipelineResult[TNorm]:
    """Wrap `result.records` so each observer sees every raw record."""
    inner = result.records

    async def _wrapped() -> AsyncIterable[PipelineRecord[TNorm]]:
        async for paired in inner:
            for observer in observers:
                observer.observe(paired.raw)

            yield paired

    return PipelineResult[TNorm](snapshot=result.snapshot, records=_wrapped())
