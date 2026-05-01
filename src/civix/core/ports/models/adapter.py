"""Source adapter contract.

Adapters fetch and snapshot raw civic records. They do not normalize.
A `SourceAdapter` produces a `FetchResult` containing a `SourceSnapshot`
(eager, populated at fetch start) and an async iterator of `RawRecord`
(lazy, paginated as consumed).

Async-first because civix is positioned as the foundation for an SDK
and HTTP API. Sync callers wrap with `asyncio.run(adapter.fetch())`.
"""

from __future__ import annotations

from collections.abc import AsyncIterable
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, SourceId
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot


@dataclass(frozen=True, slots=True)
class FetchResult:
    """An adapter's output: snapshot metadata plus a record stream.

    `records` is an async iterator rather than a materialized sequence
    so adapters can stream arbitrarily large datasets without holding
    every row in memory.
    """

    snapshot: SourceSnapshot
    records: AsyncIterable[RawRecord]


@runtime_checkable
class SourceAdapter(Protocol):
    """A configured fetcher for one (source, dataset) pair.

    Adapters are constructed with their HTTP client, clock, and any
    source-specific config. Calling `fetch()` returns a `FetchResult`
    with the snapshot populated and records ready to stream.
    """

    @property
    def source_id(self) -> SourceId: ...

    @property
    def dataset_id(self) -> DatasetId: ...

    @property
    def jurisdiction(self) -> Jurisdiction: ...

    async def fetch(self) -> FetchResult: ...
