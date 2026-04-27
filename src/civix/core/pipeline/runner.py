"""Pipeline orchestration: compose a SourceAdapter and a Mapper.

The pipeline is the single place where fetching meets mapping. Adapters
and mappers each have one job; the pipeline runs an adapter, threads
its snapshot into the mapper, and yields per-record `PipelineRecord`s
(raw + mapped together) lazily so consumers can stream over arbitrarily
large datasets.

Validation, drift detection, export, and aggregation are deliberately
out of scope for this layer. Each is a separate concern that composes
on top of `PipelineResult` from outside.
"""

from __future__ import annotations

from collections.abc import AsyncIterable
from dataclasses import dataclass

from civix.core.adapters import SourceAdapter
from civix.core.mapping import Mapper, MapResult
from civix.core.observations import RawRecord, SourceSnapshot


@dataclass(frozen=True, slots=True)
class PipelineRecord[TNorm]:
    """Pipeline output for one record: raw input + mapped output bundled.

    Carries the raw record alongside the mapped result so downstream
    consumers (export, drift detection, validation) have access to both
    without re-running the adapter. The pipeline layer owns this
    bundling so `MapResult` stays focused on the mapper's contract.
    """

    raw: RawRecord
    mapped: MapResult[TNorm]


@dataclass(frozen=True, slots=True)
class PipelineResult[TNorm]:
    """Output of `run`: snapshot eager, paired records lazy."""

    snapshot: SourceSnapshot
    records: AsyncIterable[PipelineRecord[TNorm]]


async def run[TNorm](
    adapter: SourceAdapter,
    mapper: Mapper[TNorm],
) -> PipelineResult[TNorm]:
    """Fetch from `adapter`, map each record through `mapper`, return both.

    The fetch is performed eagerly so the snapshot is populated before
    `PipelineResult` is returned. Records stay lazy: the mapper is
    invoked only as the caller iterates.
    """
    fetch_result = await adapter.fetch()

    async def _paired() -> AsyncIterable[PipelineRecord[TNorm]]:
        snapshot = fetch_result.snapshot
        async for raw in fetch_result.records:
            yield PipelineRecord(raw=raw, mapped=mapper(raw, snapshot))

    return PipelineResult[TNorm](snapshot=fetch_result.snapshot, records=_paired())
