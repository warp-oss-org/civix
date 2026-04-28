"""Stateful drift observers driven incrementally by a streaming pipeline.

Each observer mirrors one of the pure `observe_*` functions in
`observation.py`, but accumulates per-record state so a consumer can
feed records as they arrive (e.g. through `attach_observers`) and call
`finalize` once at the end.

Observers see the raw record from `PipelineRecord.raw`; the mapped
record is ignored at this layer. Drift questions are about what the
source actually emitted, independent of mapper behaviour.

Partial iteration produces a partial report: an observer that only saw
the first N records of an M-record snapshot will report on those N. The
report's `checked_record_count` reflects what was actually observed.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from pydantic import BaseModel

from civix.core.drift.analysis import compare_schema, compare_taxonomy
from civix.core.drift.observation import (
    SchemaObservationAccumulator,
    TaxonomyObservationAccumulator,
)
from civix.core.drift.report import SchemaDriftReport, TaxonomyDriftReport
from civix.core.drift.spec import SourceSchemaSpec, TaxonomySpec
from civix.core.snapshots import RawRecord, SourceSnapshot


@runtime_checkable
class DriftObserver(Protocol):
    """Per-record observer that emits a drift report on finalize."""

    def observe(self, record: RawRecord) -> None: ...

    def finalize(self, snapshot: SourceSnapshot) -> BaseModel: ...


class SchemaObserver:
    """Incremental version of `observe_schema` paired with `compare_schema`."""

    def __init__(self, *, spec: SourceSchemaSpec) -> None:
        self._spec = spec
        self._accumulator = SchemaObservationAccumulator()

    def observe(self, record: RawRecord) -> None:
        self._accumulator.observe(record)

    def finalize(self, snapshot: SourceSnapshot) -> SchemaDriftReport:
        observed = self._accumulator.build()

        return compare_schema(snapshot=snapshot, observed=observed, spec=self._spec)


class TaxonomyObserver:
    """Incremental version of `observe_taxonomy` paired with `compare_taxonomy`."""

    def __init__(self, *, specs: tuple[TaxonomySpec, ...]) -> None:
        self._specs = specs
        self._accumulator = TaxonomyObservationAccumulator(specs=specs)

    def observe(self, record: RawRecord) -> None:
        self._accumulator.observe(record)

    def finalize(self, snapshot: SourceSnapshot) -> TaxonomyDriftReport:
        observed = self._accumulator.build()

        return compare_taxonomy(snapshot=snapshot, observed=observed, specs=self._specs)
