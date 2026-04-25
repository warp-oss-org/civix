"""Mapping primitives.

This module is the typed contract that turns observations into
normalized domain records. It does not contain any mapper
implementations; those belong in `sources/`. What lives here is:

- the per-record diagnostic shape (`MappingReport`, `FieldConflict`),
- the mapper return container (`MapResult`),
- the protocol every mapper satisfies (`Mapper`).

The mapper itself is responsible for building the full normalized
domain record, including its `ProvenanceRef`. That keeps the layer
boundary clean: a pipeline calls a mapper, gets back a self-describing
record plus a report, and decides where artifacts go.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, model_validator

from civix.core.observations import RawRecord, SourceSnapshot
from civix.core.provenance import MapperVersion

_FROZEN_MODEL = ConfigDict(frozen=True, extra="forbid", strict=True)


class FieldConflict(BaseModel):
    """A per-output-field disagreement recorded in a mapping report.

    The complement to `MappedField(quality=CONFLICTED)`: the field on the
    normalized record carries the chosen value, this carries the
    candidates that disagreed and which source fields they came from.
    """

    model_config = _FROZEN_MODEL

    field_name: str
    candidates: tuple[Any, ...]
    source_fields: tuple[str, ...]

    @model_validator(mode="after")
    def _validate(self) -> FieldConflict:
        self._check_field_name()
        self._check_cardinality()
        self._check_source_fields_strings()
        return self

    def _check_field_name(self) -> None:
        if not self.field_name:
            raise ValueError("field_name must be non-empty")
        if self.field_name != self.field_name.strip():
            raise ValueError(f"field_name {self.field_name!r} has surrounding whitespace")

    def _check_cardinality(self) -> None:
        if len(self.candidates) < 2:
            raise ValueError("FieldConflict requires at least two candidates")
        if len(self.source_fields) < 2:
            raise ValueError("FieldConflict requires at least two source fields")

    def _check_source_fields_strings(self) -> None:
        for name in self.source_fields:
            if not name:
                raise ValueError("source_fields entries must be non-empty")
            if name != name.strip():
                raise ValueError(f"source_fields entry {name!r} has surrounding whitespace")


class MappingReport(BaseModel):
    """Per-record diagnostics that do not belong on the normalized record.

    Per-field quality is already on each `MappedField` of the normalized
    record; aggregating it is a consumer concern. What lives here is
    information the record itself cannot carry: source fields the mapper
    saw but did not consume, and conflict candidates whose chosen value
    is on the record but whose alternatives are not.
    """

    model_config = _FROZEN_MODEL

    unmapped_source_fields: tuple[str, ...] = ()
    conflicts: tuple[FieldConflict, ...] = ()

    @model_validator(mode="after")
    def _check_unmapped_source_fields(self) -> MappingReport:
        for name in self.unmapped_source_fields:
            if not name:
                raise ValueError("unmapped_source_fields entries must be non-empty")
            if name != name.strip():
                raise ValueError(
                    f"unmapped_source_fields entry {name!r} has surrounding whitespace"
                )
        return self


class MapResult[TNorm](BaseModel):
    """A mapper's output: the normalized record plus its mapping report."""

    model_config = _FROZEN_MODEL

    record: TNorm
    report: MappingReport


@runtime_checkable
class Mapper[TNorm](Protocol):
    """A pure transformation from one raw record to one normalized record.

    Mappers carry their own `version`; callers do not pin it externally.
    The mapper receives both the raw record and its source snapshot so
    it can assemble the normalized record's `ProvenanceRef` itself.
    """

    @property
    def version(self) -> MapperVersion: ...

    def __call__(self, record: RawRecord, snapshot: SourceSnapshot) -> MapResult[TNorm]: ...
