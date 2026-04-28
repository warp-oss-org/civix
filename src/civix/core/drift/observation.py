"""Pure observation passes over raw records.

The two phases of drift detection — observation and comparison — are
deliberately split. Observation is source-agnostic: it walks raw records
once and records what was actually there. Comparison (in `analysis.py`)
diffs the observation against an explicit spec.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from civix.core.drift.spec import JsonFieldKind, TaxonomyNormalization, TaxonomySpec
from civix.core.snapshots import RawRecord

_FROZEN_MODEL = ConfigDict(frozen=True, extra="forbid", strict=True)
_SAMPLE_LIMIT = 5


class ObservedField(BaseModel):
    """Observed shape for one source field across a record collection."""

    model_config = _FROZEN_MODEL

    name: Annotated[str, Field(min_length=1)]
    present_count: Annotated[int, Field(ge=0)]
    missing_count: Annotated[int, Field(ge=0)] = 0
    null_count: Annotated[int, Field(ge=0)] = 0
    kind_counts: Mapping[JsonFieldKind, int] = Field(
        default_factory=lambda: dict[JsonFieldKind, int]()
    )
    unsupported_type_counts: Mapping[str, int] = Field(default_factory=lambda: dict[str, int]())
    sample_source_record_ids: tuple[str, ...] = ()
    missing_sample_source_record_ids: tuple[str, ...] = ()
    null_sample_source_record_ids: tuple[str, ...] = ()
    kind_sample_source_record_ids: Mapping[JsonFieldKind, tuple[str, ...]] = Field(
        default_factory=lambda: dict[JsonFieldKind, tuple[str, ...]]()
    )
    unsupported_type_sample_source_record_ids: Mapping[str, tuple[str, ...]] = Field(
        default_factory=lambda: dict[str, tuple[str, ...]]()
    )

    @field_validator(
        "sample_source_record_ids",
        "missing_sample_source_record_ids",
        "null_sample_source_record_ids",
    )
    @classmethod
    def _sample_ids_not_empty(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        for item in value:
            if not item:
                raise ValueError("sample source record IDs must be non-empty")

        return value


class ObservedSchema(BaseModel):
    """Observed raw schema for a record collection."""

    model_config = _FROZEN_MODEL

    record_count: Annotated[int, Field(ge=0)]
    fields: Mapping[str, ObservedField] = Field(default_factory=lambda: dict[str, ObservedField]())


@dataclass(slots=True)
class _ObservedFieldBuilder:
    field_name: str
    present_count: int = 0
    missing_count: int = 0
    null_count: int = 0
    kind_counts: dict[JsonFieldKind, int] = field(
        default_factory=lambda: dict[JsonFieldKind, int]()
    )
    unsupported_type_counts: dict[str, int] = field(default_factory=lambda: dict[str, int]())
    sample_source_record_ids: list[str] = field(default_factory=lambda: list[str]())
    missing_sample_source_record_ids: list[str] = field(default_factory=lambda: list[str]())
    null_sample_source_record_ids: list[str] = field(default_factory=lambda: list[str]())
    kind_sample_source_record_ids: dict[JsonFieldKind, list[str]] = field(
        default_factory=lambda: dict[JsonFieldKind, list[str]]()
    )
    unsupported_type_sample_source_record_ids: dict[str, list[str]] = field(
        default_factory=lambda: dict[str, list[str]]()
    )

    def mark_missing(self, source_record_id: str | None) -> None:
        self.missing_count += 1
        _append_sample(self.missing_sample_source_record_ids, source_record_id)

    def mark_present(self, source_record_id: str | None) -> None:
        self.present_count += 1
        _append_sample(self.sample_source_record_ids, source_record_id)

    def mark_null(self, source_record_id: str | None) -> None:
        self.null_count += 1
        _append_sample(self.null_sample_source_record_ids, source_record_id)

    def mark_kind(self, kind: JsonFieldKind, source_record_id: str | None) -> None:
        self.kind_counts[kind] = self.kind_counts.get(kind, 0) + 1
        samples = self.kind_sample_source_record_ids.setdefault(kind, [])
        _append_sample(samples, source_record_id)

    def mark_unsupported(self, type_name: str, source_record_id: str | None) -> None:
        self.unsupported_type_counts[type_name] = self.unsupported_type_counts.get(type_name, 0) + 1
        samples = self.unsupported_type_sample_source_record_ids.setdefault(type_name, [])
        _append_sample(samples, source_record_id)

    def build(self) -> ObservedField:
        return ObservedField(
            name=self.field_name,
            present_count=self.present_count,
            missing_count=self.missing_count,
            null_count=self.null_count,
            kind_counts=dict(sorted(self.kind_counts.items(), key=lambda item: item[0].value)),
            unsupported_type_counts=dict(sorted(self.unsupported_type_counts.items())),
            sample_source_record_ids=tuple(self.sample_source_record_ids),
            missing_sample_source_record_ids=tuple(self.missing_sample_source_record_ids),
            null_sample_source_record_ids=tuple(self.null_sample_source_record_ids),
            kind_sample_source_record_ids={
                kind: tuple(samples)
                for kind, samples in sorted(
                    self.kind_sample_source_record_ids.items(),
                    key=lambda item: item[0].value,
                )
            },
            unsupported_type_sample_source_record_ids={
                type_name: tuple(samples)
                for type_name, samples in sorted(
                    self.unsupported_type_sample_source_record_ids.items()
                )
            },
        )


class SchemaObservationAccumulator:
    def __init__(self) -> None:
        self._field_builders: dict[str, _ObservedFieldBuilder] = {}
        self._record_count = 0
        self._global_sample_source_record_ids: list[str] = []

    def observe(self, record: RawRecord) -> None:
        source_record_id = record.source_record_id
        keys = set(record.raw_data)

        for missing_name in sorted(set(self._field_builders) - keys):
            self._field_builders[missing_name].mark_missing(source_record_id)

        for field_name, value in sorted(record.raw_data.items()):
            field_builder = self._field_builder_for(field_name)
            field_builder.mark_present(source_record_id)
            self._observe_value(field_builder, value, source_record_id)

        _append_sample(self._global_sample_source_record_ids, source_record_id)
        self._record_count += 1

    def build(self) -> ObservedSchema:
        return ObservedSchema(
            record_count=self._record_count,
            fields={
                field_name: self._field_builders[field_name].build()
                for field_name in sorted(self._field_builders)
            },
        )

    def _field_builder_for(self, field_name: str) -> _ObservedFieldBuilder:
        field_builder = self._field_builders.get(field_name)

        if field_builder is not None:
            return field_builder

        field_builder = _ObservedFieldBuilder(
            field_name=field_name,
            missing_count=self._record_count,
            missing_sample_source_record_ids=list(self._global_sample_source_record_ids),
        )
        self._field_builders[field_name] = field_builder

        return field_builder

    def _observe_value(
        self,
        field_builder: _ObservedFieldBuilder,
        value: Any,
        source_record_id: str | None,
    ) -> None:
        if value is None:
            field_builder.mark_null(source_record_id)
            return

        kind = _json_kind(value)

        if kind is None:
            field_builder.mark_unsupported(type(value).__name__, source_record_id)
            return

        field_builder.mark_kind(kind, source_record_id)


def observe_schema(records: Iterable[RawRecord]) -> ObservedSchema:
    """Observe raw JSON field presence, nullability, and value kinds."""
    accumulator = SchemaObservationAccumulator()

    for record in records:
        accumulator.observe(record)

    return accumulator.build()


class ObservedTaxonomyValue(BaseModel):
    """One observed value for one taxonomy field, plus its raw variants.

    `value` is the normalized form that comparison checks against the
    spec. `raw_samples` shows the unnormalized strings that mapped to it
    (e.g. `"Issued"` and `"  ISSUED  "` both normalize to `"issued"`
    under `strip_casefold`); useful for debugging false positives in the
    spec's known set.
    """

    model_config = _FROZEN_MODEL

    value: Annotated[str, Field(min_length=1)]
    count: Annotated[int, Field(ge=1)]
    raw_samples: tuple[str, ...] = ()
    sample_source_record_ids: tuple[str, ...] = ()


class ObservedTaxonomy(BaseModel):
    """Observed taxonomy values per taxonomy spec, in one snapshot."""

    model_config = _FROZEN_MODEL

    record_count: Annotated[int, Field(ge=0)]
    by_taxonomy: Mapping[str, tuple[ObservedTaxonomyValue, ...]] = Field(
        default_factory=lambda: dict[str, tuple[ObservedTaxonomyValue, ...]]()
    )


@dataclass(slots=True)
class _ObservedTaxonomyValueBuilder:
    value: str
    count: int = 0
    raw_samples: list[str] = field(default_factory=lambda: list[str]())
    sample_source_record_ids: list[str] = field(default_factory=lambda: list[str]())

    def observe(self, *, raw: str, source_record_id: str | None) -> None:
        self.count += 1

        if raw not in self.raw_samples and len(self.raw_samples) < _SAMPLE_LIMIT:
            self.raw_samples.append(raw)

        _append_sample(self.sample_source_record_ids, source_record_id)

    def build(self) -> ObservedTaxonomyValue:
        return ObservedTaxonomyValue(
            value=self.value,
            count=self.count,
            raw_samples=tuple(self.raw_samples),
            sample_source_record_ids=tuple(self.sample_source_record_ids),
        )


class TaxonomyObservationAccumulator:
    def __init__(self, *, specs: tuple[TaxonomySpec, ...]) -> None:
        self._specs = specs
        self._value_builders: dict[str, dict[str, _ObservedTaxonomyValueBuilder]] = {
            spec.taxonomy_id: {} for spec in specs
        }
        self._record_count = 0

    def observe(self, record: RawRecord) -> None:
        for spec in self._specs:
            self._observe_spec(record, spec)

        self._record_count += 1

    def build(self) -> ObservedTaxonomy:
        return ObservedTaxonomy(
            record_count=self._record_count,
            by_taxonomy={
                taxonomy_id: tuple(bucket[value].build() for value in sorted(bucket))
                for taxonomy_id, bucket in self._value_builders.items()
            },
        )

    def _observe_spec(self, record: RawRecord, spec: TaxonomySpec) -> None:
        raw_value = record.raw_data.get(spec.source_field)

        if not isinstance(raw_value, str):
            return

        normalized_value = _normalize_taxonomy_value(raw_value, spec.normalization)

        if not normalized_value:
            return

        value_builder = self._value_builder_for(spec.taxonomy_id, normalized_value)
        value_builder.observe(raw=raw_value, source_record_id=record.source_record_id)

    def _value_builder_for(
        self,
        taxonomy_id: str,
        normalized_value: str,
    ) -> _ObservedTaxonomyValueBuilder:
        taxonomy_builders = self._value_builders[taxonomy_id]
        value_builder = taxonomy_builders.get(normalized_value)

        if value_builder is not None:
            return value_builder

        value_builder = _ObservedTaxonomyValueBuilder(value=normalized_value)
        taxonomy_builders[normalized_value] = value_builder

        return value_builder


def observe_taxonomy(
    records: Iterable[RawRecord],
    specs: tuple[TaxonomySpec, ...],
) -> ObservedTaxonomy:
    """Walk raw records once and tally normalized values per taxonomy spec.

    Records whose `source_field` is missing, null, or non-string are
    skipped silently — those are schema concerns, not taxonomy concerns,
    and are surfaced by `observe_schema` instead.
    """
    accumulator = TaxonomyObservationAccumulator(specs=specs)

    for record in records:
        accumulator.observe(record)

    return accumulator.build()


def _normalize_taxonomy_value(value: str, rule: TaxonomyNormalization) -> str:
    if rule == "strip_casefold":
        return value.strip().casefold()

    return value


def _json_kind(value: Any) -> JsonFieldKind | None:
    if isinstance(value, bool):
        return JsonFieldKind.BOOLEAN

    if isinstance(value, str):
        return JsonFieldKind.STRING

    if isinstance(value, int | float):
        return JsonFieldKind.NUMBER

    if isinstance(value, Mapping):
        return JsonFieldKind.OBJECT

    if isinstance(value, list):
        return JsonFieldKind.ARRAY

    return None


def _append_sample(samples: list[str], source_record_id: str | None) -> None:
    if source_record_id is None:
        return

    if len(samples) >= _SAMPLE_LIMIT:
        return

    samples.append(source_record_id)
