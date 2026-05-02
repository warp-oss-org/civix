"""Group-level field reconciliation helpers for Toronto KSI rows."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from civix.core.mapping.errors import MappingError
from civix.core.mapping.models.mapper import FieldConflict
from civix.core.mapping.parsers import float_or_none, int_or_none, str_or_none
from civix.core.provenance.models.provenance import MapperVersion
from civix.core.snapshots.models.snapshot import RawRecord


@dataclass(frozen=True, slots=True)
class GroupChoice[T]:
    """Chosen group value plus any soft source disagreements."""

    value: T | None
    conflicts: tuple[FieldConflict, ...] = ()


def choose_text(
    records: Sequence[RawRecord],
    field_name: str,
    *,
    output_field: str,
    mapper: MapperVersion,
    hard: bool = False,
) -> GroupChoice[str]:
    values = tuple(str_or_none(record.raw_data.get(field_name)) for record in records)
    nonblank = tuple(value for value in values if value is not None)

    if not nonblank:
        return GroupChoice[str](value=None)

    normalized = [_canonical_text(value) for value in nonblank]
    counts = Counter(normalized)

    if hard and len(counts) > 1:
        raise MappingError(
            f"conflicting Toronto KSI values for {field_name!r}",
            mapper=mapper,
            source_record_id=None,
            source_fields=(field_name,),
        )

    chosen_key = sorted(counts, key=lambda key: (-counts[key], key))[0]
    chosen = next(value.strip() for value in nonblank if _canonical_text(value) == chosen_key)
    candidates = _candidate_values(nonblank)

    if len(candidates) < 2:
        return GroupChoice[str](value=chosen)

    return GroupChoice[str](
        value=chosen,
        conflicts=(
            FieldConflict(
                field_name=output_field,
                candidates=candidates,
                source_fields=tuple(field_name for _ in candidates),
            ),
        ),
    )


def choose_int(
    records: Sequence[RawRecord],
    field_name: str,
    *,
    output_field: str,
    mapper: MapperVersion,
) -> GroupChoice[int]:
    text_choice = choose_text(records, field_name, output_field=output_field, mapper=mapper)

    if text_choice.value is None:
        return GroupChoice[int](value=None, conflicts=text_choice.conflicts)

    parsed = int_or_none(text_choice.value)

    return GroupChoice[int](value=parsed, conflicts=text_choice.conflicts)


def choose_float(
    records: Sequence[RawRecord],
    field_name: str,
    *,
    output_field: str,
    mapper: MapperVersion,
    hard: bool,
) -> GroupChoice[float]:
    values = tuple(float_or_none(record.raw_data.get(field_name)) for record in records)
    present = tuple(value for value in values if value is not None)

    if not present:
        return GroupChoice[float](value=None)

    distinct = tuple(sorted(set(present)))

    if hard and len(distinct) > 1:
        raise MappingError(
            f"conflicting Toronto KSI values for {field_name!r}",
            mapper=mapper,
            source_record_id=None,
            source_fields=(field_name,),
        )

    chosen = min(present)
    candidates = _candidate_values(present)

    if len(candidates) < 2:
        return GroupChoice[float](value=chosen)

    return GroupChoice[float](
        value=chosen,
        conflicts=(
            FieldConflict(
                field_name=output_field,
                candidates=candidates,
                source_fields=tuple(field_name for _ in candidates),
            ),
        ),
    )


def collect_conflicts(choices: Iterable[GroupChoice[object]]) -> tuple[FieldConflict, ...]:
    return tuple(conflict for choice in choices for conflict in choice.conflicts)


def require_choice_value[T](choice: GroupChoice[T]) -> T:
    if choice.value is None:
        raise TypeError("expected choice value")

    return choice.value


def _canonical_text(value: str) -> str:
    return " ".join(value.strip().casefold().split())


def _candidate_values(values: Iterable[Any]) -> tuple[Any, ...]:
    candidates: list[Any] = []

    for value in values:
        candidate = value.strip() if isinstance(value, str) else value

        if candidate not in candidates:
            candidates.append(candidate)

    return tuple(candidates)
