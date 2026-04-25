"""Field-level mapping quality primitives.

A mapped field is the smallest unit of normalized output. It carries the
value, the quality state explaining how the value was produced, and the
names of the source fields the mapper consulted.

Quality state replaces fake numeric confidence with explicit, auditable
categories. The distinctions matter: a redacted value, a withheld value,
and a value the engine cannot map are not equivalent and must not
collapse into a single "missing" notion.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, model_validator


class FieldQuality(StrEnum):
    """Why a normalized value has the form it has.

    DIRECT         copied verbatim from one source field
    STANDARDIZED   same meaning, normalized representation
    DERIVED        computed deterministically from one or more source fields
    INFERRED       chosen by a heuristic or non-deterministic rule
    UNMAPPED       engine does not know how to map this field for this source
    CONFLICTED     multiple source fields disagreed on the value
    REDACTED       source explicitly withheld the value
    NOT_PROVIDED   source field exists but value is blank or null
    """

    DIRECT = "direct"
    STANDARDIZED = "standardized"
    DERIVED = "derived"
    INFERRED = "inferred"
    UNMAPPED = "unmapped"
    CONFLICTED = "conflicted"
    REDACTED = "redacted"
    NOT_PROVIDED = "not_provided"


_VALUE_REQUIRED = frozenset(
    {
        FieldQuality.DIRECT,
        FieldQuality.STANDARDIZED,
        FieldQuality.DERIVED,
        FieldQuality.INFERRED,
    }
)
_VALUE_FORBIDDEN = frozenset(
    {
        FieldQuality.UNMAPPED,
        FieldQuality.REDACTED,
        FieldQuality.NOT_PROVIDED,
    }
)


class MappedField[T](BaseModel):
    """A normalized field value with provenance-of-shape metadata.

    `T` is the concrete value type at the use site. A domain record
    declaring `business_name: MappedField[str]` will have its inner
    `value` validated as `str | None` by pydantic.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    value: T | None
    quality: FieldQuality
    source_fields: tuple[str, ...]

    @model_validator(mode="after")
    def _validate(self) -> MappedField[T]:
        self._check_value_presence()
        self._check_source_fields_count()
        self._check_source_fields_strings()
        return self

    def _check_value_presence(self) -> None:
        q = self.quality
        if q in _VALUE_REQUIRED and self.value is None:
            raise ValueError(f"quality={q.value!r} requires a value")
        if q in _VALUE_FORBIDDEN and self.value is not None:
            raise ValueError(f"quality={q.value!r} forbids a value")

    def _check_source_fields_count(self) -> None:
        q = self.quality
        n = len(self.source_fields)
        if q is FieldQuality.UNMAPPED:
            if n != 0:
                raise ValueError("quality='unmapped' requires source_fields to be empty")
            return
        if n == 0:
            raise ValueError(f"quality={q.value!r} requires at least one source field")
        if q is FieldQuality.CONFLICTED and n < 2:
            raise ValueError("quality='conflicted' requires at least two source fields")

    def _check_source_fields_strings(self) -> None:
        for name in self.source_fields:
            if not name:
                raise ValueError("source_fields entries must be non-empty")
            if name != name.strip():
                raise ValueError(f"source_fields entry {name!r} has surrounding whitespace")
