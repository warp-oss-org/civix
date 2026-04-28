"""Drift baselines: what each source dataset is expected to look like.

Specs are pure data, checked into source under each adapter's package.
A maintainer who needs to absorb a real source change opens a PR that
edits the spec and bumps its `version`; civix never mutates them.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

_FROZEN_MODEL = ConfigDict(frozen=True, extra="forbid", strict=True)


class JsonFieldKind(StrEnum):
    """Source-agnostic JSON value kinds, excluding null."""

    STRING = "string"
    NUMBER = "number"
    BOOLEAN = "boolean"
    OBJECT = "object"
    ARRAY = "array"


class SchemaFieldSpec(BaseModel):
    """Expected shape for one source field."""

    model_config = _FROZEN_MODEL

    name: Annotated[str, Field(min_length=1)]
    kinds: tuple[JsonFieldKind, ...]
    nullable: bool = False

    @model_validator(mode="after")
    def _validate(self) -> SchemaFieldSpec:
        if self.name != self.name.strip():
            raise ValueError("field spec name must not have surrounding whitespace")
        if not self.kinds:
            raise ValueError("field spec kinds must not be empty")
        if len(set(self.kinds)) != len(self.kinds):
            raise ValueError("field spec kinds must be unique")

        return self


class SourceSchemaSpec(BaseModel):
    """Explicit schema baseline for one source dataset."""

    model_config = _FROZEN_MODEL

    spec_id: Annotated[str, Field(min_length=1)]
    version: Annotated[str, Field(min_length=1)]
    fields: tuple[SchemaFieldSpec, ...]

    @model_validator(mode="after")
    def _validate(self) -> SourceSchemaSpec:
        if self.spec_id != self.spec_id.strip():
            raise ValueError("spec_id must not have surrounding whitespace")
        if self.version != self.version.strip():
            raise ValueError("version must not have surrounding whitespace")
        field_names = [field.name for field in self.fields]
        if len(set(field_names)) != len(field_names):
            raise ValueError("source schema field names must be unique")

        return self


TaxonomyNormalization = Literal["exact", "strip_casefold"]


class TaxonomySpec(BaseModel):
    """Known vocabulary for one source field.

    `source_field` names a top-level key in `RawRecord.raw_data`. The
    observed string at that key is run through `normalization` before
    being matched against `known_values` and `retired_values`. Spec
    authors should write `known_values` already in the normalized form
    they want comparisons against — the same casefold/strip rule applied
    to observed values is NOT re-applied to the spec's sets.
    """

    model_config = _FROZEN_MODEL

    taxonomy_id: Annotated[str, Field(min_length=1)]
    version: Annotated[str, Field(min_length=1)]
    source_field: Annotated[str, Field(min_length=1)]
    known_values: frozenset[str]
    retired_values: frozenset[str] = frozenset()
    normalization: TaxonomyNormalization = "exact"

    @model_validator(mode="after")
    def _validate(self) -> TaxonomySpec:
        for label, value in (
            ("taxonomy_id", self.taxonomy_id),
            ("version", self.version),
            ("source_field", self.source_field),
        ):
            if value != value.strip():
                raise ValueError(f"{label} must not have surrounding whitespace")

        overlap = self.known_values & self.retired_values
        if overlap:
            raise ValueError(
                f"known_values and retired_values must not overlap; both contain: {sorted(overlap)}"
            )

        return self
