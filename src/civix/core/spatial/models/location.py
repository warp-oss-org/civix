"""Spatial value types shared across domains.

Coordinate and Address are domain-agnostic — business licences,
permits, and procurement all carry locations of things. Their
shape is independent of any single source's column layout, so they
live in core rather than under a specific domain.
"""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator

_FROZEN_MODEL = ConfigDict(frozen=True, extra="forbid", strict=True)


class Coordinate(BaseModel):
    """A WGS84 geographic point.

    Latitude and longitude are validated against their valid ranges.
    Sentinel values like (0, 0) are not rejected here — that is a
    mapper concern, since "0,0 means missing" is a per-source quirk.
    """

    model_config = _FROZEN_MODEL

    latitude: Annotated[float, Field(ge=-90.0, le=90.0)]
    longitude: Annotated[float, Field(ge=-180.0, le=180.0)]


class Address(BaseModel):
    """A postal address for a thing, distinct from Jurisdiction.

    Jurisdiction describes the civic scope of a dataset; Address
    describes where one record's subject is located. They share
    country/region but differ in granularity and intent.

    Only `country` is required. Civic data is frequently partial
    (no street, no postal code) and the wrapping `MappedField` is
    where "no address at all" is expressed.
    """

    model_config = _FROZEN_MODEL

    country: Annotated[str, Field(min_length=1)]
    region: Annotated[str | None, Field(min_length=1)] = None
    locality: Annotated[str | None, Field(min_length=1)] = None
    street: Annotated[str | None, Field(min_length=1)] = None
    postal_code: Annotated[str | None, Field(min_length=1)] = None

    @field_validator("country", "region", "locality", "street", "postal_code")
    @classmethod
    def _no_surrounding_whitespace(cls, value: str | None) -> str | None:
        if value is None:
            return None

        if value != value.strip():
            raise ValueError("address parts must not have surrounding whitespace")

        return value
