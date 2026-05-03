"""Shared spatial geometry value types."""

from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from civix.core.spatial.models.location import Coordinate

_FROZEN_MODEL = ConfigDict(frozen=True, extra="forbid", strict=True)
NonEmptyString = Annotated[str, Field(min_length=1)]
GeometryQueryKey = tuple[NonEmptyString, NonEmptyString]


class GeometryType(StrEnum):
    """Portable source geometry kinds."""

    POINT = "point"
    LINE = "line"
    BOUNDING_BOX = "bounding_box"
    POLYGON = "polygon"
    MULTIPOLYGON = "multipolygon"
    RASTER = "raster"
    SOURCE_SPECIFIC = "source_specific"
    UNKNOWN = "unknown"


class BoundingBox(BaseModel):
    """A WGS84 bounding box."""

    model_config = _FROZEN_MODEL

    west: Annotated[float, Field(ge=-180.0, le=180.0)]
    south: Annotated[float, Field(ge=-90.0, le=90.0)]
    east: Annotated[float, Field(ge=-180.0, le=180.0)]
    north: Annotated[float, Field(ge=-90.0, le=90.0)]

    @model_validator(mode="after")
    def _validate(self) -> "BoundingBox":
        if self.east < self.west:
            raise ValueError("east must be greater than or equal to west")

        if self.north < self.south:
            raise ValueError("north must be greater than or equal to south")

        return self


class LineString(BaseModel):
    """An ordered WGS84 line geometry."""

    model_config = _FROZEN_MODEL

    coordinates: Annotated[tuple[Coordinate, ...], Field(min_length=2)]


class GeometryRef(BaseModel):
    """A deterministic reference to source-published geometry.

    Adapters that emit a geometry reference must include enough source
    URL, layer or table, source geometry identifier, query keys, and CRS
    metadata to recover the raw geometry from the original source. This
    value type is not a fetcher contract; acquisition and persistence
    policy stay in source adapters and consumers.
    """

    model_config = _FROZEN_MODEL

    geometry_type: GeometryType
    uri: NonEmptyString
    layer_name: NonEmptyString | None = None
    geometry_id: NonEmptyString | None = None
    source_crs: NonEmptyString | None = None
    query_keys: tuple[GeometryQueryKey, ...] = ()

    @field_validator("uri", "layer_name", "geometry_id", "source_crs")
    @classmethod
    def _no_surrounding_whitespace(cls, value: str | None) -> str | None:
        if value is None:
            return None

        if value != value.strip():
            raise ValueError("geometry reference strings must not have surrounding whitespace")

        return value

    @field_validator("query_keys")
    @classmethod
    def _valid_query_keys(cls, value: tuple[GeometryQueryKey, ...]) -> tuple[GeometryQueryKey, ...]:
        for key, query_value in value:
            if key != key.strip() or query_value != query_value.strip():
                raise ValueError("geometry query keys must not have surrounding whitespace")

        return value

    @model_validator(mode="after")
    def _validate(self) -> "GeometryRef":
        if self.geometry_id is None and not self.query_keys:
            raise ValueError("geometry reference requires geometry_id or query_keys")

        return self


class SpatialFootprint(BaseModel):
    """A normalized point, line, or bounding-box footprint."""

    model_config = _FROZEN_MODEL

    point: Coordinate | None = None
    line: LineString | None = None
    bounding_box: BoundingBox | None = None

    @model_validator(mode="after")
    def _validate(self) -> "SpatialFootprint":
        populated = sum(value is not None for value in (self.point, self.line, self.bounding_box))
        if populated != 1:
            raise ValueError("spatial footprint requires exactly one shape")

        return self
