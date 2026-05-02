"""Shared spatial geometry value types."""

from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, model_validator

from civix.core.spatial.models.location import Coordinate

_FROZEN_MODEL = ConfigDict(frozen=True, extra="forbid", strict=True)


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


class SpatialFootprint(BaseModel):
    """A normalized point, line, or bounding-box footprint."""

    model_config = _FROZEN_MODEL

    point: Coordinate | None = None
    line: LineString | None = None
    bounding_box: BoundingBox | None = None

    @model_validator(mode="after")
    def _validate(self) -> "SpatialFootprint":
        populated = sum(
            value is not None for value in (self.point, self.line, self.bounding_box)
        )
        if populated != 1:
            raise ValueError("spatial footprint requires exactly one shape")

        return self
