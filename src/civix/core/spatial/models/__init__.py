"""Spatial model package."""

from civix.core.spatial.models.geometry import (
    BoundingBox,
    GeometryRef,
    GeometryType,
    LineString,
    SpatialFootprint,
)
from civix.core.spatial.models.location import Address, Coordinate

__all__ = [
    "Address",
    "BoundingBox",
    "Coordinate",
    "GeometryRef",
    "GeometryType",
    "LineString",
    "SpatialFootprint",
]
