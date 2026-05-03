import pytest
from pydantic import ValidationError

from civix.core.spatial.models.geometry import (
    BoundingBox,
    GeometryRef,
    GeometryType,
    LineString,
    SpatialFootprint,
)
from civix.core.spatial.models.location import Address, Coordinate


class TestCoordinate:
    def test_typical_vancouver_point(self) -> None:
        c = Coordinate(latitude=49.2827, longitude=-123.1207)

        assert c.latitude == 49.2827
        assert c.longitude == -123.1207

    def test_extremes_accepted(self) -> None:
        Coordinate(latitude=-90.0, longitude=-180.0)
        Coordinate(latitude=90.0, longitude=180.0)
        Coordinate(latitude=0.0, longitude=0.0)

    def test_latitude_above_range_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Coordinate(latitude=90.1, longitude=0.0)

    def test_latitude_below_range_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Coordinate(latitude=-90.1, longitude=0.0)

    def test_longitude_above_range_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Coordinate(latitude=0.0, longitude=180.1)

    def test_longitude_below_range_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Coordinate(latitude=0.0, longitude=-180.1)

    def test_frozen(self) -> None:
        c = Coordinate(latitude=49.0, longitude=-123.0)

        with pytest.raises(ValidationError):
            c.latitude = 50.0  # type: ignore[misc]

    def test_extra_fields_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Coordinate.model_validate({"latitude": 49.0, "longitude": -123.0, "altitude": 100.0})


class TestAddress:
    def test_full_address(self) -> None:
        a = Address(
            country="CA",
            region="BC",
            locality="Vancouver",
            street="123 W Pender St",
            postal_code="V6B 1A1",
        )

        assert a.country == "CA"
        assert a.street == "123 W Pender St"

    def test_country_only(self) -> None:
        a = Address(country="CA")

        assert a.region is None
        assert a.locality is None
        assert a.street is None
        assert a.postal_code is None

    def test_country_required(self) -> None:
        with pytest.raises(ValidationError):
            Address()  # type: ignore[call-arg]

    def test_empty_country_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Address(country="")

    def test_surrounding_whitespace_rejected(self) -> None:
        with pytest.raises(ValidationError, match="whitespace"):
            Address(country="CA", street=" 123 Main ")

    def test_frozen(self) -> None:
        a = Address(country="CA")

        with pytest.raises(ValidationError):
            a.country = "US"  # type: ignore[misc]

    def test_extra_fields_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Address.model_validate({"country": "CA", "unit": "5"})


class TestBoundingBox:
    def test_valid_box(self) -> None:
        box = BoundingBox(west=-87.9, south=41.6, east=-87.5, north=42.1)

        assert box.west == -87.9
        assert box.north == 42.1

    def test_invalid_coordinate_ranges_rejected(self) -> None:
        with pytest.raises(ValidationError):
            BoundingBox(west=-181.0, south=41.6, east=-87.5, north=42.1)

    def test_east_before_west_rejected(self) -> None:
        with pytest.raises(ValidationError, match="east"):
            BoundingBox(west=-87.5, south=41.6, east=-87.9, north=42.1)

    def test_north_before_south_rejected(self) -> None:
        with pytest.raises(ValidationError, match="north"):
            BoundingBox(west=-87.9, south=42.1, east=-87.5, north=41.6)


class TestLineString:
    def test_two_point_line(self) -> None:
        line = LineString(
            coordinates=(
                Coordinate(latitude=40.0, longitude=-73.0),
                Coordinate(latitude=40.1, longitude=-73.1),
            )
        )

        assert len(line.coordinates) == 2

    def test_short_line_rejected(self) -> None:
        with pytest.raises(ValidationError):
            LineString(coordinates=(Coordinate(latitude=40.0, longitude=-73.0),))


class TestGeometryRef:
    def test_valid_service_layer_reference(self) -> None:
        ref = GeometryRef(
            geometry_type=GeometryType.POLYGON,
            uri="https://example.test/arcgis/rest/services/hazards/MapServer",
            layer_name="Flood Hazard Zones",
            geometry_id="FLD-1",
            source_crs="EPSG:4326",
        )

        assert ref.geometry_type is GeometryType.POLYGON
        assert ref.geometry_id == "FLD-1"

    def test_valid_query_reference(self) -> None:
        ref = GeometryRef(
            geometry_type=GeometryType.RASTER,
            uri="https://example.test/hazard-grid",
            query_keys=(("tile", "10-20"), ("band", "risk")),
        )

        assert ref.query_keys == (("tile", "10-20"), ("band", "risk"))

    def test_geometry_id_or_query_key_required(self) -> None:
        with pytest.raises(ValidationError, match="geometry_id or query_keys"):
            GeometryRef(
                geometry_type=GeometryType.POLYGON,
                uri="https://example.test/layer",
            )

    def test_empty_query_key_rejected(self) -> None:
        with pytest.raises(ValidationError):
            GeometryRef(
                geometry_type=GeometryType.POLYGON,
                uri="https://example.test/layer",
                query_keys=(("", "A"),),
            )

    def test_surrounding_whitespace_rejected(self) -> None:
        with pytest.raises(ValidationError, match="whitespace"):
            GeometryRef(
                geometry_type=GeometryType.POLYGON,
                uri=" https://example.test/layer ",
                geometry_id="FLD-1",
            )

    def test_frozen(self) -> None:
        ref = GeometryRef(
            geometry_type=GeometryType.POLYGON,
            uri="https://example.test/layer",
            geometry_id="FLD-1",
        )

        with pytest.raises(ValidationError):
            ref.geometry_id = "FLD-2"  # type: ignore[misc]

    def test_extra_fields_rejected(self) -> None:
        with pytest.raises(ValidationError):
            GeometryRef.model_validate(
                {
                    "geometry_type": "polygon",
                    "uri": "https://example.test/layer",
                    "geometry_id": "FLD-1",
                    "retrieved_at": "2026-05-01T00:00:00Z",
                }
            )


class TestSpatialFootprint:
    def test_point_footprint(self) -> None:
        point = Coordinate(latitude=49.2827, longitude=-123.1207)
        footprint = SpatialFootprint(point=point)

        assert footprint.point == point
        assert footprint.line is None

    def test_line_footprint(self) -> None:
        line = LineString(
            coordinates=(
                Coordinate(latitude=40.0, longitude=-73.0),
                Coordinate(latitude=40.1, longitude=-73.1),
            )
        )
        footprint = SpatialFootprint(line=line)

        assert footprint.line == line

    def test_bounding_box_footprint(self) -> None:
        box = BoundingBox(west=-87.9, south=41.6, east=-87.5, north=42.1)
        footprint = SpatialFootprint(bounding_box=box)

        assert footprint.bounding_box == box

    def test_empty_footprint_rejected(self) -> None:
        with pytest.raises(ValidationError, match="exactly one"):
            SpatialFootprint()

    def test_multiple_shapes_rejected(self) -> None:
        with pytest.raises(ValidationError, match="exactly one"):
            SpatialFootprint(
                point=Coordinate(latitude=40.0, longitude=-73.0),
                bounding_box=BoundingBox(west=-87.9, south=41.6, east=-87.5, north=42.1),
            )
