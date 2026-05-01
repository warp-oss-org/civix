import pytest
from pydantic import ValidationError

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
