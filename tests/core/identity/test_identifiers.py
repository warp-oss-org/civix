import pytest
from pydantic import ValidationError

from civix.core.identity.models.identifiers import (
    DatasetId,
    Jurisdiction,
    MapperId,
    SnapshotId,
    SourceId,
)


class TestIdentifiers:
    def test_newtypes_are_str_at_runtime(self) -> None:
        # NewType is a runtime no-op; it tags the value for type checkers.
        # The smoke check is that the constructors are importable and pass
        # values through unchanged.
        assert SourceId("vancouver-open-data") == "vancouver-open-data"
        assert DatasetId("business-licences") == "business-licences"
        assert SnapshotId("snap-1") == "snap-1"
        assert MapperId("vancouver-business-licences") == "vancouver-business-licences"


class TestJurisdiction:
    def test_city_level(self) -> None:
        j = Jurisdiction(country="CA", region="BC", locality="Vancouver")

        assert (j.country, j.region, j.locality) == ("CA", "BC", "Vancouver")

    def test_region_only(self) -> None:
        j = Jurisdiction(country="CA", region="BC")

        assert j.locality is None

    def test_country_only(self) -> None:
        j = Jurisdiction(country="CA")

        assert j.region is None and j.locality is None

    def test_country_required(self) -> None:
        with pytest.raises(ValidationError):
            Jurisdiction.model_validate({})

    def test_empty_country_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Jurisdiction(country="")

    def test_surrounding_whitespace_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Jurisdiction(country="CA", region=" BC ")

    def test_frozen(self) -> None:
        j = Jurisdiction(country="CA")

        with pytest.raises(ValidationError):
            j.country = "US"
