import pytest
from pydantic import ValidationError

from civix.core.taxonomy.models.category import CategoryRef


def _category() -> CategoryRef:
    return CategoryRef(
        code="passenger-car",
        label="Passenger car",
        taxonomy_id="civix.transportation-safety.vehicle-category",
        taxonomy_version="2026-05-01",
    )


class TestCategoryRef:
    def test_minimum_fields(self) -> None:
        category = _category()

        assert category.code == "passenger-car"
        assert category.taxonomy_version == "2026-05-01"

    def test_all_fields_required(self) -> None:
        with pytest.raises(ValidationError):
            CategoryRef(  # type: ignore[call-arg]
                code="passenger-car",
                label="Passenger car",
                taxonomy_id="civix.transportation-safety.vehicle-category",
            )

    def test_empty_field_rejected(self) -> None:
        with pytest.raises(ValidationError):
            CategoryRef(
                code="",
                label="Passenger car",
                taxonomy_id="civix.transportation-safety.vehicle-category",
                taxonomy_version="2026-05-01",
            )

    def test_whitespace_rejected(self) -> None:
        with pytest.raises(ValidationError, match="whitespace"):
            CategoryRef(
                code=" passenger-car",
                label="Passenger car",
                taxonomy_id="civix.transportation-safety.vehicle-category",
                taxonomy_version="2026-05-01",
            )

    def test_frozen(self) -> None:
        category = _category()

        with pytest.raises(ValidationError):
            category.code = "truck"  # type: ignore[misc]

    def test_extra_fields_rejected(self) -> None:
        with pytest.raises(ValidationError):
            CategoryRef.model_validate(
                {
                    "code": "passenger-car",
                    "label": "Passenger car",
                    "taxonomy_id": "civix.transportation-safety.vehicle-category",
                    "taxonomy_version": "2026-05-01",
                    "source": "local",
                }
            )
