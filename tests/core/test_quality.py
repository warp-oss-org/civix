from datetime import date
from enum import StrEnum

import pytest
from pydantic import BaseModel, ValidationError

from civix.core.quality import FieldQuality, MappedField


class _Status(StrEnum):
    ISSUED = "issued"
    EXPIRED = "expired"


class TestFieldQuality:
    def test_all_eight_states_present(self) -> None:
        assert {q.value for q in FieldQuality} == {
            "direct",
            "standardized",
            "derived",
            "inferred",
            "unmapped",
            "conflicted",
            "redacted",
            "not_provided",
        }

    def test_str_enum_value_is_string(self) -> None:
        assert FieldQuality.DIRECT == "direct"


class TestMappedFieldValueRules:
    def test_direct_requires_value(self) -> None:
        with pytest.raises(ValidationError, match="requires a value"):
            MappedField[str](value=None, quality=FieldQuality.DIRECT, source_fields=("x",))

    def test_standardized_requires_value(self) -> None:
        with pytest.raises(ValidationError, match="requires a value"):
            MappedField[str](value=None, quality=FieldQuality.STANDARDIZED, source_fields=("x",))

    def test_derived_requires_value(self) -> None:
        with pytest.raises(ValidationError, match="requires a value"):
            MappedField[str](value=None, quality=FieldQuality.DERIVED, source_fields=("x",))

    def test_inferred_requires_value(self) -> None:
        with pytest.raises(ValidationError, match="requires a value"):
            MappedField[str](value=None, quality=FieldQuality.INFERRED, source_fields=("x",))

    def test_unmapped_forbids_value(self) -> None:
        with pytest.raises(ValidationError, match="forbids a value"):
            MappedField[str](value="x", quality=FieldQuality.UNMAPPED, source_fields=())

    def test_redacted_forbids_value(self) -> None:
        with pytest.raises(ValidationError, match="forbids a value"):
            MappedField[str](value="x", quality=FieldQuality.REDACTED, source_fields=("x",))

    def test_not_provided_forbids_value(self) -> None:
        with pytest.raises(ValidationError, match="forbids a value"):
            MappedField[str](value="x", quality=FieldQuality.NOT_PROVIDED, source_fields=("x",))

    def test_conflicted_with_value_allowed(self) -> None:
        f = MappedField[str](
            value="picked",
            quality=FieldQuality.CONFLICTED,
            source_fields=("a", "b"),
        )

        assert f.value == "picked"

    def test_conflicted_without_value_allowed(self) -> None:
        f = MappedField[str](
            value=None,
            quality=FieldQuality.CONFLICTED,
            source_fields=("a", "b"),
        )

        assert f.value is None


class TestMappedFieldSourceFieldRules:
    def test_unmapped_requires_empty_source_fields(self) -> None:
        with pytest.raises(ValidationError, match="empty"):
            MappedField[str](value=None, quality=FieldQuality.UNMAPPED, source_fields=("x",))

    def test_unmapped_with_empty_source_fields_ok(self) -> None:
        f = MappedField[str](value=None, quality=FieldQuality.UNMAPPED, source_fields=())

        assert f.source_fields == ()

    def test_non_unmapped_requires_at_least_one_source_field(self) -> None:
        with pytest.raises(ValidationError, match="at least one source field"):
            MappedField[str](value="x", quality=FieldQuality.DIRECT, source_fields=())

    def test_redacted_requires_one_source_field(self) -> None:
        with pytest.raises(ValidationError, match="at least one source field"):
            MappedField[str](value=None, quality=FieldQuality.REDACTED, source_fields=())

    def test_conflicted_requires_two_source_fields(self) -> None:
        with pytest.raises(ValidationError, match="at least two source fields"):
            MappedField[str](value="x", quality=FieldQuality.CONFLICTED, source_fields=("a",))

    def test_source_field_whitespace_rejected(self) -> None:
        with pytest.raises(ValidationError, match="whitespace"):
            MappedField[str](value="x", quality=FieldQuality.DIRECT, source_fields=(" a",))

    def test_source_field_empty_rejected(self) -> None:
        with pytest.raises(ValidationError, match="non-empty"):
            MappedField[str](value="x", quality=FieldQuality.DIRECT, source_fields=("",))


class TestMappedFieldGenerics:
    def test_str_value(self) -> None:
        f = MappedField[str](
            value="Joe's Cafe", quality=FieldQuality.DIRECT, source_fields=("name",)
        )

        assert f.value == "Joe's Cafe"

    def test_int_value(self) -> None:
        f = MappedField[int](value=42, quality=FieldQuality.DIRECT, source_fields=("count",))

        assert f.value == 42

    def test_date_value(self) -> None:
        f = MappedField[date](
            value=date(2024, 5, 6),
            quality=FieldQuality.STANDARDIZED,
            source_fields=("issued",),
        )

        assert f.value == date(2024, 5, 6)

    def test_enum_value(self) -> None:
        f = MappedField[_Status](
            value=_Status.ISSUED,
            quality=FieldQuality.STANDARDIZED,
            source_fields=("status",),
        )

        assert f.value is _Status.ISSUED

    def test_use_site_in_a_record_validates_inner_type(self) -> None:
        class _Record(BaseModel):
            model_config = {"strict": True}
            name: MappedField[str]

        with pytest.raises(ValidationError):
            _Record.model_validate(
                {
                    "name": {
                        "value": 123,
                        "quality": "direct",
                        "source_fields": ("x",),
                    }
                }
            )


class TestMappedFieldFrozen:
    def test_frozen(self) -> None:
        f = MappedField[str](value="x", quality=FieldQuality.DIRECT, source_fields=("a",))

        with pytest.raises(ValidationError):
            f.value = "y"  # type: ignore[misc]

    def test_extra_fields_rejected(self) -> None:
        with pytest.raises(ValidationError):
            MappedField[str].model_validate(
                {
                    "value": "x",
                    "quality": "direct",
                    "source_fields": ("a",),
                    "extra": "nope",
                }
            )
