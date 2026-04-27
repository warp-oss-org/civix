from datetime import UTC, date, datetime
from typing import Any

import pytest
from pydantic import ValidationError

from civix.core.identity import (
    DatasetId,
    Jurisdiction,
    MapperId,
    SnapshotId,
    SourceId,
)
from civix.core.provenance import MapperVersion, ProvenanceRef
from civix.core.quality import FieldQuality, MappedField
from civix.core.spatial import Address, Coordinate
from civix.domains.business_licences import (
    BusinessLicence,
    CategoryRef,
    LicenceStatus,
)


def _provenance() -> ProvenanceRef:
    return ProvenanceRef(
        snapshot_id=SnapshotId("snap-1"),
        source_id=SourceId("vancouver-open-data"),
        dataset_id=DatasetId("business-licences"),
        jurisdiction=Jurisdiction(country="CA", region="BC", locality="Vancouver"),
        fetched_at=datetime(2026, 4, 25, 12, 0, tzinfo=UTC),
        mapper=MapperVersion(
            mapper_id=MapperId("vancouver-business-licences"),
            version="0.1.0",
        ),
        source_record_id="VAN-2024-12345",
    )


def _category() -> CategoryRef:
    return CategoryRef(
        code="restaurant",
        label="Restaurant",
        taxonomy_id="civix.business-licences",
        taxonomy_version="2024-05-06",
    )


def _licence(**overrides: Any) -> BusinessLicence:
    defaults: dict[str, Any] = {
        "provenance": _provenance(),
        "business_name": MappedField[str](
            value="Joe's Cafe",
            quality=FieldQuality.DIRECT,
            source_fields=("businessname",),
        ),
        "licence_number": MappedField[str](
            value="24-123456",
            quality=FieldQuality.DIRECT,
            source_fields=("licencenumber",),
        ),
        "status": MappedField[LicenceStatus](
            value=LicenceStatus.ACTIVE,
            quality=FieldQuality.STANDARDIZED,
            source_fields=("status",),
        ),
        "category": MappedField[CategoryRef](
            value=_category(),
            quality=FieldQuality.STANDARDIZED,
            source_fields=("businesstype", "businesssubtype"),
        ),
        "issued_at": MappedField[date](
            value=date(2024, 5, 6),
            quality=FieldQuality.STANDARDIZED,
            source_fields=("issueddate",),
        ),
        "expires_at": MappedField[date](
            value=date(2025, 12, 31),
            quality=FieldQuality.STANDARDIZED,
            source_fields=("expireddate",),
        ),
        "address": MappedField[Address](
            value=Address(
                country="CA",
                region="BC",
                locality="Vancouver",
                street="123 W Pender St",
                postal_code="V6B 1A1",
            ),
            quality=FieldQuality.DERIVED,
            source_fields=("house", "street", "city", "province", "country", "postalcode"),
        ),
        "coordinate": MappedField[Coordinate](
            value=Coordinate(latitude=49.2827, longitude=-123.1207),
            quality=FieldQuality.DIRECT,
            source_fields=("geo_point_2d",),
        ),
        "neighbourhood": MappedField[str](
            value="Downtown",
            quality=FieldQuality.DIRECT,
            source_fields=("localarea",),
        ),
    }
    defaults.update(overrides)

    return BusinessLicence(**defaults)


class TestLicenceStatus:
    def test_cross_jurisdiction_set_present(self) -> None:
        assert {s.value for s in LicenceStatus} == {
            "active",
            "pending",
            "inactive",
            "expired",
            "cancelled",
            "revoked",
            "suspended",
            "surrendered",
            "renewal_due",
            "unknown",
        }


class TestCategoryRef:
    def test_minimum_fields(self) -> None:
        c = _category()

        assert c.code == "restaurant"
        assert c.taxonomy_version == "2024-05-06"

    def test_all_fields_required(self) -> None:
        with pytest.raises(ValidationError):
            CategoryRef(  # type: ignore[call-arg]
                code="restaurant",
                label="Restaurant",
                taxonomy_id="civix.business-licences",
            )

    def test_empty_field_rejected(self) -> None:
        with pytest.raises(ValidationError):
            CategoryRef(
                code="",
                label="Restaurant",
                taxonomy_id="civix.business-licences",
                taxonomy_version="2024-05-06",
            )

    def test_whitespace_rejected(self) -> None:
        with pytest.raises(ValidationError, match="whitespace"):
            CategoryRef(
                code=" restaurant",
                label="Restaurant",
                taxonomy_id="civix.business-licences",
                taxonomy_version="2024-05-06",
            )

    def test_frozen(self) -> None:
        c = _category()

        with pytest.raises(ValidationError):
            c.code = "other"  # type: ignore[misc]


class TestBusinessLicence:
    def test_full_record(self) -> None:
        licence = _licence()

        assert licence.business_name.value == "Joe's Cafe"
        assert licence.status.value is LicenceStatus.ACTIVE
        assert licence.coordinate.value == Coordinate(latitude=49.2827, longitude=-123.1207)

    def test_provenance_carried(self) -> None:
        licence = _licence()

        assert licence.provenance.source_record_id == "VAN-2024-12345"
        assert licence.provenance.jurisdiction.locality == "Vancouver"

    def test_missing_field_uses_not_provided_quality(self) -> None:
        licence = _licence(
            coordinate=MappedField[Coordinate](
                value=None,
                quality=FieldQuality.NOT_PROVIDED,
                source_fields=("geo_point_2d",),
            )
        )

        assert licence.coordinate.value is None
        assert licence.coordinate.quality is FieldQuality.NOT_PROVIDED

    def test_unrecognized_status_uses_inferred_unknown(self) -> None:
        licence = _licence(
            status=MappedField[LicenceStatus](
                value=LicenceStatus.UNKNOWN,
                quality=FieldQuality.INFERRED,
                source_fields=("status",),
            )
        )

        assert licence.status.value is LicenceStatus.UNKNOWN
        assert licence.status.quality is FieldQuality.INFERRED

    def test_redacted_field(self) -> None:
        licence = _licence(
            address=MappedField[Address](
                value=None,
                quality=FieldQuality.REDACTED,
                source_fields=("street",),
            )
        )

        assert licence.address.value is None
        assert licence.address.quality is FieldQuality.REDACTED

    def test_inner_value_type_is_validated(self) -> None:
        with pytest.raises(ValidationError):
            _licence(
                status=MappedField[LicenceStatus].model_validate(
                    {
                        "value": "not-a-real-status",
                        "quality": "standardized",
                        "source_fields": ("status",),
                    }
                )
            )

    def test_provenance_field_required(self) -> None:
        with pytest.raises(ValidationError):
            BusinessLicence.model_validate(
                {
                    "business_name": {
                        "value": "X",
                        "quality": "direct",
                        "source_fields": ("businessname",),
                    }
                }
            )

    def test_frozen(self) -> None:
        licence = _licence()

        with pytest.raises(ValidationError):
            licence.business_name = MappedField[str](  # type: ignore[misc]
                value="Other", quality=FieldQuality.DIRECT, source_fields=("x",)
            )

    def test_extra_fields_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _licence(unexpected="nope")
