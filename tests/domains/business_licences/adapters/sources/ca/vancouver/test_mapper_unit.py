"""Unit tests for VancouverBusinessLicencesMapper.

Tests go through the public mapper interface only — the per-field
helpers are private to the module. Each test overrides the relevant
field(s) on a complete baseline raw row and asserts on the resulting
`BusinessLicence`.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import pytest

from civix.core.identity.models.identifiers import (
    DatasetId,
    Jurisdiction,
    SnapshotId,
    SourceId,
)
from civix.core.quality.models.fields import FieldQuality
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.spatial.models.location import Address, Coordinate
from civix.domains.business_licences.adapters.sources.ca.vancouver import (
    MAPPER_ID,
    MAPPER_VERSION,
    VancouverBusinessLicencesMapper,
)
from civix.domains.business_licences.adapters.sources.ca.vancouver.schema import (
    ADAPTER_CONSUMED_FIELDS,
)
from civix.domains.business_licences.models.licence import BusinessLicence, LicenceStatus

PINNED_NOW = datetime(2026, 4, 25, 12, 0, tzinfo=UTC)


def _snapshot() -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId("snap-1"),
        source_id=SourceId("vancouver-open-data"),
        dataset_id=DatasetId("business-licences"),
        jurisdiction=Jurisdiction(country="CA", region="BC", locality="Vancouver"),
        fetched_at=PINNED_NOW,
        record_count=1,
    )


def _raw(**fields: Any) -> dict[str, Any]:
    """A complete raw row with every Vancouver field, overrideable per test."""
    base: dict[str, Any] = {
        "folderyear": "24",
        "licencersn": "1",
        "licencenumber": "24-000001",
        "licencerevisionnumber": "00",
        "businessname": "Test Co",
        "businesstradename": None,
        "status": "Issued",
        "issueddate": "2024-05-06T00:00:00+00:00",
        "expireddate": "2025-12-31",
        "businesstype": "Restaurant",
        "businesssubtype": None,
        "unit": None,
        "unittype": None,
        "house": "100",
        "street": "Main St",
        "city": "Vancouver",
        "province": "BC",
        "country": "CA",
        "postalcode": "V6B 1A1",
        "localarea": "Downtown",
        "numberofemployees": 5.0,
        "feepaid": 100.0,
        "extractdate": "2026-04-25T00:00:00+00:00",
        "geom": None,
        "geo_point_2d": {"lon": -123.1207, "lat": 49.2827},
    }
    base.update(fields)

    return base


def _map(*, source_record_id: str | None = "VAN-1", **raw_overrides: Any) -> BusinessLicence:
    """Run the mapper against a raw row built from defaults + overrides."""
    snap = _snapshot()
    raw = RawRecord(
        snapshot_id=snap.snapshot_id,
        raw_data=_raw(**raw_overrides),
        source_record_id=source_record_id,
    )

    return VancouverBusinessLicencesMapper()(raw, snap).record


def _map_with_report(**raw_overrides: Any) -> tuple[BusinessLicence, tuple[str, ...]]:
    snap = _snapshot()
    raw = RawRecord(snapshot_id=snap.snapshot_id, raw_data=_raw(**raw_overrides))
    result = VancouverBusinessLicencesMapper()(raw, snap)

    return result.record, result.report.unmapped_source_fields


class TestBusinessName:
    def test_typical(self) -> None:
        licence = _map(businessname="Joe's Cafe")

        assert licence.business_name.value == "Joe's Cafe"
        assert licence.business_name.quality is FieldQuality.DIRECT

    def test_redacted_sentinel(self) -> None:
        licence = _map(businessname="REDACTED")

        assert licence.business_name.value is None
        assert licence.business_name.quality is FieldQuality.REDACTED

    def test_null(self) -> None:
        licence = _map(businessname=None)

        assert licence.business_name.value is None
        assert licence.business_name.quality is FieldQuality.NOT_PROVIDED

    def test_empty_string(self) -> None:
        licence = _map(businessname="")

        assert licence.business_name.value is None
        assert licence.business_name.quality is FieldQuality.NOT_PROVIDED


class TestLicenceNumber:
    def test_typical(self) -> None:
        licence = _map(licencenumber="24-123456")

        assert licence.licence_number.value == "24-123456"
        assert licence.licence_number.quality is FieldQuality.DIRECT

    def test_missing(self) -> None:
        licence = _map(licencenumber=None)

        assert licence.licence_number.value is None
        assert licence.licence_number.quality is FieldQuality.NOT_PROVIDED


class TestStatus:
    @pytest.mark.parametrize(
        "source_value,expected",
        [
            ("Issued", LicenceStatus.ACTIVE),
            ("Active", LicenceStatus.ACTIVE),
            ("Pending", LicenceStatus.PENDING),
            ("Inactive", LicenceStatus.INACTIVE),
            ("Expired", LicenceStatus.EXPIRED),
            ("Cancelled", LicenceStatus.CANCELLED),
            ("Gone Out of Business", LicenceStatus.INACTIVE),
        ],
    )
    def test_known_status_standardized(self, source_value: str, expected: LicenceStatus) -> None:
        licence = _map(status=source_value)

        assert licence.status.value is expected
        assert licence.status.quality is FieldQuality.STANDARDIZED

    def test_case_insensitive(self) -> None:
        licence = _map(status="ISSUED")

        assert licence.status.value is LicenceStatus.ACTIVE
        assert licence.status.quality is FieldQuality.STANDARDIZED

    def test_whitespace_tolerated(self) -> None:
        licence = _map(status="  Issued  ")

        assert licence.status.value is LicenceStatus.ACTIVE

    def test_unknown_value_inferred_unknown(self) -> None:
        licence = _map(status="Quantum Superposition")

        assert licence.status.value is LicenceStatus.UNKNOWN
        assert licence.status.quality is FieldQuality.INFERRED

    def test_null(self) -> None:
        licence = _map(status=None)

        assert licence.status.value is None
        assert licence.status.quality is FieldQuality.NOT_PROVIDED

    def test_empty_string(self) -> None:
        licence = _map(status="")

        assert licence.status.value is None
        assert licence.status.quality is FieldQuality.NOT_PROVIDED

    def test_non_string_type_inferred_unknown(self) -> None:
        licence = _map(status=42)

        assert licence.status.value is LicenceStatus.UNKNOWN
        assert licence.status.quality is FieldQuality.INFERRED


class TestCategory:
    def test_type_only(self) -> None:
        licence = _map(businesstype="Restaurant", businesssubtype=None)

        assert licence.category.value is not None
        assert licence.category.value.code == "restaurant"
        assert licence.category.value.label == "Restaurant"
        assert licence.category.value.taxonomy_id == "vancouver-business-types"
        assert licence.category.value.taxonomy_version == "2024-05-06"
        assert licence.category.quality is FieldQuality.DERIVED
        assert licence.category.source_fields == ("businesstype", "businesssubtype")

    def test_type_and_subtype_combined(self) -> None:
        licence = _map(businesstype="Restaurant", businesssubtype="Class 1")

        assert licence.category.value is not None
        assert licence.category.value.code == "restaurant-class-1"
        assert licence.category.value.label == "Restaurant - Class 1"

    def test_missing_type_not_provided(self) -> None:
        licence = _map(businesstype=None, businesssubtype=None)

        assert licence.category.value is None
        assert licence.category.quality is FieldQuality.NOT_PROVIDED

    def test_messy_label_slugified(self) -> None:
        licence = _map(
            businesstype="Health Care Professionals & Services",
            businesssubtype=None,
        )

        assert licence.category.value is not None
        assert licence.category.value.code == "health-care-professionals-services"

    def test_multi_space_collapses_to_single_hyphen(self) -> None:
        licence = _map(businesstype="Multiple   Spaces", businesssubtype=None)

        assert licence.category.value is not None
        assert licence.category.value.code == "multiple-spaces"


class TestIssuedAt:
    def test_utc_midnight_lands_on_previous_day_in_vancouver(self) -> None:
        # 2024-05-06T00:00 UTC == 2024-05-05 17:00 PDT.
        licence = _map(issueddate="2024-05-06T00:00:00+00:00")

        assert licence.issued_at.value == date(2024, 5, 5)
        assert licence.issued_at.quality is FieldQuality.STANDARDIZED

    def test_vancouver_local_morning_stays_same_day(self) -> None:
        # 2024-05-06T17:00 UTC == 2024-05-06 10:00 PDT.
        licence = _map(issueddate="2024-05-06T17:00:00+00:00")

        assert licence.issued_at.value == date(2024, 5, 6)

    def test_missing(self) -> None:
        licence = _map(issueddate=None)

        assert licence.issued_at.value is None
        assert licence.issued_at.quality is FieldQuality.NOT_PROVIDED

    def test_unparseable(self) -> None:
        licence = _map(issueddate="yesterday")

        assert licence.issued_at.value is None
        assert licence.issued_at.quality is FieldQuality.NOT_PROVIDED


class TestExpiresAt:
    def test_typical(self) -> None:
        licence = _map(expireddate="2025-12-31")

        assert licence.expires_at.value == date(2025, 12, 31)
        assert licence.expires_at.quality is FieldQuality.STANDARDIZED

    def test_missing(self) -> None:
        licence = _map(expireddate=None)

        assert licence.expires_at.value is None
        assert licence.expires_at.quality is FieldQuality.NOT_PROVIDED

    def test_unparseable(self) -> None:
        licence = _map(expireddate="next year")

        assert licence.expires_at.value is None
        assert licence.expires_at.quality is FieldQuality.NOT_PROVIDED


class TestAddress:
    def test_full_address(self) -> None:
        licence = _map()

        assert licence.address.value == Address(
            country="CA",
            region="BC",
            locality="Vancouver",
            street="100 Main St",
            postal_code="V6B 1A1",
        )
        assert licence.address.quality is FieldQuality.DERIVED

    def test_with_suite_unit(self) -> None:
        licence = _map(unit="500", unittype="Suite")

        assert licence.address.value is not None
        assert licence.address.value.street == "100 Main St Suite 500"

    def test_unit_without_unittype_uses_default_label(self) -> None:
        licence = _map(unit="500", unittype=None)

        assert licence.address.value is not None
        assert licence.address.value.street == "100 Main St Unit 500"

    def test_house_only_no_street(self) -> None:
        licence = _map(house="100", street=None)

        assert licence.address.value is not None
        assert licence.address.value.street == "100"

    def test_street_only_no_house(self) -> None:
        licence = _map(house=None, street="Main St")

        assert licence.address.value is not None
        assert licence.address.value.street == "Main St"

    def test_country_only(self) -> None:
        licence = _map(
            province=None,
            city=None,
            house=None,
            street=None,
            postalcode=None,
            unit=None,
            unittype=None,
        )

        assert licence.address.value == Address(country="CA")
        assert licence.address.quality is FieldQuality.DERIVED

    def test_partial_address_when_house_redacted(self) -> None:
        # Typical Vancouver redaction: street/postal/house null but
        # city/province/country present. Address survives partially.
        licence = _map(
            house=None,
            street=None,
            postalcode=None,
            unit=None,
            unittype=None,
        )

        assert licence.address.value == Address(
            country="CA",
            region="BC",
            locality="Vancouver",
        )

    def test_missing_country_not_provided(self) -> None:
        licence = _map(country=None)

        assert licence.address.value is None
        assert licence.address.quality is FieldQuality.NOT_PROVIDED


class TestCoordinate:
    def test_typical(self) -> None:
        licence = _map()

        assert licence.coordinate.value == Coordinate(latitude=49.2827, longitude=-123.1207)
        assert licence.coordinate.quality is FieldQuality.STANDARDIZED

    def test_null_geo_point(self) -> None:
        licence = _map(geo_point_2d=None)

        assert licence.coordinate.value is None
        assert licence.coordinate.quality is FieldQuality.NOT_PROVIDED

    def test_missing_lat(self) -> None:
        licence = _map(geo_point_2d={"lon": -123.0})

        assert licence.coordinate.value is None
        assert licence.coordinate.quality is FieldQuality.NOT_PROVIDED

    def test_out_of_range_lat_degrades_to_not_provided(self) -> None:
        licence = _map(geo_point_2d={"lat": 999.0, "lon": 0.0})

        assert licence.coordinate.value is None
        assert licence.coordinate.quality is FieldQuality.NOT_PROVIDED

    def test_string_lat_not_provided(self) -> None:
        licence = _map(geo_point_2d={"lat": "49.0", "lon": -123.0})

        assert licence.coordinate.value is None
        assert licence.coordinate.quality is FieldQuality.NOT_PROVIDED


class TestNeighbourhood:
    def test_typical(self) -> None:
        licence = _map(localarea="Downtown")

        assert licence.neighbourhood.value == "Downtown"
        assert licence.neighbourhood.quality is FieldQuality.DIRECT

    def test_missing(self) -> None:
        licence = _map(localarea=None)

        assert licence.neighbourhood.value is None
        assert licence.neighbourhood.quality is FieldQuality.NOT_PROVIDED


class TestProvenance:
    def test_provenance_carries_mapper_version(self) -> None:
        licence = _map()

        assert licence.provenance.mapper.mapper_id == MAPPER_ID
        assert licence.provenance.mapper.version == MAPPER_VERSION

    def test_provenance_carries_snapshot_id(self) -> None:
        licence = _map()

        assert licence.provenance.snapshot_id == _snapshot().snapshot_id

    def test_provenance_threads_source_record_id(self) -> None:
        licence = _map(source_record_id="VAN-42")

        assert licence.provenance.source_record_id == "VAN-42"


class TestUnmappedSourceFields:
    def test_excludes_adapter_consumed_fields(self) -> None:
        _, unmapped = _map_with_report()

        assert "licencersn" not in unmapped
        assert "extractdate" not in unmapped

    def test_includes_fields_mapper_chose_not_to_surface(self) -> None:
        _, unmapped = _map_with_report()

        for name in (
            "businesstradename",
            "feepaid",
            "folderyear",
            "geom",
            "licencerevisionnumber",
            "numberofemployees",
        ):
            assert name in unmapped, f"expected {name} in unmapped"

    def test_unmapped_list_is_sorted(self) -> None:
        _, unmapped = _map_with_report()

        assert list(unmapped) == sorted(unmapped)

    def test_known_unmapped_set_pinned(self) -> None:
        # Pinned so a new source field fails loudly here rather than
        # slipping silently into unmapped. Forces a maintainer decision.
        _, unmapped = _map_with_report()

        assert set(unmapped) == {
            "businesstradename",
            "feepaid",
            "folderyear",
            "geom",
            "licencerevisionnumber",
            "numberofemployees",
        }


class TestAdapterConsumedFieldsSchema:
    def test_schema_pins_adapter_consumed_fields(self) -> None:
        # Pinned because the mapper's unmapped_source_fields builder
        # depends on this set; removal here would silently miscount.
        assert "licencersn" in ADAPTER_CONSUMED_FIELDS
        assert "extractdate" in ADAPTER_CONSUMED_FIELDS
