"""Unit tests for NycBusinessLicencesMapper."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import pytest

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, SnapshotId, SourceId
from civix.core.quality.models.fields import FieldQuality
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.spatial.models.location import Address, Coordinate
from civix.domains.business_licences.adapters.sources.us.nyc import (
    MAPPER_ID,
    MAPPER_VERSION,
    NycBusinessLicencesMapper,
)
from civix.domains.business_licences.adapters.sources.us.nyc.schema import ADAPTER_CONSUMED_FIELDS
from civix.domains.business_licences.models.licence import BusinessLicence, LicenceStatus

PINNED_NOW = datetime(2026, 4, 30, 12, 0, tzinfo=UTC)


def _snapshot() -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId("snap-1"),
        source_id=SourceId("nyc-open-data"),
        dataset_id=DatasetId("w7w3-xahh"),
        jurisdiction=Jurisdiction(country="US", region="NY", locality="New York"),
        fetched_at=PINNED_NOW,
        record_count=1,
    )


def _raw(**fields: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "license_nbr": "0002902-DCA",
        "business_name": "GEM FINANCIAL SERVICES, INC.",
        "dba_trade_name": "GEM PAWNBROKERS",
        "business_unique_id": "BA-1216876-2022",
        "business_category": "Pawnbroker",
        "license_type": "Premises",
        "license_status": "Ready for Renewal",
        "license_creation_date": "2007-04-18T00:00:00.000",
        "lic_expir_dd": "2026-04-30T00:00:00.000",
        "detail": None,
        "contact_phone": "7182371166",
        "address_type": "Complete Address",
        "address_building": "608",
        "address_street_name": "8TH AVE",
        "address_street_name_2": None,
        "street3": None,
        "unit_type": None,
        "apt_suite": None,
        "address_city": "NEW YORK",
        "address_state": "NY",
        "address_zip": "10018",
        "address_borough": "Manhattan",
        "community_board": "105",
        "council_district": "03",
        "bin": "1014495",
        "bbl": "1007890005",
        "nta": "MN17",
        "census_block_2010_": "1004",
        "census_tract": "113",
        "latitude": "40.755613",
        "longitude": "-73.990962",
    }
    base.update(fields)

    return base


def _map(*, source_record_id: str | None = "0002902-DCA", **raw_overrides: Any) -> BusinessLicence:
    snap = _snapshot()
    raw = RawRecord(
        snapshot_id=snap.snapshot_id,
        raw_data=_raw(**raw_overrides),
        source_record_id=source_record_id,
    )

    return NycBusinessLicencesMapper()(raw, snap).record


def _map_with_report(**raw_overrides: Any) -> tuple[BusinessLicence, tuple[str, ...]]:
    snap = _snapshot()
    raw = RawRecord(snapshot_id=snap.snapshot_id, raw_data=_raw(**raw_overrides))
    result = NycBusinessLicencesMapper()(raw, snap)

    return result.record, result.report.unmapped_source_fields


class TestBusinessName:
    def test_dba_preferred(self) -> None:
        licence = _map(business_name="Legal LLC", dba_trade_name="Storefront")

        assert licence.business_name.value == "Storefront"
        assert licence.business_name.quality is FieldQuality.DIRECT
        assert licence.business_name.source_fields == ("dba_trade_name",)

    def test_legal_name_fallback(self) -> None:
        licence = _map(business_name="Legal LLC", dba_trade_name=None)

        assert licence.business_name.value == "Legal LLC"
        assert licence.business_name.quality is FieldQuality.DIRECT
        assert licence.business_name.source_fields == ("dba_trade_name", "business_name")

    def test_missing(self) -> None:
        licence = _map(business_name=None, dba_trade_name=None)

        assert licence.business_name.value is None
        assert licence.business_name.quality is FieldQuality.NOT_PROVIDED


class TestLicenceNumber:
    def test_typical(self) -> None:
        licence = _map(license_nbr="0016371-DCA")

        assert licence.licence_number.value == "0016371-DCA"
        assert licence.licence_number.quality is FieldQuality.DIRECT

    def test_missing(self) -> None:
        licence = _map(license_nbr=None)

        assert licence.licence_number.value is None
        assert licence.licence_number.quality is FieldQuality.NOT_PROVIDED


class TestStatus:
    @pytest.mark.parametrize(
        "source_value,expected",
        [
            ("Active", LicenceStatus.ACTIVE),
            ("Expired", LicenceStatus.EXPIRED),
            ("Surrendered", LicenceStatus.SURRENDERED),
            ("Revoked", LicenceStatus.REVOKED),
            ("Suspended", LicenceStatus.SUSPENDED),
            ("Ready for Renewal", LicenceStatus.RENEWAL_DUE),
        ],
    )
    def test_known_statuses_standardized(
        self,
        source_value: str,
        expected: LicenceStatus,
    ) -> None:
        licence = _map(license_status=source_value)

        assert licence.status.value is expected
        assert licence.status.quality is FieldQuality.STANDARDIZED

    @pytest.mark.parametrize(
        "source_value",
        [
            "Failed to Renew",
            "Voided",
            "Out of Business",
            "Close",
            "TOL",
            "Paused",
        ],
    )
    def test_unknown_statuses_inferred_unknown(self, source_value: str) -> None:
        licence = _map(license_status=source_value)

        assert licence.status.value is LicenceStatus.UNKNOWN
        assert licence.status.quality is FieldQuality.INFERRED

    def test_missing(self) -> None:
        licence = _map(license_status=None)

        assert licence.status.value is None
        assert licence.status.quality is FieldQuality.NOT_PROVIDED


class TestCategory:
    def test_typical(self) -> None:
        licence = _map(business_category="Secondhand Dealer - General")

        assert licence.category.value is not None
        assert licence.category.value.code == "secondhand-dealer-general"
        assert licence.category.value.label == "Secondhand Dealer - General"
        assert licence.category.value.taxonomy_id == "nyc-dcwp-business-categories"
        assert licence.category.value.taxonomy_version == "2026-04-30"
        assert licence.category.quality is FieldQuality.DERIVED

    def test_missing(self) -> None:
        licence = _map(business_category=None)

        assert licence.category.value is None
        assert licence.category.quality is FieldQuality.NOT_PROVIDED


class TestDates:
    def test_issued_at_typical(self) -> None:
        licence = _map(license_creation_date="2007-04-18T00:00:00.000")

        assert licence.issued_at.value == date(2007, 4, 18)
        assert licence.issued_at.quality is FieldQuality.STANDARDIZED

    def test_expires_at_typical(self) -> None:
        licence = _map(lic_expir_dd="2026-04-30T00:00:00.000")

        assert licence.expires_at.value == date(2026, 4, 30)
        assert licence.expires_at.quality is FieldQuality.STANDARDIZED

    def test_null_dates(self) -> None:
        licence = _map(license_creation_date=None, lic_expir_dd=None)

        assert licence.issued_at.value is None
        assert licence.issued_at.quality is FieldQuality.NOT_PROVIDED
        assert licence.expires_at.value is None
        assert licence.expires_at.quality is FieldQuality.NOT_PROVIDED

    def test_unparseable_date(self) -> None:
        licence = _map(license_creation_date="last week")

        assert licence.issued_at.value is None
        assert licence.issued_at.quality is FieldQuality.NOT_PROVIDED


class TestAddress:
    def test_street_with_jurisdiction_parts(self) -> None:
        licence = _map(
            address_building="608",
            address_street_name="8TH AVE",
            address_city="NEW YORK",
            address_state="NY",
            address_zip="10018",
        )

        assert licence.address.value == Address(
            country="US",
            region="NY",
            locality="NEW YORK",
            street="608 8TH AVE",
            postal_code="10018",
        )

        assert licence.address.quality is FieldQuality.DERIVED

    def test_street_with_unit(self) -> None:
        licence = _map(
            address_building="480",
            address_street_name="PARK AVENUE",
            unit_type="Suite",
            apt_suite="1200",
        )

        assert licence.address.value is not None
        assert licence.address.value.street == "480 PARK AVENUE Suite 1200"

    def test_unit_without_unit_type_uses_default_label(self) -> None:
        licence = _map(unit_type=None, apt_suite="2B")

        assert licence.address.value is not None
        assert licence.address.value.street == "608 8TH AVE Unit 2B"

    def test_missing_street_still_carries_address_parts(self) -> None:
        licence = _map(address_building=None, address_street_name=None)

        assert licence.address.value == Address(
            country="US",
            region="NY",
            locality="NEW YORK",
            postal_code="10018",
        )

        assert licence.address.quality is FieldQuality.DERIVED


class TestCoordinate:
    def test_typical_lat_lon_text(self) -> None:
        licence = _map(latitude="40.755613", longitude="-73.990962")

        assert licence.coordinate.value == Coordinate(latitude=40.755613, longitude=-73.990962)
        assert licence.coordinate.quality is FieldQuality.STANDARDIZED

    def test_numeric_lat_lon(self) -> None:
        licence = _map(latitude=40.76248502732357, longitude=-73.97012500177938)

        assert licence.coordinate.value == Coordinate(
            latitude=40.76248502732357,
            longitude=-73.97012500177938,
        )

    def test_null_latitude(self) -> None:
        licence = _map(latitude=None)

        assert licence.coordinate.value is None
        assert licence.coordinate.quality is FieldQuality.NOT_PROVIDED

    def test_bad_coordinate_text(self) -> None:
        licence = _map(latitude="north", longitude="-73.990962")

        assert licence.coordinate.value is None
        assert licence.coordinate.quality is FieldQuality.NOT_PROVIDED

    def test_out_of_range_coordinate_not_provided(self) -> None:
        licence = _map(latitude="999", longitude="-73.990962")

        assert licence.coordinate.value is None
        assert licence.coordinate.quality is FieldQuality.NOT_PROVIDED


class TestNeighbourhood:
    def test_unmapped(self) -> None:
        licence = _map()

        assert licence.neighbourhood.value is None
        assert licence.neighbourhood.quality is FieldQuality.UNMAPPED
        assert licence.neighbourhood.source_fields == ()


class TestProvenance:
    def test_provenance_carries_mapper_version(self) -> None:
        licence = _map()

        assert licence.provenance.mapper.mapper_id == MAPPER_ID
        assert licence.provenance.mapper.version == MAPPER_VERSION

    def test_provenance_threads_source_record_id(self) -> None:
        licence = _map(source_record_id="0016371-DCA")

        assert licence.provenance.source_record_id == "0016371-DCA"


class TestUnmappedSourceFields:
    def test_excludes_adapter_consumed_fields(self) -> None:
        _, unmapped = _map_with_report()

        assert "license_nbr" not in unmapped

    def test_known_unmapped_set_pinned(self) -> None:
        _, unmapped = _map_with_report()

        assert set(unmapped) == {
            "address_borough",
            "address_type",
            "bbl",
            "bin",
            "business_name",
            "business_unique_id",
            "census_block_2010_",
            "census_tract",
            "community_board",
            "contact_phone",
            "council_district",
            "detail",
            "license_type",
            "nta",
        }

        assert list(unmapped) == sorted(unmapped)


class TestAdapterConsumedFieldsSchema:
    def test_schema_pins_adapter_consumed_fields(self) -> None:
        assert "license_nbr" in ADAPTER_CONSUMED_FIELDS
