"""Unit tests for EdmontonBusinessLicencesMapper."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

from civix.core.identity import DatasetId, Jurisdiction, SnapshotId, SourceId
from civix.core.quality import FieldQuality
from civix.core.snapshots import RawRecord, SourceSnapshot
from civix.core.spatial import Address, Coordinate
from civix.domains.business_licences import BusinessLicence
from civix.infra.sources.ca.edmonton_business_licences import (
    MAPPER_ID,
    MAPPER_VERSION,
    EdmontonBusinessLicencesMapper,
)
from civix.infra.sources.ca.edmonton_business_licences.schema import (
    ADAPTER_CONSUMED_FIELDS,
)

PINNED_NOW = datetime(2026, 4, 30, 12, 0, tzinfo=UTC)


def _snapshot() -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId("snap-1"),
        source_id=SourceId("edmonton-open-data"),
        dataset_id=DatasetId("qhi4-bdpu"),
        jurisdiction=Jurisdiction(country="CA", region="AB", locality="Edmonton"),
        fetched_at=PINNED_NOW,
        record_count=1,
    )


def _raw(**fields: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "business_licence_category": "Restaurant or Food Service",
        "business_name": "PRAIRIE CAFE",
        "business_address": "2020, 10060 - JASPER AVENUE NW",
        "externalid": "100031017-001",
        "most_recent_issue_date": "2026-01-15",
        "expiry_date": "2027-01-15",
        "business_improvement_area": "Downtown",
        "neighbourhood_id": "1090",
        "neighbourhood": "Downtown",
        "ward": "O-day'min",
        "latitude": "53.5426116941546",
        "longitude": "-113.49464046380713",
        "location": "(53.5426116941546,-113.49464046380713)",
        "count": "1",
        "geometry_point": "POINT (-113.49464046380713 53.5426116941546)",
        "originalissuedate": "20200115000000",
        "licenceduration": "1 Year",
        "licencetype": "Commercial",
    }
    base.update(fields)

    return base


def _map(
    *,
    source_record_id: str | None = "100031017-001",
    **raw_overrides: Any,
) -> BusinessLicence:
    snap = _snapshot()
    raw = RawRecord(
        snapshot_id=snap.snapshot_id,
        raw_data=_raw(**raw_overrides),
        source_record_id=source_record_id,
    )

    return EdmontonBusinessLicencesMapper()(raw, snap).record


def _map_with_report(**raw_overrides: Any) -> tuple[BusinessLicence, tuple[str, ...]]:
    snap = _snapshot()
    raw = RawRecord(snapshot_id=snap.snapshot_id, raw_data=_raw(**raw_overrides))
    result = EdmontonBusinessLicencesMapper()(raw, snap)

    return result.record, result.report.unmapped_source_fields


class TestBusinessName:
    def test_typical(self) -> None:
        licence = _map(business_name="Prairie Books")

        assert licence.business_name.value == "Prairie Books"
        assert licence.business_name.quality is FieldQuality.DIRECT

    def test_null(self) -> None:
        licence = _map(business_name=None)

        assert licence.business_name.value is None
        assert licence.business_name.quality is FieldQuality.NOT_PROVIDED

    def test_empty_string(self) -> None:
        licence = _map(business_name="  ")

        assert licence.business_name.value is None
        assert licence.business_name.quality is FieldQuality.NOT_PROVIDED


class TestLicenceNumber:
    def test_typical(self) -> None:
        licence = _map(externalid="200001-001")

        assert licence.licence_number.value == "200001-001"
        assert licence.licence_number.quality is FieldQuality.DIRECT

    def test_missing(self) -> None:
        licence = _map(externalid=None)

        assert licence.licence_number.value is None
        assert licence.licence_number.quality is FieldQuality.NOT_PROVIDED


class TestStatus:
    def test_status_is_not_inferred_from_dates(self) -> None:
        licence = _map(expiry_date="2027-01-15")

        assert licence.status.value is None
        assert licence.status.quality is FieldQuality.NOT_PROVIDED
        assert licence.status.source_fields == ("expiry_date",)


class TestCategory:
    def test_single_category(self) -> None:
        licence = _map(business_licence_category="Restaurant or Food Service")

        assert licence.category.value is not None
        assert licence.category.value.code == "restaurant-or-food-service"
        assert licence.category.value.label == "Restaurant or Food Service"
        assert licence.category.value.taxonomy_id == "edmonton-business-licence-categories"
        assert licence.category.value.taxonomy_version == "2026-04-30"
        assert licence.category.quality is FieldQuality.DERIVED

    def test_multi_value_category_uses_first_as_primary(self) -> None:
        licence = _map(
            business_licence_category=(
                "Restaurant or Food Service;Alcohol Sales "
                "(Consumption On-Premises / Minors Allowed)"
            )
        )

        assert licence.category.value is not None
        assert licence.category.value.code == "restaurant-or-food-service"
        assert licence.category.value.label == "Restaurant or Food Service"

    def test_missing_category(self) -> None:
        licence = _map(business_licence_category=None)

        assert licence.category.value is None
        assert licence.category.quality is FieldQuality.NOT_PROVIDED


class TestDates:
    def test_issued_at_typical(self) -> None:
        licence = _map(most_recent_issue_date="2026-01-15")

        assert licence.issued_at.value == date(2026, 1, 15)
        assert licence.issued_at.quality is FieldQuality.STANDARDIZED

    def test_expires_at_typical(self) -> None:
        licence = _map(expiry_date="2027-01-15")

        assert licence.expires_at.value == date(2027, 1, 15)
        assert licence.expires_at.quality is FieldQuality.STANDARDIZED

    def test_null_dates(self) -> None:
        licence = _map(most_recent_issue_date=None, expiry_date=None)

        assert licence.issued_at.value is None
        assert licence.issued_at.quality is FieldQuality.NOT_PROVIDED
        assert licence.expires_at.value is None
        assert licence.expires_at.quality is FieldQuality.NOT_PROVIDED

    def test_unparseable_date(self) -> None:
        licence = _map(most_recent_issue_date="last week")

        assert licence.issued_at.value is None
        assert licence.issued_at.quality is FieldQuality.NOT_PROVIDED


class TestAddress:
    def test_street_with_inferred_jurisdiction_parts(self) -> None:
        licence = _map(business_address="2020, 10060 - JASPER AVENUE NW")

        assert licence.address.value == Address(
            country="CA",
            region="AB",
            locality="Edmonton",
            street="2020, 10060 - JASPER AVENUE NW",
            postal_code=None,
        )
        assert licence.address.quality is FieldQuality.DERIVED

    def test_missing_street_still_carries_inferred_city(self) -> None:
        licence = _map(business_address=None)

        assert licence.address.value == Address(country="CA", region="AB", locality="Edmonton")
        assert licence.address.quality is FieldQuality.DERIVED


class TestCoordinate:
    def test_typical_lat_lon_text(self) -> None:
        licence = _map(latitude="53.5426116941546", longitude="-113.49464046380713")

        assert licence.coordinate.value == Coordinate(
            latitude=53.5426116941546,
            longitude=-113.49464046380713,
        )
        assert licence.coordinate.quality is FieldQuality.STANDARDIZED

    def test_null_latitude(self) -> None:
        licence = _map(latitude=None)

        assert licence.coordinate.value is None
        assert licence.coordinate.quality is FieldQuality.NOT_PROVIDED

    def test_bad_coordinate_text(self) -> None:
        licence = _map(latitude="north", longitude="-113.49464046380713")

        assert licence.coordinate.value is None
        assert licence.coordinate.quality is FieldQuality.NOT_PROVIDED

    def test_out_of_range_coordinate_not_provided(self) -> None:
        licence = _map(latitude="999", longitude="-113.49464046380713")

        assert licence.coordinate.value is None
        assert licence.coordinate.quality is FieldQuality.NOT_PROVIDED


class TestNeighbourhood:
    def test_typical(self) -> None:
        licence = _map(neighbourhood="Downtown")

        assert licence.neighbourhood.value == "Downtown"
        assert licence.neighbourhood.quality is FieldQuality.DIRECT

    def test_missing(self) -> None:
        licence = _map(neighbourhood=None)

        assert licence.neighbourhood.value is None
        assert licence.neighbourhood.quality is FieldQuality.NOT_PROVIDED


class TestProvenance:
    def test_provenance_carries_mapper_version(self) -> None:
        licence = _map()

        assert licence.provenance.mapper.mapper_id == MAPPER_ID
        assert licence.provenance.mapper.version == MAPPER_VERSION

    def test_provenance_threads_source_record_id(self) -> None:
        licence = _map(source_record_id="100099-001")

        assert licence.provenance.source_record_id == "100099-001"


class TestUnmappedSourceFields:
    def test_excludes_adapter_consumed_fields(self) -> None:
        _, unmapped = _map_with_report()

        assert "externalid" not in unmapped

    def test_known_unmapped_set_pinned(self) -> None:
        _, unmapped = _map_with_report()

        assert set(unmapped) == {
            "business_improvement_area",
            "count",
            "geometry_point",
            "licenceduration",
            "licencetype",
            "location",
            "neighbourhood_id",
            "originalissuedate",
            "ward",
        }
        assert list(unmapped) == sorted(unmapped)


class TestAdapterConsumedFieldsSchema:
    def test_schema_pins_adapter_consumed_fields(self) -> None:
        assert "externalid" in ADAPTER_CONSUMED_FIELDS
