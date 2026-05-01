"""Unit tests for CalgaryBusinessLicencesMapper."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import pytest

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, SnapshotId, SourceId
from civix.core.quality.models.fields import FieldQuality
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.spatial.models.location import Address, Coordinate
from civix.domains.business_licences.adapters.sources.ca.calgary import (
    MAPPER_ID,
    MAPPER_VERSION,
    CalgaryBusinessLicencesMapper,
)
from civix.domains.business_licences.adapters.sources.ca.calgary.schema import (
    ADAPTER_CONSUMED_FIELDS,
)
from civix.domains.business_licences.models.licence import BusinessLicence, LicenceStatus

PINNED_NOW = datetime(2026, 4, 29, 12, 0, tzinfo=UTC)


def _snapshot() -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId("snap-1"),
        source_id=SourceId("calgary-open-data"),
        dataset_id=DatasetId("vdjc-pybd"),
        jurisdiction=Jurisdiction(country="CA", region="AB", locality="Calgary"),
        fetched_at=PINNED_NOW,
        record_count=1,
    )


def _raw(**fields: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "getbusid": "100001",
        "tradename": "PRAIRIE CAFE",
        "homeoccind": "N",
        "address": "100 MAIN ST SE",
        "comdistcd": "DWT",
        "comdistnm": "DOWNTOWN COMMERCIAL CORE",
        "licencetypes": "FOOD SERVICE - PREMISES",
        "first_iss_dt": "2020-01-15T00:00:00.000",
        "exp_dt": "2026-01-14T00:00:00.000",
        "jobstatusdesc": "Licensed",
        "point": {"type": "Point", "coordinates": [-114.0719, 51.0447]},
        "globalid": "11111111-1111-1111-1111-111111111111",
    }
    base.update(fields)

    return base


def _map(*, source_record_id: str | None = "100001", **raw_overrides: Any) -> BusinessLicence:
    snap = _snapshot()
    raw = RawRecord(
        snapshot_id=snap.snapshot_id,
        raw_data=_raw(**raw_overrides),
        source_record_id=source_record_id,
    )

    return CalgaryBusinessLicencesMapper()(raw, snap).record


def _map_with_report(**raw_overrides: Any) -> tuple[BusinessLicence, tuple[str, ...]]:
    snap = _snapshot()
    raw = RawRecord(snapshot_id=snap.snapshot_id, raw_data=_raw(**raw_overrides))
    result = CalgaryBusinessLicencesMapper()(raw, snap)

    return result.record, result.report.unmapped_source_fields


class TestBusinessName:
    def test_typical(self) -> None:
        licence = _map(tradename="Prairie Books")

        assert licence.business_name.value == "Prairie Books"
        assert licence.business_name.quality is FieldQuality.DIRECT

    def test_null(self) -> None:
        licence = _map(tradename=None)

        assert licence.business_name.value is None
        assert licence.business_name.quality is FieldQuality.NOT_PROVIDED

    def test_empty_string(self) -> None:
        licence = _map(tradename="  ")

        assert licence.business_name.value is None
        assert licence.business_name.quality is FieldQuality.NOT_PROVIDED


class TestLicenceNumber:
    def test_typical(self) -> None:
        licence = _map(getbusid="200001")

        assert licence.licence_number.value == "200001"
        assert licence.licence_number.quality is FieldQuality.DIRECT

    def test_missing(self) -> None:
        licence = _map(getbusid=None)

        assert licence.licence_number.value is None
        assert licence.licence_number.quality is FieldQuality.NOT_PROVIDED


class TestStatus:
    @pytest.mark.parametrize(
        "source_value,expected,quality",
        [
            ("Licensed", LicenceStatus.ACTIVE, FieldQuality.STANDARDIZED),
            ("Renewal Licensed", LicenceStatus.ACTIVE, FieldQuality.STANDARDIZED),
            ("Pending Renewal", LicenceStatus.RENEWAL_DUE, FieldQuality.STANDARDIZED),
            ("Renewal Invoiced", LicenceStatus.RENEWAL_DUE, FieldQuality.STANDARDIZED),
            (
                "Renewal Notification Sent",
                LicenceStatus.RENEWAL_DUE,
                FieldQuality.STANDARDIZED,
            ),
            ("Move in Progress", LicenceStatus.UNKNOWN, FieldQuality.INFERRED),
            ("Close in Progress", LicenceStatus.UNKNOWN, FieldQuality.INFERRED),
        ],
    )
    def test_known_statuses(
        self,
        source_value: str,
        expected: LicenceStatus,
        quality: FieldQuality,
    ) -> None:
        licence = _map(jobstatusdesc=source_value)

        assert licence.status.value is expected
        assert licence.status.quality is quality

    def test_case_insensitive(self) -> None:
        licence = _map(jobstatusdesc=" licensed ")

        assert licence.status.value is LicenceStatus.ACTIVE
        assert licence.status.quality is FieldQuality.STANDARDIZED

    def test_unknown_value_inferred_unknown(self) -> None:
        licence = _map(jobstatusdesc="Paused for Review")

        assert licence.status.value is LicenceStatus.UNKNOWN
        assert licence.status.quality is FieldQuality.INFERRED

    def test_null(self) -> None:
        licence = _map(jobstatusdesc=None)

        assert licence.status.value is None
        assert licence.status.quality is FieldQuality.NOT_PROVIDED

    def test_empty_string(self) -> None:
        licence = _map(jobstatusdesc="")

        assert licence.status.value is None
        assert licence.status.quality is FieldQuality.NOT_PROVIDED


class TestCategory:
    def test_single_category(self) -> None:
        licence = _map(licencetypes="FOOD SERVICE - PREMISES")

        assert licence.category.value is not None
        assert licence.category.value.code == "food-service-premises"
        assert licence.category.value.label == "FOOD SERVICE - PREMISES"
        assert licence.category.value.taxonomy_id == "calgary-business-licence-types"
        assert licence.category.value.taxonomy_version == "2026-04-29"
        assert licence.category.quality is FieldQuality.DERIVED

    def test_multi_value_category_uses_first_as_primary(self) -> None:
        licence = _map(licencetypes="RETAIL DEALER - PREMISES,\nFOOD SERVICE - PREMISES")

        assert licence.category.value is not None
        assert licence.category.value.code == "retail-dealer-premises"
        assert licence.category.value.label == "RETAIL DEALER - PREMISES"

    def test_missing_category(self) -> None:
        licence = _map(licencetypes=None)

        assert licence.category.value is None
        assert licence.category.quality is FieldQuality.NOT_PROVIDED


class TestDates:
    def test_issued_at_typical(self) -> None:
        licence = _map(first_iss_dt="2020-01-15T00:00:00.000")

        assert licence.issued_at.value == date(2020, 1, 15)
        assert licence.issued_at.quality is FieldQuality.STANDARDIZED

    def test_expires_at_typical(self) -> None:
        licence = _map(exp_dt="2026-01-14T00:00:00.000Z")

        assert licence.expires_at.value == date(2026, 1, 14)
        assert licence.expires_at.quality is FieldQuality.STANDARDIZED

    def test_null_dates(self) -> None:
        licence = _map(first_iss_dt=None, exp_dt=None)

        assert licence.issued_at.value is None
        assert licence.issued_at.quality is FieldQuality.NOT_PROVIDED
        assert licence.expires_at.value is None
        assert licence.expires_at.quality is FieldQuality.NOT_PROVIDED

    def test_unparseable_date(self) -> None:
        licence = _map(first_iss_dt="last week")

        assert licence.issued_at.value is None
        assert licence.issued_at.quality is FieldQuality.NOT_PROVIDED


class TestAddress:
    def test_street_with_inferred_jurisdiction_parts(self) -> None:
        licence = _map(address="100 MAIN ST SE")

        assert licence.address.value == Address(
            country="CA",
            region="AB",
            locality="Calgary",
            street="100 MAIN ST SE",
            postal_code=None,
        )
        assert licence.address.quality is FieldQuality.DERIVED

    def test_missing_street_still_carries_inferred_city(self) -> None:
        licence = _map(address=None)

        assert licence.address.value == Address(country="CA", region="AB", locality="Calgary")
        assert licence.address.quality is FieldQuality.DERIVED


class TestCoordinate:
    def test_typical_socrata_point(self) -> None:
        licence = _map(point={"type": "Point", "coordinates": [-114.0719, 51.0447]})

        assert licence.coordinate.value == Coordinate(latitude=51.0447, longitude=-114.0719)
        assert licence.coordinate.quality is FieldQuality.STANDARDIZED

    def test_null_point(self) -> None:
        licence = _map(point=None)

        assert licence.coordinate.value is None
        assert licence.coordinate.quality is FieldQuality.NOT_PROVIDED

    def test_wrong_type_not_provided(self) -> None:
        licence = _map(point={"type": "LineString", "coordinates": [-114.0, 51.0]})

        assert licence.coordinate.value is None
        assert licence.coordinate.quality is FieldQuality.NOT_PROVIDED

    def test_missing_lat(self) -> None:
        licence = _map(point={"type": "Point", "coordinates": [-114.0]})

        assert licence.coordinate.value is None
        assert licence.coordinate.quality is FieldQuality.NOT_PROVIDED

    def test_string_coordinate_not_provided(self) -> None:
        licence = _map(point={"type": "Point", "coordinates": ["-114.0", 51.0]})

        assert licence.coordinate.value is None
        assert licence.coordinate.quality is FieldQuality.NOT_PROVIDED

    def test_out_of_range_coordinate_not_provided(self) -> None:
        licence = _map(point={"type": "Point", "coordinates": [-114.0, 999.0]})

        assert licence.coordinate.value is None
        assert licence.coordinate.quality is FieldQuality.NOT_PROVIDED


class TestNeighbourhood:
    def test_typical(self) -> None:
        licence = _map(comdistnm="MISSION")

        assert licence.neighbourhood.value == "MISSION"
        assert licence.neighbourhood.quality is FieldQuality.DIRECT

    def test_missing(self) -> None:
        licence = _map(comdistnm=None)

        assert licence.neighbourhood.value is None
        assert licence.neighbourhood.quality is FieldQuality.NOT_PROVIDED


class TestProvenance:
    def test_provenance_carries_mapper_version(self) -> None:
        licence = _map()

        assert licence.provenance.mapper.mapper_id == MAPPER_ID
        assert licence.provenance.mapper.version == MAPPER_VERSION

    def test_provenance_threads_source_record_id(self) -> None:
        licence = _map(source_record_id="100099")

        assert licence.provenance.source_record_id == "100099"


class TestUnmappedSourceFields:
    def test_excludes_adapter_consumed_fields(self) -> None:
        _, unmapped = _map_with_report()

        assert "getbusid" not in unmapped

    def test_known_unmapped_set_pinned(self) -> None:
        _, unmapped = _map_with_report()

        assert set(unmapped) == {"comdistcd", "globalid", "homeoccind"}
        assert list(unmapped) == sorted(unmapped)


class TestAdapterConsumedFieldsSchema:
    def test_schema_pins_adapter_consumed_fields(self) -> None:
        assert "getbusid" in ADAPTER_CONSUMED_FIELDS
