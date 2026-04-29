"""Unit tests for TorontoBusinessLicencesMapper.

Tests go through the public mapper interface only — the per-field
helpers are private to the module. Each test overrides the relevant
field(s) on a complete baseline raw row and asserts on the resulting
`BusinessLicence`.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

from civix.core.identity import (
    DatasetId,
    Jurisdiction,
    SnapshotId,
    SourceId,
)
from civix.core.quality import FieldQuality
from civix.core.snapshots import RawRecord, SourceSnapshot
from civix.core.spatial import Address
from civix.domains.business_licences import BusinessLicence, LicenceStatus
from civix.infra.sources.ca.toronto_business_licences import (
    MAPPER_ID,
    MAPPER_VERSION,
    TorontoBusinessLicencesMapper,
)
from civix.infra.sources.ca.toronto_business_licences.schema import (
    ADAPTER_CONSUMED_FIELDS,
)

PINNED_NOW = datetime(2026, 4, 28, 12, 0, tzinfo=UTC)


def _snapshot() -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId("snap-1"),
        source_id=SourceId("toronto-open-data"),
        dataset_id=DatasetId("municipal-licensing-and-standards-business-licences-and-permits"),
        jurisdiction=Jurisdiction(country="CA", region="ON", locality="Toronto"),
        fetched_at=PINNED_NOW,
        record_count=1,
    )


def _raw(**fields: Any) -> dict[str, Any]:
    """A complete raw row with every Toronto field, overrideable per test."""
    base: dict[str, Any] = {
        "_id": 1,
        "Category": "HOLISTIC CENTRE",
        "Licence No.": "B66-1234567",
        "Operating Name": "ZEN STUDIO",
        "Issued": "2024-05-06",
        "Client Name": "ZEN STUDIO INC",
        "Business Phone": None,
        "Business Phone Ext.": None,
        "Licence Address Line 1": "100 KING ST W, SUITE 200",
        "Licence Address Line 2": "TORONTO, ON",
        "Licence Address Line 3": "M5X 1A9",
        "Ward": 10,
        "Conditions": "HOLISTIC PRACTITIONER;",
        "Free Form Conditions Line 1": None,
        "Free Form Conditions Line 2": None,
        "Plate No.": None,
        "Endorsements": "HOLISTIC PRACTITIONER;",
        "Cancel Date": None,
        "Last Record Update": "2024-05-06",
    }
    base.update(fields)

    return base


def _map(*, source_record_id: str | None = "TOR-1", **raw_overrides: Any) -> BusinessLicence:
    """Run the mapper against a raw row built from defaults + overrides."""
    snap = _snapshot()
    raw = RawRecord(
        snapshot_id=snap.snapshot_id,
        raw_data=_raw(**raw_overrides),
        source_record_id=source_record_id,
    )

    return TorontoBusinessLicencesMapper()(raw, snap).record


def _map_with_report(**raw_overrides: Any) -> tuple[BusinessLicence, tuple[str, ...]]:
    snap = _snapshot()
    raw = RawRecord(snapshot_id=snap.snapshot_id, raw_data=_raw(**raw_overrides))
    result = TorontoBusinessLicencesMapper()(raw, snap)

    return result.record, result.report.unmapped_source_fields


class TestBusinessName:
    def test_typical(self) -> None:
        licence = _map(**{"Operating Name": "Joe's Cafe"})

        assert licence.business_name.value == "Joe's Cafe"
        assert licence.business_name.quality is FieldQuality.DIRECT

    def test_null(self) -> None:
        licence = _map(**{"Operating Name": None})

        assert licence.business_name.value is None
        assert licence.business_name.quality is FieldQuality.NOT_PROVIDED

    def test_empty_string(self) -> None:
        licence = _map(**{"Operating Name": ""})

        assert licence.business_name.value is None
        assert licence.business_name.quality is FieldQuality.NOT_PROVIDED

    def test_whitespace_only_treated_as_missing(self) -> None:
        licence = _map(**{"Operating Name": "   "})

        assert licence.business_name.value is None
        assert licence.business_name.quality is FieldQuality.NOT_PROVIDED


class TestLicenceNumber:
    def test_typical(self) -> None:
        licence = _map(**{"Licence No.": "B02-4741962"})

        assert licence.licence_number.value == "B02-4741962"
        assert licence.licence_number.quality is FieldQuality.DIRECT

    def test_missing(self) -> None:
        licence = _map(**{"Licence No.": None})

        assert licence.licence_number.value is None
        assert licence.licence_number.quality is FieldQuality.NOT_PROVIDED


class TestStatus:
    def test_cancel_date_present_yields_cancelled_derived(self) -> None:
        licence = _map(**{"Cancel Date": "2018-12-07"})

        assert licence.status.value is LicenceStatus.CANCELLED
        assert licence.status.quality is FieldQuality.DERIVED
        assert licence.status.source_fields == ("Cancel Date",)

    def test_cancel_date_null_yields_not_provided(self) -> None:
        # Toronto's source has no positive "active" signal — absence of a
        # cancel date is not the same as confirmed-active.
        licence = _map(**{"Cancel Date": None})

        assert licence.status.value is None
        assert licence.status.quality is FieldQuality.NOT_PROVIDED
        assert licence.status.source_fields == ("Cancel Date",)

    def test_cancel_date_empty_string_yields_not_provided(self) -> None:
        licence = _map(**{"Cancel Date": ""})

        assert licence.status.value is None
        assert licence.status.quality is FieldQuality.NOT_PROVIDED


class TestCategory:
    def test_typical(self) -> None:
        licence = _map(**{"Category": "HOLISTIC CENTRE"})

        assert licence.category.value is not None
        assert licence.category.value.code == "holistic-centre"
        assert licence.category.value.label == "HOLISTIC CENTRE"
        assert licence.category.value.taxonomy_id == "toronto-business-categories"
        assert licence.category.quality is FieldQuality.DERIVED
        assert licence.category.source_fields == ("Category",)

    def test_missing(self) -> None:
        licence = _map(**{"Category": None})

        assert licence.category.value is None
        assert licence.category.quality is FieldQuality.NOT_PROVIDED

    def test_empty_string(self) -> None:
        licence = _map(**{"Category": ""})

        assert licence.category.value is None
        assert licence.category.quality is FieldQuality.NOT_PROVIDED

    def test_messy_label_slugified(self) -> None:
        licence = _map(**{"Category": "DRIVING SCHOOL OPERATOR (B)"})

        assert licence.category.value is not None
        assert licence.category.value.code == "driving-school-operator-b"


class TestIssuedAt:
    def test_iso_date(self) -> None:
        licence = _map(**{"Issued": "2018-01-18"})

        assert licence.issued_at.value == date(2018, 1, 18)
        assert licence.issued_at.quality is FieldQuality.STANDARDIZED

    def test_missing(self) -> None:
        licence = _map(**{"Issued": None})

        assert licence.issued_at.value is None
        assert licence.issued_at.quality is FieldQuality.NOT_PROVIDED

    def test_empty_string(self) -> None:
        licence = _map(**{"Issued": ""})

        assert licence.issued_at.value is None
        assert licence.issued_at.quality is FieldQuality.NOT_PROVIDED

    def test_unparseable(self) -> None:
        licence = _map(**{"Issued": "yesterday"})

        assert licence.issued_at.value is None
        assert licence.issued_at.quality is FieldQuality.NOT_PROVIDED


class TestExpiresAt:
    def test_unmapped_because_source_has_no_expiry_field(self) -> None:
        licence = _map()

        assert licence.expires_at.value is None
        assert licence.expires_at.quality is FieldQuality.UNMAPPED
        assert licence.expires_at.source_fields == ()


class TestAddress:
    def test_full_address(self) -> None:
        licence = _map()

        assert licence.address.value == Address(
            country="CA",
            region="ON",
            locality="TORONTO",
            street="100 KING ST W, SUITE 200",
            postal_code="M5X 1A9",
        )
        assert licence.address.quality is FieldQuality.DERIVED

    def test_locality_outside_toronto(self) -> None:
        licence = _map(
            **{
                "Licence Address Line 2": "MISSISSAUGA, ON",
            }
        )

        assert licence.address.value is not None
        assert licence.address.value.locality == "MISSISSAUGA"
        assert licence.address.value.region == "ON"

    def test_line2_without_comma_drops_locality_and_region(self) -> None:
        licence = _map(**{"Licence Address Line 2": "TORONTO"})

        assert licence.address.value is not None
        assert licence.address.value.locality is None
        assert licence.address.value.region is None
        assert licence.address.value.country == "CA"

    def test_line2_missing(self) -> None:
        licence = _map(**{"Licence Address Line 2": None})

        assert licence.address.value is not None
        assert licence.address.value.locality is None
        assert licence.address.value.region is None

    def test_line1_missing(self) -> None:
        licence = _map(**{"Licence Address Line 1": None})

        assert licence.address.value is not None
        assert licence.address.value.street is None
        assert licence.address.value.country == "CA"

    def test_postal_missing(self) -> None:
        licence = _map(**{"Licence Address Line 3": None})

        assert licence.address.value is not None
        assert licence.address.value.postal_code is None

    def test_all_lines_missing_still_yields_country_only(self) -> None:
        # Country is inferred, not sourced — there is always enough to
        # produce a degenerate Address(country="CA").
        licence = _map(
            **{
                "Licence Address Line 1": None,
                "Licence Address Line 2": None,
                "Licence Address Line 3": None,
            }
        )

        assert licence.address.value == Address(country="CA")
        assert licence.address.quality is FieldQuality.DERIVED


class TestCoordinate:
    def test_unmapped_because_source_has_no_coordinate(self) -> None:
        licence = _map()

        assert licence.coordinate.value is None
        assert licence.coordinate.quality is FieldQuality.UNMAPPED
        assert licence.coordinate.source_fields == ()


class TestNeighbourhood:
    def test_unmapped_because_source_has_no_neighbourhood(self) -> None:
        # Ward is administrative, not a neighbourhood, so it is not
        # promoted into the neighbourhood field.
        licence = _map()

        assert licence.neighbourhood.value is None
        assert licence.neighbourhood.quality is FieldQuality.UNMAPPED
        assert licence.neighbourhood.source_fields == ()


class TestProvenance:
    def test_provenance_carries_mapper_version(self) -> None:
        licence = _map()

        assert licence.provenance.mapper.mapper_id == MAPPER_ID
        assert licence.provenance.mapper.version == MAPPER_VERSION

    def test_provenance_carries_snapshot_id(self) -> None:
        licence = _map()

        assert licence.provenance.snapshot_id == _snapshot().snapshot_id

    def test_provenance_threads_source_record_id(self) -> None:
        licence = _map(source_record_id="TOR-42")

        assert licence.provenance.source_record_id == "TOR-42"


class TestUnmappedSourceFields:
    def test_excludes_adapter_consumed_fields(self) -> None:
        _, unmapped = _map_with_report()

        assert "Licence No." not in unmapped

    def test_excludes_mapper_consumed_fields(self) -> None:
        _, unmapped = _map_with_report()

        for consumed in (
            "Operating Name",
            "Category",
            "Issued",
            "Cancel Date",
            "Licence Address Line 1",
            "Licence Address Line 2",
            "Licence Address Line 3",
        ):
            assert consumed not in unmapped, f"expected {consumed} not in unmapped"

    def test_unmapped_list_is_sorted(self) -> None:
        _, unmapped = _map_with_report()

        assert list(unmapped) == sorted(unmapped)

    def test_known_unmapped_set_pinned(self) -> None:
        # Pinned so a new source field fails loudly here rather than
        # slipping silently into unmapped. Forces a maintainer decision.
        _, unmapped = _map_with_report()

        assert set(unmapped) == {
            "_id",
            "Business Phone",
            "Business Phone Ext.",
            "Client Name",
            "Conditions",
            "Endorsements",
            "Free Form Conditions Line 1",
            "Free Form Conditions Line 2",
            "Last Record Update",
            "Plate No.",
            "Ward",
        }


class TestAdapterConsumedFieldsSchema:
    def test_schema_pins_adapter_consumed_fields(self) -> None:
        # Pinned because the mapper's unmapped_source_fields builder
        # depends on this set; removal here would silently miscount.
        assert "Licence No." in ADAPTER_CONSUMED_FIELDS
