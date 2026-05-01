from datetime import UTC, date, datetime
from typing import Any

import pytest
from pydantic import ValidationError

from civix.core.identity.models.identifiers import (
    DatasetId,
    Jurisdiction,
    MapperId,
    SnapshotId,
    SourceId,
)
from civix.core.provenance.models.provenance import MapperVersion, ProvenanceRef
from civix.core.quality.models.fields import FieldQuality, MappedField
from civix.core.spatial.models.location import Address, Coordinate
from civix.core.taxonomy.models.category import CategoryRef
from civix.domains.transportation_safety.models.collision import (
    CollisionSeverity,
    TrafficCollision,
)
from civix.domains.transportation_safety.models.parties import (
    ContributingFactor,
    RoadUserRole,
)
from civix.domains.transportation_safety.models.person import (
    CollisionPerson,
    InjuryOutcome,
)
from civix.domains.transportation_safety.models.road import (
    SpeedLimit,
    SpeedLimitUnit,
)
from civix.domains.transportation_safety.models.time import (
    OccurrenceTime,
    OccurrenceTimePrecision,
    OccurrenceTimezoneStatus,
)
from civix.domains.transportation_safety.models.vehicle import (
    CollisionVehicle,
    VehicleCategory,
)


def _mapped[T](
    value: T,
    *source_fields: str,
    quality: FieldQuality = FieldQuality.DIRECT,
) -> MappedField[T]:
    return MappedField[T](value=value, quality=quality, source_fields=source_fields)


def _provenance(source_record_id: str = "collision-1") -> ProvenanceRef:
    return ProvenanceRef(
        snapshot_id=SnapshotId("snap-transportation-1"),
        source_id=SourceId("transportation-safety-test-source"),
        dataset_id=DatasetId("transportation-safety-collisions"),
        jurisdiction=Jurisdiction(country="US", region="IL", locality="Chicago"),
        fetched_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
        mapper=MapperVersion(
            mapper_id=MapperId("transportation-safety-test-mapper"),
            version="0.1.0",
        ),
        source_record_id=source_record_id,
    )


def _category(code: str = "clear") -> CategoryRef:
    return CategoryRef(
        code=code,
        label=code.replace("-", " ").title(),
        taxonomy_id="civix.transportation-safety.test",
        taxonomy_version="2026-05-01",
    )


def _factor(rank: int | None = 1) -> ContributingFactor:
    return ContributingFactor(
        raw_label="Driver failed to yield",
        rank=rank,
        category=_category("failed-to-yield"),
    )


def _occurred() -> OccurrenceTime:
    return OccurrenceTime(
        precision=OccurrenceTimePrecision.DATETIME,
        datetime_value=datetime(2026, 4, 1, 8, 30),
        timezone_status=OccurrenceTimezoneStatus.NAMED_LOCAL,
        timezone="America/Chicago",
    )


def _collision(**overrides: Any) -> TrafficCollision:
    defaults: dict[str, Any] = {
        "provenance": _provenance(),
        "collision_id": "crash-1",
        "occurred_at": _mapped(_occurred(), "occurred_at"),
        "severity": _mapped(
            CollisionSeverity.SERIOUS_INJURY,
            "severity",
            quality=FieldQuality.STANDARDIZED,
        ),
        "address": _mapped(
            Address(country="US", region="IL", locality="Chicago", street="1 N State St"),
            "street_address",
            quality=FieldQuality.DERIVED,
        ),
        "coordinate": _mapped(Coordinate(latitude=41.8837, longitude=-87.6277), "location"),
        "locality": _mapped("Chicago", "locality"),
        "road_names": _mapped(("State St", "Madison St"), "road_names"),
        "intersection_related": _mapped(True, "intersection"),
        "location_description": _mapped("State St at Madison St", "location_description"),
        "weather": _mapped(_category("clear"), "weather", quality=FieldQuality.STANDARDIZED),
        "lighting": _mapped(_category("daylight"), "lighting", quality=FieldQuality.STANDARDIZED),
        "road_surface": _mapped(
            _category("dry"), "road_surface", quality=FieldQuality.STANDARDIZED
        ),
        "road_condition": _mapped(
            _category("no-defects"),
            "road_condition",
            quality=FieldQuality.STANDARDIZED,
        ),
        "traffic_control": _mapped(
            _category("traffic-signal"),
            "traffic_control",
            quality=FieldQuality.STANDARDIZED,
        ),
        "speed_limit": _mapped(
            SpeedLimit(value=30, unit=SpeedLimitUnit.MILES_PER_HOUR),
            "speed_limit",
        ),
        "fatal_count": _mapped(0, "fatal_count"),
        "serious_injury_count": _mapped(1, "serious_injury_count"),
        "minor_injury_count": _mapped(0, "minor_injury_count"),
        "possible_injury_count": _mapped(0, "possible_injury_count"),
        "uninjured_count": _mapped(2, "uninjured_count"),
        "unknown_injury_count": _mapped(0, "unknown_injury_count"),
        "total_injured_count": _mapped(1, "total_injured_count"),
        "vehicle_count": _mapped(2, "vehicle_count"),
        "person_count": _mapped(3, "person_count"),
        "contributing_factors": _mapped((_factor(),), "contributing_factors"),
    }
    defaults.update(overrides)

    return TrafficCollision(**defaults)


def _vehicle(**overrides: Any) -> CollisionVehicle:
    defaults: dict[str, Any] = {
        "provenance": _provenance("vehicle-1"),
        "collision_id": "crash-1",
        "vehicle_id": "vehicle-1",
        "category": _mapped(
            VehicleCategory.PASSENGER_CAR,
            "vehicle_category",
            quality=FieldQuality.STANDARDIZED,
        ),
        "road_user_role": _mapped(
            RoadUserRole.DRIVER,
            "road_user_role",
            quality=FieldQuality.STANDARDIZED,
        ),
        "occupant_count": _mapped(1, "occupant_count"),
        "travel_direction": _mapped(
            _category("north"), "travel_direction", quality=FieldQuality.STANDARDIZED
        ),
        "maneuver": _mapped(
            _category("turning-left"), "maneuver", quality=FieldQuality.STANDARDIZED
        ),
        "damage": _mapped(_category("front"), "damage", quality=FieldQuality.STANDARDIZED),
        "contributing_factors": _mapped((_factor(),), "contributing_factors"),
    }
    defaults.update(overrides)

    return CollisionVehicle(**defaults)


def _person(**overrides: Any) -> CollisionPerson:
    defaults: dict[str, Any] = {
        "provenance": _provenance("person-1"),
        "collision_id": "crash-1",
        "person_id": "person-1",
        "vehicle_id": "vehicle-1",
        "role": _mapped(RoadUserRole.DRIVER, "role", quality=FieldQuality.STANDARDIZED),
        "injury_outcome": _mapped(
            InjuryOutcome.SERIOUS,
            "injury",
            quality=FieldQuality.STANDARDIZED,
        ),
        "age": _mapped(34, "age"),
        "safety_equipment": _mapped(
            _category("seatbelt-used"),
            "safety_equipment",
            quality=FieldQuality.STANDARDIZED,
        ),
        "position_in_vehicle": _mapped(
            _category("driver"), "position", quality=FieldQuality.STANDARDIZED
        ),
        "ejection": _mapped(
            _category("not-ejected"), "ejection", quality=FieldQuality.STANDARDIZED
        ),
        "contributing_factors": _mapped((_factor(),), "contributing_factors"),
    }
    defaults.update(overrides)

    return CollisionPerson(**defaults)


class TestOccurrenceTime:
    def test_date_hour_precision(self) -> None:
        occurred = OccurrenceTime(
            precision=OccurrenceTimePrecision.DATE_HOUR,
            date_value=date(2026, 4, 1),
            hour_value=8,
            timezone_status=OccurrenceTimezoneStatus.NAMED_LOCAL,
            timezone="America/Toronto",
        )

        assert occurred.precision is OccurrenceTimePrecision.DATE_HOUR
        assert occurred.hour_value == 8

    def test_year_precision(self) -> None:
        occurred = OccurrenceTime(
            precision=OccurrenceTimePrecision.YEAR,
            year_value=2024,
            timezone_status=OccurrenceTimezoneStatus.UNKNOWN,
        )

        assert occurred.year_value == 2024

    def test_date_precision_rejects_hour(self) -> None:
        with pytest.raises(ValidationError, match="date precision"):
            OccurrenceTime(
                precision=OccurrenceTimePrecision.DATE,
                date_value=date(2026, 4, 1),
                hour_value=8,
            )

    def test_named_timezone_requires_timezone_metadata(self) -> None:
        with pytest.raises(ValidationError, match="timezone metadata"):
            OccurrenceTime(
                precision=OccurrenceTimePrecision.DATE,
                date_value=date(2026, 4, 1),
                timezone_status=OccurrenceTimezoneStatus.NAMED_LOCAL,
            )

    def test_utc_timezone_requires_zero_offset(self) -> None:
        occurred = OccurrenceTime(
            precision=OccurrenceTimePrecision.DATETIME,
            datetime_value=datetime(2026, 4, 1, 12, 0, tzinfo=UTC),
            timezone_status=OccurrenceTimezoneStatus.UTC,
        )

        assert occurred.datetime_value == datetime(2026, 4, 1, 12, 0, tzinfo=UTC)

    def test_timezone_aware_datetime_cannot_be_local_unspecified(self) -> None:
        with pytest.raises(ValidationError, match="local_unspecified"):
            OccurrenceTime(
                precision=OccurrenceTimePrecision.DATETIME,
                datetime_value=datetime(2026, 4, 1, 12, 0, tzinfo=UTC),
                timezone_status=OccurrenceTimezoneStatus.LOCAL_UNSPECIFIED,
            )


class TestTrafficCollision:
    def test_full_record(self) -> None:
        collision = _collision()

        assert collision.collision_id == "crash-1"
        assert collision.severity.value is CollisionSeverity.SERIOUS_INJURY
        assert collision.serious_injury_count.value == 1
        assert collision.total_injured_count.value == 1
        assert collision.person_count.value == 3

    def test_missing_and_unmapped_fields_preserve_quality(self) -> None:
        collision = _collision(
            address=MappedField[Address](
                value=None,
                quality=FieldQuality.REDACTED,
                source_fields=("street_address",),
            ),
            road_condition=MappedField[CategoryRef](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            speed_limit=MappedField[SpeedLimit](
                value=None,
                quality=FieldQuality.NOT_PROVIDED,
                source_fields=("speed_limit",),
            ),
        )

        assert collision.address.quality is FieldQuality.REDACTED
        assert collision.road_condition.quality is FieldQuality.UNMAPPED
        assert collision.speed_limit.quality is FieldQuality.NOT_PROVIDED

    def test_frozen(self) -> None:
        collision = _collision()

        with pytest.raises(ValidationError):
            collision.locality = _mapped("Other", "locality")  # type: ignore[misc]

    def test_extra_fields_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _collision(unexpected="nope")

    def test_empty_identifier_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _collision(collision_id="")

    def test_invalid_coordinate_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _collision(coordinate=_mapped(Coordinate(latitude=91.0, longitude=0.0), "location"))


class TestCollisionVehicle:
    def test_full_record(self) -> None:
        vehicle = _vehicle()

        assert vehicle.vehicle_id == "vehicle-1"
        assert vehicle.category.value is VehicleCategory.PASSENGER_CAR
        assert vehicle.contributing_factors.value == (_factor(),)

    def test_source_unit_can_represent_pedestrian_unit(self) -> None:
        vehicle = _vehicle(
            category=_mapped(
                VehicleCategory.PEDESTRIAN_UNIT,
                "unit_category",
                quality=FieldQuality.STANDARDIZED,
            ),
            road_user_role=_mapped(
                RoadUserRole.PEDESTRIAN,
                "unit_role",
                quality=FieldQuality.STANDARDIZED,
            ),
            occupant_count=_mapped(1, "unit_count"),
        )

        assert vehicle.category.value is VehicleCategory.PEDESTRIAN_UNIT
        assert vehicle.road_user_role.value is RoadUserRole.PEDESTRIAN

    def test_negative_occupant_count_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _vehicle(occupant_count=_mapped(-1, "occupant_count"))


class TestCollisionPerson:
    def test_full_record(self) -> None:
        person = _person()

        assert person.person_id == "person-1"
        assert person.role.value is RoadUserRole.DRIVER
        assert person.injury_outcome.value is InjuryOutcome.SERIOUS

    def test_vehicle_id_can_be_absent(self) -> None:
        person = _person(vehicle_id=None)

        assert person.vehicle_id is None

    def test_invalid_role_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _person(role=_mapped("driver-ish", "role"))


class TestContributingFactor:
    def test_ordered_factor(self) -> None:
        factor = ContributingFactor(
            raw_label="Unsafe speed",
            rank=2,
            category=_category("unsafe-speed"),
        )

        assert factor.rank == 2

    def test_raw_label_required(self) -> None:
        with pytest.raises(ValidationError):
            ContributingFactor(raw_label="")


class TestSourceDesignShapes:
    def test_chicago_shape(self) -> None:
        collision = _collision(collision_id="chicago-crash")
        vehicle = _vehicle(
            collision_id="chicago-crash",
            vehicle_id="unit-1",
            category=_mapped(
                VehicleCategory.BICYCLE, "UNIT_TYPE", quality=FieldQuality.STANDARDIZED
            ),
        )
        person = _person(
            collision_id="chicago-crash",
            person_id="person-1",
            vehicle_id="unit-1",
        )

        assert collision.collision_id == vehicle.collision_id == person.collision_id

    def test_toronto_ksi_person_grain_shape(self) -> None:
        collision = _collision(
            collision_id="toronto-ksi-1",
            severity=_mapped(CollisionSeverity.SERIOUS_INJURY, "acclass"),
        )
        person = _person(
            collision_id="toronto-ksi-1",
            person_id="person-4",
            vehicle_id=None,
            role=_mapped(RoadUserRole.PASSENGER, "road_user", quality=FieldQuality.STANDARDIZED),
        )

        assert collision.severity.value is CollisionSeverity.SERIOUS_INJURY
        assert person.vehicle_id is None

    def test_stats19_shape(self) -> None:
        collision = _collision(
            collision_id="stats19-accident-1",
            severity=_mapped(CollisionSeverity.MINOR_INJURY, "accident_severity"),
        )
        vehicle = _vehicle(
            collision_id="stats19-accident-1",
            vehicle_id="vehicle-1",
        )
        casualty = _person(
            collision_id="stats19-accident-1",
            person_id="casualty-1",
            vehicle_id="vehicle-1",
            injury_outcome=_mapped(InjuryOutcome.MINOR, "casualty_severity"),
        )

        assert collision.collision_id == casualty.collision_id
        assert vehicle.vehicle_id == casualty.vehicle_id

    def test_france_baac_shape(self) -> None:
        collision = _collision(
            collision_id="baac-accident-1",
            location_description=_mapped("agglomeration road", "lieu"),
        )
        vehicle = _vehicle(
            collision_id="baac-accident-1",
            vehicle_id="A01",
            category=_mapped(VehicleCategory.MICROMOBILITY, "catv"),
        )
        user = _person(
            collision_id="baac-accident-1",
            person_id="user-1",
            vehicle_id="A01",
            role=_mapped(RoadUserRole.CYCLIST, "catu"),
        )

        assert collision.collision_id == vehicle.collision_id == user.collision_id

    def test_domain_fields_do_not_use_source_names(self) -> None:
        source_names = {
            "CRASH_RECORD_ID",
            "COLLISION_ID",
            "Num_Acc",
            "accident_index",
            "veh_no",
            "per_no",
        }
        domain_fields = (
            set(TrafficCollision.model_fields)
            | set(CollisionVehicle.model_fields)
            | set(CollisionPerson.model_fields)
        )

        assert source_names.isdisjoint(domain_fields)
