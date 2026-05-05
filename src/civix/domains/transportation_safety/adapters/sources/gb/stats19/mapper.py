"""Great Britain STATS19 mappers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Final

from pydantic import ValidationError

from civix.core.identity.models.identifiers import MapperId
from civix.core.mapping.errors import MappingError
from civix.core.mapping.models.mapper import MappingReport, MapResult
from civix.core.mapping.parsers import float_or_none, int_or_none, require_text, str_or_none
from civix.core.provenance.models.provenance import MapperVersion, ProvenanceRef
from civix.core.quality.models.fields import FieldQuality, MappedField
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.spatial.models.location import Address, Coordinate
from civix.core.taxonomy.models.category import CategoryRef
from civix.domains.transportation_safety.adapters.sources.gb.stats19.adapter import (
    STATS19_RELEASE,
)
from civix.domains.transportation_safety.models.collision import (
    CollisionSeverity,
    TrafficCollision,
)
from civix.domains.transportation_safety.models.parties import (
    ContributingFactor,
    RoadUserRole,
)
from civix.domains.transportation_safety.models.person import CollisionPerson, InjuryOutcome
from civix.domains.transportation_safety.models.road import SpeedLimit, SpeedLimitUnit
from civix.domains.transportation_safety.models.time import (
    OccurrenceTime,
    OccurrenceTimePrecision,
    OccurrenceTimezoneStatus,
)
from civix.domains.transportation_safety.models.vehicle import (
    CollisionVehicle,
    VehicleCategory,
)

COLLISION_MAPPER_ID: Final[MapperId] = MapperId("stats19-collisions")
VEHICLE_MAPPER_ID: Final[MapperId] = MapperId("stats19-vehicles")
CASUALTY_MAPPER_ID: Final[MapperId] = MapperId("stats19-casualties")
MAPPER_VERSION: Final[str] = "0.1.0"

_TIMEZONE: Final[str] = "Europe/London"
_CONTRIBUTING_FACTORS_SOURCE_FIELD: Final[str] = "dft_sensitive_data.contributory_factors"
_SAFETY_EQUIPMENT_SOURCE_FIELD: Final[str] = "dft_open_data.safety_equipment"
_EJECTION_SOURCE_FIELD: Final[str] = "dft_open_data.ejection"
_ADDRESS_SOURCE_FIELD: Final[str] = "dft_open_data.address"
_LOCALITY_SOURCE_FIELD: Final[str] = "dft_open_data.locality"

_COLLISION_CONSUMED_FIELDS: Final[frozenset[str]] = frozenset({"accident_index"})
_VEHICLE_CONSUMED_FIELDS: Final[frozenset[str]] = frozenset({"accident_index", "vehicle_reference"})
_CASUALTY_CONSUMED_FIELDS: Final[frozenset[str]] = frozenset(
    {"accident_index", "vehicle_reference", "casualty_reference"}
)

_SEVERITY_MAP: Final[dict[str, CollisionSeverity]] = {
    "1": CollisionSeverity.FATAL,
    "2": CollisionSeverity.SERIOUS_INJURY,
    "3": CollisionSeverity.MINOR_INJURY,
}
_INJURY_MAP: Final[dict[str, InjuryOutcome]] = {
    "1": InjuryOutcome.FATAL,
    "2": InjuryOutcome.SERIOUS,
    "3": InjuryOutcome.MINOR,
}
_CASUALTY_CLASS_ROLE_MAP: Final[dict[str, RoadUserRole]] = {
    "2": RoadUserRole.PASSENGER,
    "3": RoadUserRole.PEDESTRIAN,
}
_VEHICLE_TYPE_CATEGORY_MAP: Final[dict[str, VehicleCategory]] = {
    "1": VehicleCategory.BICYCLE,
    "2": VehicleCategory.MOTORCYCLE,
    "3": VehicleCategory.MOTORCYCLE,
    "4": VehicleCategory.MOTORCYCLE,
    "5": VehicleCategory.MOTORCYCLE,
    "8": VehicleCategory.PASSENGER_CAR,
    "9": VehicleCategory.PASSENGER_CAR,
    "10": VehicleCategory.BUS,
    "11": VehicleCategory.BUS,
    "19": VehicleCategory.OTHER,
    "90": VehicleCategory.OTHER,
}
_VEHICLE_TYPE_ROLE_MAP: Final[dict[str, RoadUserRole]] = {
    "1": RoadUserRole.CYCLIST,
    "2": RoadUserRole.MOTORCYCLIST,
    "3": RoadUserRole.MOTORCYCLIST,
    "4": RoadUserRole.MOTORCYCLIST,
    "5": RoadUserRole.MOTORCYCLIST,
}
_NO_VEHICLE_REFERENCES: Final[frozenset[str]] = frozenset({"-1"})
_NO_DATA_CODES_BY_FIELD: Final[dict[str, frozenset[str]]] = {
    "junction_control": frozenset({"9"}),
    "junction_detail": frozenset({"99"}),
    "light_conditions": frozenset({"9"}),
    "road_surface_conditions": frozenset({"9"}),
    "weather_conditions": frozenset({"9"}),
}

_WEATHER_LABELS: Final[dict[str, str]] = {
    "1": "Fine no high winds",
    "2": "Raining no high winds",
    "3": "Snowing no high winds",
    "4": "Fine high winds",
    "5": "Raining high winds",
    "6": "Snowing high winds",
    "7": "Fog or mist",
    "8": "Other",
    "9": "Unknown",
}
_LIGHT_LABELS: Final[dict[str, str]] = {
    "1": "Daylight",
    "4": "Darkness - lights lit",
    "5": "Darkness - lights unlit",
    "6": "Darkness - no lighting",
    "7": "Darkness - lighting unknown",
    "9": "Unknown",
}
_ROAD_SURFACE_LABELS: Final[dict[str, str]] = {
    "1": "Dry",
    "2": "Wet or damp",
    "3": "Snow",
    "4": "Frost or ice",
    "5": "Flood over 3cm deep",
    "6": "Oil or diesel",
    "7": "Mud",
    "9": "Unknown",
}
_JUNCTION_DETAIL_LABELS: Final[dict[str, str]] = {
    "0": "Not at junction or within 20 metres",
    "1": "Roundabout",
    "2": "Mini-roundabout",
    "3": "T or staggered junction",
    "5": "Slip road",
    "6": "Crossroads",
    "7": "More than 4 arms",
    "8": "Private drive or entrance",
    "9": "Other junction",
    "99": "Unknown",
}
_JUNCTION_CONTROL_LABELS: Final[dict[str, str]] = {
    "0": "Not at junction or within 20 metres",
    "1": "Authorised person",
    "2": "Automatic traffic signal",
    "3": "Stop sign",
    "4": "Give way or uncontrolled",
    "9": "Unknown",
}
_MANOEUVRE_LABELS: Final[dict[str, str]] = {
    "1": "Reversing",
    "2": "Parked",
    "3": "Waiting to go",
    "4": "Slowing or stopping",
    "5": "Moving off",
    "7": "U-turn",
    "9": "Turning left",
    "10": "Waiting to turn left",
    "13": "Overtaking moving vehicle",
    "16": "Going ahead left-hand bend",
    "18": "Going ahead other",
}
_CASUALTY_CLASS_LABELS: Final[dict[str, str]] = {
    "1": "Driver or rider",
    "2": "Passenger",
    "3": "Pedestrian",
}


@dataclass(frozen=True, slots=True)
class Stats19CollisionMapper:
    """Maps STATS19 collision rows to `TrafficCollision`."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=COLLISION_MAPPER_ID, version=MAPPER_VERSION)

    def __call__(self, record: RawRecord, snapshot: SourceSnapshot) -> MapResult[TrafficCollision]:
        raw = record.raw_data
        collision = TrafficCollision(
            provenance=_build_provenance(record=record, snapshot=snapshot, mapper=self.version),
            collision_id=require_text(
                raw.get("accident_index"),
                field_name="accident_index",
                mapper=self.version,
                source_record_id=record.source_record_id,
            ),
            occurred_at=_map_occurred_at(raw),
            severity=_map_collision_severity(raw),
            address=MappedField[Address](
                value=None,
                quality=FieldQuality.NOT_PROVIDED,
                source_fields=(_ADDRESS_SOURCE_FIELD,),
            ),
            coordinate=_map_coordinate(raw),
            locality=MappedField[str](
                value=None,
                quality=FieldQuality.NOT_PROVIDED,
                source_fields=(_LOCALITY_SOURCE_FIELD,),
            ),
            road_names=MappedField[tuple[str, ...]](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            intersection_related=_map_intersection_related(raw),
            location_description=MappedField[str](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            weather=_map_source_category(
                raw,
                "weather_conditions",
                labels=_WEATHER_LABELS,
                taxonomy_id="stats19-weather-conditions",
            ),
            lighting=_map_source_category(
                raw,
                "light_conditions",
                labels=_LIGHT_LABELS,
                taxonomy_id="stats19-light-conditions",
            ),
            road_surface=_map_source_category(
                raw,
                "road_surface_conditions",
                labels=_ROAD_SURFACE_LABELS,
                taxonomy_id="stats19-road-surface-conditions",
            ),
            road_condition=MappedField[CategoryRef](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            traffic_control=_map_source_category(
                raw,
                "junction_control",
                labels=_JUNCTION_CONTROL_LABELS,
                taxonomy_id="stats19-junction-control",
            ),
            speed_limit=_map_speed_limit(raw),
            fatal_count=MappedField[int](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            serious_injury_count=MappedField[int](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            minor_injury_count=MappedField[int](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            possible_injury_count=MappedField[int](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            uninjured_count=MappedField[int](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            unknown_injury_count=MappedField[int](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            total_injured_count=_map_count(raw, "number_of_casualties"),
            vehicle_count=_map_count(raw, "number_of_vehicles"),
            person_count=_map_count(raw, "number_of_casualties"),
            contributing_factors=MappedField[tuple[ContributingFactor, ...]](
                value=None,
                quality=FieldQuality.NOT_PROVIDED,
                source_fields=(_CONTRIBUTING_FACTORS_SOURCE_FIELD,),
            ),
        )
        report = MappingReport(unmapped_source_fields=_unmapped_source_fields(raw, collision))

        return MapResult[TrafficCollision](record=collision, report=report)


@dataclass(frozen=True, slots=True)
class Stats19VehicleMapper:
    """Maps STATS19 vehicle rows to `CollisionVehicle`."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=VEHICLE_MAPPER_ID, version=MAPPER_VERSION)

    def __call__(self, record: RawRecord, snapshot: SourceSnapshot) -> MapResult[CollisionVehicle]:
        raw = record.raw_data
        accident_index = require_text(
            raw.get("accident_index"),
            field_name="accident_index",
            mapper=self.version,
            source_record_id=record.source_record_id,
        )
        vehicle_reference = _require_positive_reference(
            raw.get("vehicle_reference"),
            field_name="vehicle_reference",
            mapper=self.version,
            source_record_id=record.source_record_id,
        )
        vehicle = CollisionVehicle(
            provenance=_build_provenance(record=record, snapshot=snapshot, mapper=self.version),
            collision_id=accident_index,
            vehicle_id=_vehicle_id(accident_index, vehicle_reference),
            category=_map_vehicle_category(raw),
            road_user_role=_map_vehicle_role(raw),
            occupant_count=MappedField[int](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            travel_direction=_map_travel_direction(raw),
            maneuver=_map_source_category(
                raw,
                "vehicle_manoeuvre",
                labels=_MANOEUVRE_LABELS,
                taxonomy_id="stats19-vehicle-manoeuvre",
            ),
            damage=MappedField[CategoryRef](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            contributing_factors=MappedField[tuple[ContributingFactor, ...]](
                value=None,
                quality=FieldQuality.NOT_PROVIDED,
                source_fields=(_CONTRIBUTING_FACTORS_SOURCE_FIELD,),
            ),
        )
        report = MappingReport(unmapped_source_fields=_unmapped_source_fields(raw, vehicle))

        return MapResult[CollisionVehicle](record=vehicle, report=report)


@dataclass(frozen=True, slots=True)
class Stats19CasualtyMapper:
    """Maps STATS19 casualty rows to `CollisionPerson`."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=CASUALTY_MAPPER_ID, version=MAPPER_VERSION)

    def __call__(self, record: RawRecord, snapshot: SourceSnapshot) -> MapResult[CollisionPerson]:
        raw = record.raw_data
        accident_index = require_text(
            raw.get("accident_index"),
            field_name="accident_index",
            mapper=self.version,
            source_record_id=record.source_record_id,
        )
        casualty_reference = require_text(
            raw.get("casualty_reference"),
            field_name="casualty_reference",
            mapper=self.version,
            source_record_id=record.source_record_id,
        )
        person = CollisionPerson(
            provenance=_build_provenance(record=record, snapshot=snapshot, mapper=self.version),
            collision_id=accident_index,
            person_id=f"{accident_index}:{casualty_reference}",
            vehicle_id=_map_casualty_vehicle_id(raw, mapper=self.version, record=record),
            role=_map_casualty_role(raw),
            injury_outcome=_map_person_injury(raw),
            # `age_band_of_casualty` stays raw until the domain has an age-band slot.
            age=_map_count(raw, "age_of_casualty"),
            safety_equipment=MappedField[CategoryRef](
                value=None,
                quality=FieldQuality.NOT_PROVIDED,
                source_fields=(_SAFETY_EQUIPMENT_SOURCE_FIELD,),
            ),
            position_in_vehicle=_map_source_category(
                raw,
                "casualty_class",
                labels=_CASUALTY_CLASS_LABELS,
                taxonomy_id="stats19-casualty-class",
            ),
            ejection=MappedField[CategoryRef](
                value=None,
                quality=FieldQuality.NOT_PROVIDED,
                source_fields=(_EJECTION_SOURCE_FIELD,),
            ),
            contributing_factors=MappedField[tuple[ContributingFactor, ...]](
                value=None,
                quality=FieldQuality.NOT_PROVIDED,
                source_fields=(_CONTRIBUTING_FACTORS_SOURCE_FIELD,),
            ),
        )
        report = MappingReport(unmapped_source_fields=_unmapped_source_fields(raw, person))

        return MapResult[CollisionPerson](record=person, report=report)


def _map_occurred_at(raw: Mapping[str, Any]) -> MappedField[OccurrenceTime]:
    parsed_date = _parse_date(raw.get("date"))
    parsed_time = str_or_none(raw.get("time"))

    if parsed_date is None:
        return MappedField[OccurrenceTime](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("date",),
        )

    if parsed_time is None:
        return MappedField[OccurrenceTime](
            value=OccurrenceTime(
                precision=OccurrenceTimePrecision.DATE,
                date_value=parsed_date,
                timezone_status=OccurrenceTimezoneStatus.NAMED_LOCAL,
                timezone=_TIMEZONE,
            ),
            quality=FieldQuality.STANDARDIZED,
            source_fields=("date",),
        )

    parsed_datetime = _parse_datetime(parsed_date, parsed_time)

    if parsed_datetime is None:
        return MappedField[OccurrenceTime](
            value=OccurrenceTime(
                precision=OccurrenceTimePrecision.DATE,
                date_value=parsed_date,
                timezone_status=OccurrenceTimezoneStatus.NAMED_LOCAL,
                timezone=_TIMEZONE,
            ),
            quality=FieldQuality.STANDARDIZED,
            source_fields=("date",),
        )

    return MappedField[OccurrenceTime](
        value=OccurrenceTime(
            precision=OccurrenceTimePrecision.DATETIME,
            datetime_value=parsed_datetime,
            timezone_status=OccurrenceTimezoneStatus.NAMED_LOCAL,
            timezone=_TIMEZONE,
        ),
        quality=FieldQuality.STANDARDIZED,
        source_fields=("date", "time"),
    )


def _parse_date(value: object) -> date | None:
    text = str_or_none(value)

    if text is None:
        return None

    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue

    return None


def _parse_datetime(date_value: date, time_value: str) -> datetime | None:
    try:
        hour, minute = (int(part) for part in time_value.split(":", maxsplit=1))
    except ValueError:
        return None

    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None

    return datetime(
        year=date_value.year,
        month=date_value.month,
        day=date_value.day,
        hour=hour,
        minute=minute,
    )


def _map_collision_severity(raw: Mapping[str, Any]) -> MappedField[CollisionSeverity]:
    code = str_or_none(raw.get("accident_severity"))

    if code is None:
        return MappedField[CollisionSeverity](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("accident_severity",),
        )

    severity = _SEVERITY_MAP.get(code)

    if severity is None:
        return MappedField[CollisionSeverity](
            value=CollisionSeverity.UNKNOWN,
            quality=FieldQuality.INFERRED,
            source_fields=("accident_severity",),
        )

    return MappedField[CollisionSeverity](
        value=severity,
        quality=FieldQuality.STANDARDIZED,
        source_fields=("accident_severity",),
    )


def _map_coordinate(raw: Mapping[str, Any]) -> MappedField[Coordinate]:
    latitude = float_or_none(raw.get("latitude"))
    longitude = float_or_none(raw.get("longitude"))

    if latitude is None or longitude is None:
        return MappedField[Coordinate](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("latitude", "longitude"),
        )

    try:
        coordinate = Coordinate(latitude=latitude, longitude=longitude)
    except ValidationError:
        return MappedField[Coordinate](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("latitude", "longitude"),
        )

    return MappedField[Coordinate](
        value=coordinate,
        quality=FieldQuality.STANDARDIZED,
        source_fields=("latitude", "longitude"),
    )


def _map_intersection_related(raw: Mapping[str, Any]) -> MappedField[bool]:
    code = str_or_none(raw.get("junction_detail"))

    if code is None:
        return MappedField[bool](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("junction_detail",),
        )

    if code == "0":
        return MappedField[bool](
            value=False,
            quality=FieldQuality.STANDARDIZED,
            source_fields=("junction_detail",),
        )

    if code in _JUNCTION_DETAIL_LABELS and code not in _no_data_codes("junction_detail"):
        return MappedField[bool](
            value=True,
            quality=FieldQuality.STANDARDIZED,
            source_fields=("junction_detail",),
        )

    if code in _no_data_codes("junction_detail"):
        return MappedField[bool](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("junction_detail",),
        )

    return MappedField[bool](value=None, quality=FieldQuality.UNMAPPED, source_fields=())


def _map_speed_limit(raw: Mapping[str, Any]) -> MappedField[SpeedLimit]:
    value = int_or_none(raw.get("speed_limit"))

    if value is None or value < 0 or value == 99:
        return MappedField[SpeedLimit](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("speed_limit",),
        )

    return MappedField[SpeedLimit](
        value=SpeedLimit(value=value, unit=SpeedLimitUnit.MILES_PER_HOUR),
        quality=FieldQuality.STANDARDIZED,
        source_fields=("speed_limit",),
    )


def _map_count(raw: Mapping[str, Any], field_name: str) -> MappedField[int]:
    value = int_or_none(raw.get(field_name))

    if value is None or value < 0:
        return MappedField[int](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=(field_name,),
        )

    return MappedField[int](
        value=value,
        quality=FieldQuality.STANDARDIZED,
        source_fields=(field_name,),
    )


def _map_vehicle_category(raw: Mapping[str, Any]) -> MappedField[VehicleCategory]:
    code = str_or_none(raw.get("vehicle_type"))

    if code is None:
        return MappedField[VehicleCategory](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("vehicle_type",),
        )

    category = _VEHICLE_TYPE_CATEGORY_MAP.get(code)

    if category is None:
        return MappedField[VehicleCategory](
            value=VehicleCategory.UNKNOWN,
            quality=FieldQuality.INFERRED,
            source_fields=("vehicle_type",),
        )

    return MappedField[VehicleCategory](
        value=category,
        quality=FieldQuality.STANDARDIZED,
        source_fields=("vehicle_type",),
    )


def _map_vehicle_role(raw: Mapping[str, Any]) -> MappedField[RoadUserRole]:
    code = str_or_none(raw.get("vehicle_type"))

    if code is None:
        return MappedField[RoadUserRole](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("vehicle_type",),
        )

    role = _VEHICLE_TYPE_ROLE_MAP.get(code)

    if role is None:
        return MappedField[RoadUserRole](
            value=RoadUserRole.UNKNOWN,
            quality=FieldQuality.INFERRED,
            source_fields=("vehicle_type",),
        )

    return MappedField[RoadUserRole](
        value=role,
        quality=FieldQuality.STANDARDIZED,
        source_fields=("vehicle_type",),
    )


def _map_travel_direction(raw: Mapping[str, Any]) -> MappedField[CategoryRef]:
    to_value = str_or_none(raw.get("vehicle_direction_to"))

    if to_value is None:
        return MappedField[CategoryRef](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("vehicle_direction_to",),
        )

    return MappedField[CategoryRef](
        value=CategoryRef(
            code=to_value,
            label=to_value,
            taxonomy_id="stats19-vehicle-direction-to",
            taxonomy_version=STATS19_RELEASE,
        ),
        quality=FieldQuality.STANDARDIZED,
        source_fields=("vehicle_direction_to",),
    )


def _map_casualty_vehicle_id(
    raw: Mapping[str, Any],
    *,
    mapper: MapperVersion,
    record: RawRecord,
) -> str | None:
    reference = str_or_none(raw.get("vehicle_reference"))

    if reference is None or reference in _NO_VEHICLE_REFERENCES:
        return None

    value = int_or_none(reference)

    if value is None or value <= 0:
        raise MappingError(
            "unsupported STATS19 casualty vehicle_reference",
            mapper=mapper,
            source_record_id=record.source_record_id,
            source_fields=("vehicle_reference",),
        )

    accident_index = require_text(
        raw.get("accident_index"),
        field_name="accident_index",
        mapper=mapper,
        source_record_id=record.source_record_id,
    )

    return _vehicle_id(accident_index, reference)


def _map_casualty_role(raw: Mapping[str, Any]) -> MappedField[RoadUserRole]:
    casualty_class = str_or_none(raw.get("casualty_class"))
    casualty_type = str_or_none(raw.get("casualty_type"))

    if casualty_class is None:
        return MappedField[RoadUserRole](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("casualty_class",),
        )

    if casualty_class == "1":
        role = _VEHICLE_TYPE_ROLE_MAP.get(casualty_type or "", RoadUserRole.DRIVER)
        quality = (
            FieldQuality.INFERRED
            if casualty_type is not None
            and casualty_type not in _VEHICLE_TYPE_ROLE_MAP
            and casualty_type not in _VEHICLE_TYPE_CATEGORY_MAP
            else FieldQuality.STANDARDIZED
        )
    else:
        role = _CASUALTY_CLASS_ROLE_MAP.get(casualty_class)

        if role is None:
            return MappedField[RoadUserRole](
                value=RoadUserRole.UNKNOWN,
                quality=FieldQuality.INFERRED,
                source_fields=("casualty_class", "casualty_type"),
            )

        quality = FieldQuality.STANDARDIZED

    return MappedField[RoadUserRole](
        value=role,
        quality=quality,
        source_fields=("casualty_class", "casualty_type"),
    )


def _map_person_injury(raw: Mapping[str, Any]) -> MappedField[InjuryOutcome]:
    code = str_or_none(raw.get("casualty_severity"))

    if code is None:
        return MappedField[InjuryOutcome](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("casualty_severity",),
        )

    injury = _INJURY_MAP.get(code)

    if injury is None:
        return MappedField[InjuryOutcome](
            value=InjuryOutcome.UNKNOWN,
            quality=FieldQuality.INFERRED,
            source_fields=("casualty_severity",),
        )

    return MappedField[InjuryOutcome](
        value=injury,
        quality=FieldQuality.STANDARDIZED,
        source_fields=("casualty_severity",),
    )


def _map_source_category(
    raw: Mapping[str, Any],
    field_name: str,
    *,
    labels: Mapping[str, str],
    taxonomy_id: str,
) -> MappedField[CategoryRef]:
    code = str_or_none(raw.get(field_name))

    if code is None or code in _no_data_codes(field_name):
        return MappedField[CategoryRef](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=(field_name,),
        )

    label = labels.get(code)

    if label is None:
        return MappedField[CategoryRef](
            value=CategoryRef(
                code=code,
                label=f"Unsupported code {code}",
                taxonomy_id=taxonomy_id,
                taxonomy_version=STATS19_RELEASE,
            ),
            quality=FieldQuality.INFERRED,
            source_fields=(field_name,),
        )

    return MappedField[CategoryRef](
        value=CategoryRef(
            code=code,
            label=label,
            taxonomy_id=taxonomy_id,
            taxonomy_version=STATS19_RELEASE,
        ),
        quality=FieldQuality.STANDARDIZED,
        source_fields=(field_name,),
    )


def _no_data_codes(field_name: str) -> frozenset[str]:
    return _NO_DATA_CODES_BY_FIELD.get(field_name, frozenset())


def _require_positive_reference(
    value: object,
    *,
    field_name: str,
    mapper: MapperVersion,
    source_record_id: str | None,
) -> str:
    reference = require_text(
        value,
        field_name=field_name,
        mapper=mapper,
        source_record_id=source_record_id,
    )
    parsed = int_or_none(reference)

    if parsed is not None and parsed > 0:
        return reference

    raise MappingError(
        f"invalid STATS19 {field_name}",
        mapper=mapper,
        source_record_id=source_record_id,
        source_fields=(field_name,),
    )


def _vehicle_id(accident_index: str, vehicle_reference: str) -> str:
    return f"{accident_index}:{vehicle_reference}"


def _build_provenance(
    *,
    record: RawRecord,
    snapshot: SourceSnapshot,
    mapper: MapperVersion,
) -> ProvenanceRef:
    return ProvenanceRef(
        snapshot_id=snapshot.snapshot_id,
        source_id=snapshot.source_id,
        dataset_id=snapshot.dataset_id,
        jurisdiction=snapshot.jurisdiction,
        fetched_at=snapshot.fetched_at,
        mapper=mapper,
        source_record_id=record.source_record_id,
    )


def _unmapped_source_fields(
    raw: Mapping[str, Any],
    record: TrafficCollision | CollisionVehicle | CollisionPerson,
) -> tuple[str, ...]:
    consumed = _base_consumed_fields(record)

    for field_name in record.__class__.model_fields:
        attr = getattr(record, field_name)

        if isinstance(attr, MappedField):
            consumed.update(
                source_field for source_field in attr.source_fields if source_field in raw
            )

    return tuple(sorted(name for name in raw if name not in consumed))


def _base_consumed_fields(
    record: TrafficCollision | CollisionVehicle | CollisionPerson,
) -> set[str]:
    if isinstance(record, TrafficCollision):
        return set(_COLLISION_CONSUMED_FIELDS)

    if isinstance(record, CollisionVehicle):
        return set(_VEHICLE_CONSUMED_FIELDS)

    return set(_CASUALTY_CONSUMED_FIELDS)
