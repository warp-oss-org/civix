"""France BAAC / ONISR mappers."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Final

from pydantic import ValidationError

from civix.core.identity.models.identifiers import MapperId
from civix.core.mapping.errors import MappingError
from civix.core.mapping.models.mapper import MappingReport, MapResult
from civix.core.mapping.parsers import int_or_none, require_text, str_or_none
from civix.core.provenance.models.provenance import MapperVersion, ProvenanceRef
from civix.core.quality.models.fields import FieldQuality, MappedField
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.spatial.models.location import Address, Coordinate
from civix.core.taxonomy.models.category import CategoryRef
from civix.domains.transportation_safety.adapters.sources.fr.baac.adapter import BAAC_RELEASE
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

COLLISION_MAPPER_ID: Final[MapperId] = MapperId("baac-collisions")
VEHICLE_MAPPER_ID: Final[MapperId] = MapperId("baac-vehicles")
USER_MAPPER_ID: Final[MapperId] = MapperId("baac-users")
MAPPER_VERSION: Final[str] = "0.1.0"

_TIMEZONE: Final[str] = "Europe/Paris"
_COUNTRY: Final[str] = "FR"
_LOCALITY_SOURCE_FIELD: Final[str] = "baac.commune_code"
_EJECTION_SOURCE_FIELD: Final[str] = "baac_open_data.ejection"

_COLLISION_CONSUMED_FIELDS: Final[frozenset[str]] = frozenset({"Num_Acc"})
_VEHICLE_CONSUMED_FIELDS: Final[frozenset[str]] = frozenset({"Num_Acc", "id_vehicule", "num_veh"})
_USER_CONSUMED_FIELDS: Final[frozenset[str]] = frozenset(
    {"Num_Acc", "id_usager", "id_vehicule", "num_veh"}
)

_GRAV_INJURY_MAP: Final[dict[str, InjuryOutcome]] = {
    "2": InjuryOutcome.FATAL,
    "3": InjuryOutcome.SERIOUS,
    "4": InjuryOutcome.MINOR,
    "1": InjuryOutcome.UNINJURED,
}
_CATU_ROLE_MAP: Final[dict[str, RoadUserRole]] = {
    "1": RoadUserRole.DRIVER,
    "2": RoadUserRole.PASSENGER,
    "3": RoadUserRole.PEDESTRIAN,
}
_CATV_CATEGORY_MAP: Final[dict[str, VehicleCategory]] = {
    "1": VehicleCategory.BICYCLE,
    "7": VehicleCategory.PASSENGER_CAR,
    "30": VehicleCategory.MOTORCYCLE,
}
_CATV_ROLE_MAP: Final[dict[str, RoadUserRole]] = {
    "1": RoadUserRole.CYCLIST,
    "30": RoadUserRole.MOTORCYCLIST,
}

_LIGHT_LABELS: Final[dict[str, str]] = {
    "1": "Daylight",
    "5": "Night with public lighting on",
}
_WEATHER_LABELS: Final[dict[str, str]] = {
    "1": "Normal",
    "2": "Light rain",
}
_COLLISION_TYPE_LABELS: Final[dict[str, str]] = {
    "3": "Two vehicles - side",
    "6": "Other collision",
}
_INTERSECTION_LABELS: Final[dict[str, str]] = {
    "1": "Outside intersection",
    "2": "X intersection",
    "3": "T intersection",
    "4": "Y intersection",
    "5": "Intersection with more than 4 branches",
    "6": "Roundabout",
    "7": "Square",
    "8": "Level crossing",
    "9": "Other intersection",
}
_ROAD_SURFACE_LABELS: Final[dict[str, str]] = {
    "1": "Normal",
    "2": "Wet",
}
_MANOEUVRE_LABELS: Final[dict[str, str]] = {
    "1": "Straight ahead",
    "13": "Changing lane to the left",
    "15": "Turning left",
}
_SAFETY_EQUIPMENT_LABELS: Final[dict[str, str]] = {
    "1": "Seat belt",
    "2": "Helmet",
    "8": "Other equipment",
}
_PEDESTRIAN_ACTION_LABELS: Final[dict[str, str]] = {
    "0": "Not applicable",
    "1": "Crossing",
    "3": "Walking along roadway",
}
_POSITION_LABELS: Final[dict[str, str]] = {
    "1": "Driver seat",
    "2": "Front passenger",
}

_NO_DATA_CODES_BY_FIELD: Final[dict[str, frozenset[str]]] = {
    "actp": frozenset({"0"}),
    "an_nais": frozenset({"0"}),
    "secu1": frozenset({"0", "-1"}),
    "vma": frozenset({"0", "-1"}),
}


@dataclass(frozen=True, slots=True)
class BaacLinkedResult:
    """Mapped records for one linked BAAC accident group."""

    collision: MapResult[TrafficCollision]
    vehicles: tuple[MapResult[CollisionVehicle], ...]
    people: tuple[MapResult[CollisionPerson], ...]


@dataclass(frozen=True, slots=True)
class BaacCollisionMapper:
    """Maps BAAC characteristics and location rows to `TrafficCollision`."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=COLLISION_MAPPER_ID, version=MAPPER_VERSION)

    def __call__(
        self,
        record: RawRecord,
        location_record: RawRecord,
        snapshot: SourceSnapshot,
        *,
        user_records: Sequence[RawRecord] = (),
        vehicle_records: Sequence[RawRecord] = (),
    ) -> MapResult[TrafficCollision]:
        raw = _merged_collision_raw(record.raw_data, location_record.raw_data)
        accident_id = require_text(
            record.raw_data.get("Num_Acc"),
            field_name="Num_Acc",
            mapper=self.version,
            source_record_id=record.source_record_id,
        )
        _require_matching_accident_id(
            accident_id=accident_id,
            record=location_record,
            mapper=self.version,
        )
        for user_record in user_records:
            _require_matching_accident_id(
                accident_id=accident_id,
                record=user_record,
                mapper=self.version,
            )
        for vehicle_record in vehicle_records:
            _require_matching_accident_id(
                accident_id=accident_id,
                record=vehicle_record,
                mapper=self.version,
            )

        collision = TrafficCollision(
            provenance=_build_provenance(record=record, snapshot=snapshot, mapper=self.version),
            collision_id=accident_id,
            occurred_at=_map_occurred_at(record.raw_data),
            severity=_map_collision_severity(user_records),
            address=_map_address(record.raw_data),
            coordinate=_map_coordinate(record.raw_data),
            locality=MappedField[str](
                value=None,
                quality=FieldQuality.NOT_PROVIDED,
                source_fields=(_LOCALITY_SOURCE_FIELD,),
            ),
            road_names=_map_road_names(location_record.raw_data),
            intersection_related=_map_intersection_related(record.raw_data),
            location_description=_map_location_description(record.raw_data),
            weather=_map_source_category(
                record.raw_data,
                "atm",
                labels=_WEATHER_LABELS,
                taxonomy_id="baac-atm",
            ),
            lighting=_map_source_category(
                record.raw_data,
                "lum",
                labels=_LIGHT_LABELS,
                taxonomy_id="baac-lum",
            ),
            road_surface=_map_source_category(
                location_record.raw_data,
                "surf",
                labels=_ROAD_SURFACE_LABELS,
                taxonomy_id="baac-surf",
            ),
            road_condition=MappedField[CategoryRef](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            traffic_control=MappedField[CategoryRef](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            speed_limit=_map_speed_limit(location_record.raw_data),
            fatal_count=_map_injury_count(user_records, codes=frozenset({"2"})),
            serious_injury_count=_map_injury_count(user_records, codes=frozenset({"3"})),
            minor_injury_count=_map_injury_count(user_records, codes=frozenset({"4"})),
            possible_injury_count=MappedField[int](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            uninjured_count=_map_injury_count(user_records, codes=frozenset({"1"})),
            unknown_injury_count=_map_unknown_injury_count(user_records),
            total_injured_count=_map_injury_count(user_records, codes=frozenset({"2", "3", "4"})),
            vehicle_count=_map_record_count(vehicle_records, source_fields=("id_vehicule",)),
            person_count=_map_record_count(user_records, source_fields=("id_usager",)),
            contributing_factors=MappedField[tuple[ContributingFactor, ...]](
                value=(
                    ContributingFactor(
                        raw_label=_category_label(
                            code=str_or_none(record.raw_data.get("col")),
                            labels=_COLLISION_TYPE_LABELS,
                        ),
                        category=_category_ref(
                            code=str_or_none(record.raw_data.get("col")),
                            labels=_COLLISION_TYPE_LABELS,
                            taxonomy_id="baac-col",
                        ),
                    ),
                ),
                quality=FieldQuality.STANDARDIZED,
                source_fields=("col",),
            ),
        )
        report = MappingReport(unmapped_source_fields=_unmapped_source_fields(raw, collision))

        return MapResult[TrafficCollision](record=collision, report=report)


@dataclass(frozen=True, slots=True)
class BaacVehicleMapper:
    """Maps BAAC vehicle rows to `CollisionVehicle`."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=VEHICLE_MAPPER_ID, version=MAPPER_VERSION)

    def __call__(self, record: RawRecord, snapshot: SourceSnapshot) -> MapResult[CollisionVehicle]:
        raw = record.raw_data
        accident_id = require_text(
            raw.get("Num_Acc"),
            field_name="Num_Acc",
            mapper=self.version,
            source_record_id=record.source_record_id,
        )
        vehicle_id = _vehicle_id(raw, mapper=self.version, record=record)
        vehicle = CollisionVehicle(
            provenance=_build_provenance(record=record, snapshot=snapshot, mapper=self.version),
            collision_id=accident_id,
            vehicle_id=vehicle_id,
            category=_map_vehicle_category(raw),
            road_user_role=_map_vehicle_role(raw),
            occupant_count=_map_count(raw, "occutc"),
            travel_direction=MappedField[CategoryRef](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            maneuver=_map_source_category(
                raw,
                "manv",
                labels=_MANOEUVRE_LABELS,
                taxonomy_id="baac-manv",
            ),
            damage=MappedField[CategoryRef](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            contributing_factors=MappedField[tuple[ContributingFactor, ...]](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
        )
        report = MappingReport(unmapped_source_fields=_unmapped_source_fields(raw, vehicle))

        return MapResult[CollisionVehicle](record=vehicle, report=report)


@dataclass(frozen=True, slots=True)
class BaacUserMapper:
    """Maps BAAC user rows to `CollisionPerson`."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=USER_MAPPER_ID, version=MAPPER_VERSION)

    def __call__(
        self,
        record: RawRecord,
        snapshot: SourceSnapshot,
        *,
        occurrence_year: int | None = None,
    ) -> MapResult[CollisionPerson]:
        raw = record.raw_data
        accident_id = require_text(
            raw.get("Num_Acc"),
            field_name="Num_Acc",
            mapper=self.version,
            source_record_id=record.source_record_id,
        )
        person = CollisionPerson(
            provenance=_build_provenance(record=record, snapshot=snapshot, mapper=self.version),
            collision_id=accident_id,
            person_id=_person_id(raw, mapper=self.version, record=record),
            vehicle_id=_map_person_vehicle_id(raw),
            role=_map_person_role(raw),
            injury_outcome=_map_person_injury(raw),
            age=_map_age(raw, occurrence_year=occurrence_year),
            safety_equipment=_map_source_category(
                raw,
                "secu1",
                labels=_SAFETY_EQUIPMENT_LABELS,
                taxonomy_id="baac-secu1",
            ),
            position_in_vehicle=_map_position(raw),
            ejection=MappedField[CategoryRef](
                value=None,
                quality=FieldQuality.NOT_PROVIDED,
                source_fields=(_EJECTION_SOURCE_FIELD,),
            ),
            contributing_factors=_map_person_contributing_factors(raw),
        )
        report = MappingReport(unmapped_source_fields=_unmapped_source_fields(raw, person))

        return MapResult[CollisionPerson](record=person, report=report)


@dataclass(frozen=True, slots=True)
class BaacLinkedMapper:
    """Maps linked BAAC characteristics, location, vehicle, and user rows by `Num_Acc`."""

    collision_mapper: BaacCollisionMapper = BaacCollisionMapper()
    vehicle_mapper: BaacVehicleMapper = BaacVehicleMapper()
    user_mapper: BaacUserMapper = BaacUserMapper()

    def map_records(
        self,
        *,
        characteristics: Iterable[RawRecord],
        locations: Iterable[RawRecord],
        vehicles: Iterable[RawRecord],
        users: Iterable[RawRecord],
        characteristics_snapshot: SourceSnapshot,
        locations_snapshot: SourceSnapshot,
        vehicles_snapshot: SourceSnapshot,
        users_snapshot: SourceSnapshot,
    ) -> tuple[BaacLinkedResult, ...]:
        location_groups = _group_by_accident_id(locations, mapper=self.collision_mapper.version)
        vehicle_groups = _group_by_accident_id(vehicles, mapper=self.vehicle_mapper.version)
        user_groups = _group_by_accident_id(users, mapper=self.user_mapper.version)

        results: list[BaacLinkedResult] = []
        for characteristic_record in sorted(
            characteristics,
            key=lambda row: require_text(
                row.raw_data.get("Num_Acc"),
                field_name="Num_Acc",
                mapper=self.collision_mapper.version,
                source_record_id=row.source_record_id,
            ),
        ):
            accident_id = require_text(
                characteristic_record.raw_data.get("Num_Acc"),
                field_name="Num_Acc",
                mapper=self.collision_mapper.version,
                source_record_id=characteristic_record.source_record_id,
            )
            location_record = _single_location_record(
                accident_id=accident_id,
                records=location_groups.get(accident_id, ()),
            )
            user_records = user_groups.get(accident_id, ())
            vehicle_records = vehicle_groups.get(accident_id, ())
            occurrence_year = int_or_none(characteristic_record.raw_data.get("an"))
            collision = self.collision_mapper(
                characteristic_record,
                location_record,
                characteristics_snapshot,
                user_records=user_records,
                vehicle_records=vehicle_records,
            )
            mapped_vehicles = tuple(
                self.vehicle_mapper(record, vehicles_snapshot) for record in vehicle_records
            )
            mapped_people = tuple(
                self.user_mapper(record, users_snapshot, occurrence_year=occurrence_year)
                for record in user_records
            )

            results.append(
                BaacLinkedResult(
                    collision=collision,
                    vehicles=mapped_vehicles,
                    people=mapped_people,
                )
            )

        return tuple(results)


def _map_occurred_at(raw: Mapping[str, Any]) -> MappedField[OccurrenceTime]:
    parsed_date = _parse_date(raw)

    if parsed_date is None:
        return MappedField[OccurrenceTime](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("jour", "mois", "an"),
        )

    parsed_datetime = _parse_datetime(parsed_date, raw.get("hrmn"))

    if parsed_datetime is None:
        return MappedField[OccurrenceTime](
            value=OccurrenceTime(
                precision=OccurrenceTimePrecision.DATE,
                date_value=parsed_date,
                timezone_status=OccurrenceTimezoneStatus.NAMED_LOCAL,
                timezone=_TIMEZONE,
            ),
            quality=FieldQuality.STANDARDIZED,
            source_fields=("jour", "mois", "an"),
        )

    return MappedField[OccurrenceTime](
        value=OccurrenceTime(
            precision=OccurrenceTimePrecision.DATETIME,
            datetime_value=parsed_datetime,
            timezone_status=OccurrenceTimezoneStatus.NAMED_LOCAL,
            timezone=_TIMEZONE,
        ),
        quality=FieldQuality.STANDARDIZED,
        source_fields=("jour", "mois", "an", "hrmn"),
    )


def _parse_date(raw: Mapping[str, Any]) -> date | None:
    year = int_or_none(raw.get("an"))
    month = int_or_none(raw.get("mois"))
    day = int_or_none(raw.get("jour"))

    if year is None or month is None or day is None:
        return None

    try:
        return date(year=year, month=month, day=day)
    except ValueError:
        return None


def _parse_datetime(date_value: date, value: object) -> datetime | None:
    text = str_or_none(value)

    if text is None:
        return None

    if ":" in text:
        parts = text.split(":", maxsplit=1)
    else:
        normalized = text.zfill(4)
        parts = [normalized[:2], normalized[2:]]

    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except (IndexError, ValueError):
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


def _map_collision_severity(records: Sequence[RawRecord]) -> MappedField[CollisionSeverity]:
    if not records:
        return MappedField[CollisionSeverity](
            value=None,
            quality=FieldQuality.UNMAPPED,
            source_fields=(),
        )

    codes = tuple(str_or_none(record.raw_data.get("grav")) for record in records)

    if "2" in codes:
        return _derived_collision_severity(CollisionSeverity.FATAL)

    if "3" in codes:
        return _derived_collision_severity(CollisionSeverity.SERIOUS_INJURY)

    if "4" in codes:
        return _derived_collision_severity(CollisionSeverity.MINOR_INJURY)

    # BAAC is injury-collision scoped; an all-uninjured group is inconsistent enough
    # that event severity should not be upgraded to property-damage-only.
    if all(code == "1" for code in codes if code is not None):
        return _derived_collision_severity(CollisionSeverity.UNKNOWN)

    return MappedField[CollisionSeverity](
        value=CollisionSeverity.UNKNOWN,
        quality=FieldQuality.INFERRED,
        source_fields=("grav",),
    )


def _derived_collision_severity(value: CollisionSeverity) -> MappedField[CollisionSeverity]:
    return MappedField[CollisionSeverity](
        value=value,
        quality=FieldQuality.DERIVED,
        source_fields=("grav",),
    )


def _map_address(raw: Mapping[str, Any]) -> MappedField[Address]:
    street = str_or_none(raw.get("adr"))

    if street is None:
        return MappedField[Address](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("adr",),
        )

    return MappedField[Address](
        value=Address(country=_COUNTRY, street=street),
        quality=FieldQuality.DIRECT,
        source_fields=("adr",),
    )


def _map_coordinate(raw: Mapping[str, Any]) -> MappedField[Coordinate]:
    latitude = _french_float(raw.get("lat"))
    longitude = _french_float(raw.get("long"))

    if latitude is None or longitude is None:
        return MappedField[Coordinate](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("lat", "long"),
        )

    try:
        coordinate = Coordinate(latitude=latitude, longitude=longitude)
    except ValidationError:
        return MappedField[Coordinate](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("lat", "long"),
        )

    return MappedField[Coordinate](
        value=coordinate,
        quality=FieldQuality.STANDARDIZED,
        source_fields=("lat", "long"),
    )


def _french_float(value: object) -> float | None:
    text = str_or_none(value)

    if text is None:
        return None

    try:
        return float(text.replace(",", "."))
    except ValueError:
        return None


def _map_road_names(raw: Mapping[str, Any]) -> MappedField[tuple[str, ...]]:
    road_name = str_or_none(raw.get("voie"))

    if road_name is None:
        return MappedField[tuple[str, ...]](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("voie",),
        )

    return MappedField[tuple[str, ...]](
        value=(road_name,),
        quality=FieldQuality.DIRECT,
        source_fields=("voie",),
    )


def _map_intersection_related(raw: Mapping[str, Any]) -> MappedField[bool]:
    code = str_or_none(raw.get("int"))

    if code is None:
        return MappedField[bool](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("int",),
        )

    if code in {"0", "1"}:
        return MappedField[bool](
            value=False,
            quality=FieldQuality.STANDARDIZED,
            source_fields=("int",),
        )

    if code in _INTERSECTION_LABELS:
        return MappedField[bool](
            value=True,
            quality=FieldQuality.STANDARDIZED,
            source_fields=("int",),
        )

    return MappedField[bool](
        value=True,
        quality=FieldQuality.INFERRED,
        source_fields=("int",),
    )


def _map_location_description(raw: Mapping[str, Any]) -> MappedField[str]:
    aggregate_code = str_or_none(raw.get("agg"))

    if aggregate_code is None:
        return MappedField[str](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("agg",),
        )

    label = "Built-up area" if aggregate_code == "2" else "Outside built-up area"

    return MappedField[str](
        value=label,
        quality=FieldQuality.STANDARDIZED,
        source_fields=("agg",),
    )


def _map_speed_limit(raw: Mapping[str, Any]) -> MappedField[SpeedLimit]:
    code = str_or_none(raw.get("vma"))

    if code is None or code in _no_data_codes("vma"):
        return MappedField[SpeedLimit](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("vma",),
        )

    value = int_or_none(code)

    if value is None or value < 0:
        return MappedField[SpeedLimit](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("vma",),
        )

    return MappedField[SpeedLimit](
        value=SpeedLimit(value=value, unit=SpeedLimitUnit.KILOMETRES_PER_HOUR),
        quality=FieldQuality.STANDARDIZED,
        source_fields=("vma",),
    )


def _map_injury_count(
    records: Sequence[RawRecord],
    *,
    codes: frozenset[str],
) -> MappedField[int]:
    if not records:
        return MappedField[int](value=None, quality=FieldQuality.UNMAPPED, source_fields=())

    value = sum(1 for record in records if str_or_none(record.raw_data.get("grav")) in codes)

    return MappedField[int](
        value=value,
        quality=FieldQuality.DERIVED,
        source_fields=("grav",),
    )


def _map_unknown_injury_count(records: Sequence[RawRecord]) -> MappedField[int]:
    if not records:
        return MappedField[int](value=None, quality=FieldQuality.UNMAPPED, source_fields=())

    value = sum(
        1
        for record in records
        if str_or_none(record.raw_data.get("grav")) not in {"1", "2", "3", "4"}
    )

    return MappedField[int](
        value=value,
        quality=FieldQuality.DERIVED,
        source_fields=("grav",),
    )


def _map_record_count(
    records: Sequence[RawRecord],
    *,
    source_fields: tuple[str, ...],
) -> MappedField[int]:
    if not records:
        return MappedField[int](value=None, quality=FieldQuality.UNMAPPED, source_fields=())

    return MappedField[int](
        value=len(records),
        quality=FieldQuality.DERIVED,
        source_fields=source_fields,
    )


def _vehicle_id(
    raw: Mapping[str, Any],
    *,
    mapper: MapperVersion,
    record: RawRecord,
) -> str:
    accident_id = require_text(
        raw.get("Num_Acc"),
        field_name="Num_Acc",
        mapper=mapper,
        source_record_id=record.source_record_id,
    )
    source_vehicle_id = str_or_none(raw.get("id_vehicule"))

    if source_vehicle_id is not None:
        return f"{accident_id}:{source_vehicle_id}"

    vehicle_number = require_text(
        raw.get("num_veh"),
        field_name="num_veh",
        mapper=mapper,
        source_record_id=record.source_record_id,
    )

    return f"{accident_id}:{vehicle_number}"


def _map_vehicle_category(raw: Mapping[str, Any]) -> MappedField[VehicleCategory]:
    code = str_or_none(raw.get("catv"))

    if code is None:
        return MappedField[VehicleCategory](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("catv",),
        )

    category = _CATV_CATEGORY_MAP.get(code, VehicleCategory.UNKNOWN)
    quality = FieldQuality.STANDARDIZED if code in _CATV_CATEGORY_MAP else FieldQuality.INFERRED

    return MappedField[VehicleCategory](
        value=category,
        quality=quality,
        source_fields=("catv",),
    )


def _map_vehicle_role(raw: Mapping[str, Any]) -> MappedField[RoadUserRole]:
    code = str_or_none(raw.get("catv"))

    if code is None:
        return MappedField[RoadUserRole](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("catv",),
        )

    role = _CATV_ROLE_MAP.get(code, RoadUserRole.UNKNOWN)
    quality = FieldQuality.STANDARDIZED if code in _CATV_ROLE_MAP else FieldQuality.INFERRED

    return MappedField[RoadUserRole](
        value=role,
        quality=quality,
        source_fields=("catv",),
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


def _person_id(
    raw: Mapping[str, Any],
    *,
    mapper: MapperVersion,
    record: RawRecord,
) -> str:
    accident_id = require_text(
        raw.get("Num_Acc"),
        field_name="Num_Acc",
        mapper=mapper,
        source_record_id=record.source_record_id,
    )
    source_user_id = str_or_none(raw.get("id_usager"))

    if source_user_id is not None:
        return f"{accident_id}:{source_user_id}"

    vehicle_number = str_or_none(raw.get("num_veh"))
    place = str_or_none(raw.get("place"))

    if vehicle_number is not None and place is not None:
        return f"{accident_id}:{vehicle_number}:{place}"

    raise MappingError(
        "missing BAAC user identity fields",
        mapper=mapper,
        source_record_id=record.source_record_id,
        source_fields=("id_usager", "num_veh", "place"),
    )


def _map_person_vehicle_id(raw: Mapping[str, Any]) -> str | None:
    accident_id = str_or_none(raw.get("Num_Acc"))
    source_vehicle_id = str_or_none(raw.get("id_vehicule"))

    if accident_id is None:
        return None

    if source_vehicle_id is not None:
        return f"{accident_id}:{source_vehicle_id}"

    vehicle_number = str_or_none(raw.get("num_veh"))

    if vehicle_number is None:
        return None

    return f"{accident_id}:{vehicle_number}"


def _map_person_role(raw: Mapping[str, Any]) -> MappedField[RoadUserRole]:
    code = str_or_none(raw.get("catu"))

    if code is None:
        return MappedField[RoadUserRole](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("catu",),
        )

    role = _CATU_ROLE_MAP.get(code, RoadUserRole.UNKNOWN)
    quality = FieldQuality.STANDARDIZED if code in _CATU_ROLE_MAP else FieldQuality.INFERRED

    return MappedField[RoadUserRole](
        value=role,
        quality=quality,
        source_fields=("catu",),
    )


def _map_person_injury(raw: Mapping[str, Any]) -> MappedField[InjuryOutcome]:
    code = str_or_none(raw.get("grav"))

    if code is None:
        return MappedField[InjuryOutcome](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("grav",),
        )

    outcome = _GRAV_INJURY_MAP.get(code, InjuryOutcome.UNKNOWN)
    quality = FieldQuality.STANDARDIZED if code in _GRAV_INJURY_MAP else FieldQuality.INFERRED

    return MappedField[InjuryOutcome](
        value=outcome,
        quality=quality,
        source_fields=("grav",),
    )


def _map_age(
    raw: Mapping[str, Any],
    *,
    occurrence_year: int | None,
) -> MappedField[int]:
    birth_year = int_or_none(raw.get("an_nais"))

    if birth_year is None or birth_year <= 0:
        return MappedField[int](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("an_nais",),
        )

    if occurrence_year is None or occurrence_year < birth_year:
        return MappedField[int](
            value=None,
            quality=FieldQuality.UNMAPPED,
            source_fields=(),
        )

    return MappedField[int](
        value=occurrence_year - birth_year,
        quality=FieldQuality.DERIVED,
        source_fields=("an_nais", "an"),
    )


def _map_position(raw: Mapping[str, Any]) -> MappedField[CategoryRef]:
    code = str_or_none(raw.get("place"))

    if code is None:
        return MappedField[CategoryRef](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("place",),
        )

    label = _POSITION_LABELS.get(code)

    if label is None:
        return MappedField[CategoryRef](
            value=CategoryRef(
                code=code,
                label=f"Unsupported code {code}",
                taxonomy_id="baac-place",
                taxonomy_version=BAAC_RELEASE,
            ),
            quality=FieldQuality.INFERRED,
            source_fields=("place",),
        )

    return MappedField[CategoryRef](
        value=CategoryRef(
            code=code,
            label=label,
            taxonomy_id="baac-place",
            taxonomy_version=BAAC_RELEASE,
        ),
        quality=FieldQuality.STANDARDIZED,
        source_fields=("place",),
    )


def _map_person_contributing_factors(
    raw: Mapping[str, Any],
) -> MappedField[tuple[ContributingFactor, ...]]:
    code = str_or_none(raw.get("actp"))

    if code is None or code in _no_data_codes("actp"):
        return MappedField[tuple[ContributingFactor, ...]](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("actp",),
        )

    label = _category_label(code=code, labels=_PEDESTRIAN_ACTION_LABELS)

    return MappedField[tuple[ContributingFactor, ...]](
        value=(
            ContributingFactor(
                raw_label=label,
                category=_category_ref(
                    code=code,
                    labels=_PEDESTRIAN_ACTION_LABELS,
                    taxonomy_id="baac-actp",
                ),
            ),
        ),
        quality=FieldQuality.STANDARDIZED
        if code in _PEDESTRIAN_ACTION_LABELS
        else FieldQuality.INFERRED,
        source_fields=("actp",),
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
                taxonomy_version=BAAC_RELEASE,
            ),
            quality=FieldQuality.INFERRED,
            source_fields=(field_name,),
        )

    return MappedField[CategoryRef](
        value=CategoryRef(
            code=code,
            label=label,
            taxonomy_id=taxonomy_id,
            taxonomy_version=BAAC_RELEASE,
        ),
        quality=FieldQuality.STANDARDIZED,
        source_fields=(field_name,),
    )


def _category_ref(
    *,
    code: str | None,
    labels: Mapping[str, str],
    taxonomy_id: str,
) -> CategoryRef | None:
    if code is None:
        return None

    return CategoryRef(
        code=code,
        label=_category_label(code=code, labels=labels),
        taxonomy_id=taxonomy_id,
        taxonomy_version=BAAC_RELEASE,
    )


def _category_label(
    *,
    code: str | None,
    labels: Mapping[str, str],
) -> str:
    if code is None:
        return "Not provided"

    return labels.get(code, f"Unsupported code {code}")


def _no_data_codes(field_name: str) -> frozenset[str]:
    return _NO_DATA_CODES_BY_FIELD.get(field_name, frozenset())


def _require_matching_accident_id(
    *,
    accident_id: str,
    record: RawRecord,
    mapper: MapperVersion,
) -> None:
    record_accident_id = require_text(
        record.raw_data.get("Num_Acc"),
        field_name="Num_Acc",
        mapper=mapper,
        source_record_id=record.source_record_id,
    )

    if record_accident_id == accident_id:
        return

    raise MappingError(
        "BAAC linked rows contain multiple Num_Acc values",
        mapper=mapper,
        source_record_id=record.source_record_id,
        source_fields=("Num_Acc",),
    )


def _single_location_record(
    *,
    accident_id: str,
    records: Sequence[RawRecord],
) -> RawRecord:
    if len(records) == 1:
        return records[0]

    raise MappingError(
        "BAAC accident group must contain exactly one location row",
        mapper=MapperVersion(mapper_id=COLLISION_MAPPER_ID, version=MAPPER_VERSION),
        source_record_id=accident_id,
        source_fields=("Num_Acc",),
    )


def _group_by_accident_id(
    records: Iterable[RawRecord],
    *,
    mapper: MapperVersion,
) -> dict[str, tuple[RawRecord, ...]]:
    grouped: defaultdict[str, list[RawRecord]] = defaultdict(list)

    for record in records:
        accident_id = require_text(
            record.raw_data.get("Num_Acc"),
            field_name="Num_Acc",
            mapper=mapper,
            source_record_id=record.source_record_id,
        )
        grouped[accident_id].append(record)

    return {key: tuple(group) for key, group in grouped.items()}


def _merged_collision_raw(
    characteristics: Mapping[str, Any],
    locations: Mapping[str, Any],
) -> dict[str, Any]:
    merged = dict(locations)
    merged.update(characteristics)

    return merged


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

    return tuple(sorted(field_name for field_name in raw if field_name not in consumed))


def _base_consumed_fields(
    record: TrafficCollision | CollisionVehicle | CollisionPerson,
) -> set[str]:
    if isinstance(record, TrafficCollision):
        return set(_COLLISION_CONSUMED_FIELDS)

    if isinstance(record, CollisionVehicle):
        return set(_VEHICLE_CONSUMED_FIELDS)

    return set(_USER_CONSUMED_FIELDS)
