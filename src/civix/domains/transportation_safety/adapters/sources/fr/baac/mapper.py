"""France BAAC / ONISR mappers."""

from __future__ import annotations

from collections.abc import Mapping
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
from civix.domains.transportation_safety.models.road import SpeedLimit
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
}


@dataclass(frozen=True, slots=True)
class BaacCollisionMapper:
    """Maps BAAC characteristics rows to `TrafficCollision`."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=COLLISION_MAPPER_ID, version=MAPPER_VERSION)

    def __call__(self, record: RawRecord, snapshot: SourceSnapshot) -> MapResult[TrafficCollision]:
        raw = record.raw_data
        accident_id = require_text(
            record.raw_data.get("Num_Acc"),
            field_name="Num_Acc",
            mapper=self.version,
            source_record_id=record.source_record_id,
        )

        collision = TrafficCollision(
            provenance=_build_provenance(record=record, snapshot=snapshot, mapper=self.version),
            collision_id=accident_id,
            occurred_at=_map_occurred_at(record.raw_data),
            severity=MappedField[CollisionSeverity](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            address=_map_address(record.raw_data),
            coordinate=_map_coordinate(record.raw_data),
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
            road_surface=MappedField[CategoryRef](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
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
            speed_limit=MappedField[SpeedLimit](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
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
            total_injured_count=MappedField[int](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            vehicle_count=MappedField[int](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            person_count=MappedField[int](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
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
