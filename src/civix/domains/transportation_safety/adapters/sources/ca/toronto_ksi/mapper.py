"""Toronto KSI grouped mapper."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any, Final

from pydantic import ValidationError

from civix.core.identity.models.identifiers import MapperId
from civix.core.mapping.errors import MappingError
from civix.core.mapping.models.mapper import MappingReport, MapResult
from civix.core.mapping.parsers import (
    int_or_none,
    require_text,
    slugify,
    str_or_none,
)
from civix.core.provenance.models.provenance import MapperVersion, ProvenanceRef
from civix.core.quality.models.fields import FieldQuality, MappedField
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.spatial.models.location import Address, Coordinate
from civix.core.taxonomy.models.category import CategoryRef
from civix.domains.transportation_safety.adapters.sources.ca.toronto_ksi._grouping import (
    GroupChoice,
    choose_float,
    choose_int,
    choose_text,
    collect_conflicts,
    require_choice_value,
)
from civix.domains.transportation_safety.adapters.sources.ca.toronto_ksi.schema import (
    TORONTO_KSI_SCHEMA,
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

MAPPER_ID: Final[MapperId] = MapperId("toronto-ksi")
MAPPER_VERSION: Final[str] = "0.1.0"

_TAXONOMY_VERSION: Final[str] = TORONTO_KSI_SCHEMA.version
_TORONTO_TIMEZONE: Final[str] = "America/Toronto"
_LOCALITY: Final[str] = "Toronto"
_REGION: Final[str] = "ON"
_COUNTRY: Final[str] = "CA"
_LOCALITY_SOURCE_FIELD: Final[str] = "snapshot.jurisdiction.locality"
_COLLISION_SOURCE_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "collision_id",
        "accdate",
        "stname1",
        "stname2",
        "stname3",
        "per_inv",
        "acclass",
        "accloc",
        "traffictl",
        "impactype",
        "visible",
        "light",
        "rdsfcond",
        "road_class",
        "longitude",
        "latitude",
        "wardname",
        "division",
        "neighbourhood",
    }
)
_VEHICLE_SOURCE_FIELDS: Final[frozenset[str]] = frozenset(
    {"collision_id", "veh_no", "vehtype", "initdir", "manoeuvre"}
)
_PERSON_SOURCE_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "collision_id",
        "per_no",
        "veh_no",
        "road_user",
        "injury",
        "invage",
        "safequip",
        "drivact",
        "drivcond",
        "pedact",
        "pedcond",
        "pedtype",
        "cyclistype",
        "cycact",
        "cyccond",
    }
)
_MAPPER_CONSUMED_FIELDS: Final[frozenset[str]] = (
    _COLLISION_SOURCE_FIELDS | _VEHICLE_SOURCE_FIELDS | _PERSON_SOURCE_FIELDS | frozenset({"_id"})
)

_SEVERITY_MAP: Final[dict[str, CollisionSeverity]] = {
    "fatal": CollisionSeverity.FATAL,
    "non-fatal injury": CollisionSeverity.SERIOUS_INJURY,
}
_INJURY_MAP: Final[dict[str, InjuryOutcome]] = {
    "fatal": InjuryOutcome.FATAL,
    "major": InjuryOutcome.SERIOUS,
    "minor": InjuryOutcome.MINOR,
    "none": InjuryOutcome.UNINJURED,
}


@dataclass(frozen=True, slots=True)
class TorontoKsiGroupResult:
    """Normalized records produced from one Toronto KSI collision group."""

    collision: MapResult[TrafficCollision]
    vehicles: tuple[MapResult[CollisionVehicle], ...]
    people: tuple[MapResult[CollisionPerson], ...]


@dataclass(frozen=True, slots=True)
class TorontoKsiGroupedMapper:
    """Maps person-level Toronto KSI rows grouped by `collision_id`."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=MAPPER_ID, version=MAPPER_VERSION)

    def group_records(self, records: Iterable[RawRecord]) -> tuple[tuple[RawRecord, ...], ...]:
        groups: defaultdict[str, list[RawRecord]] = defaultdict(list)

        for record in records:
            collision_id = require_text(
                record.raw_data.get("collision_id"),
                field_name="collision_id",
                mapper=self.version,
                source_record_id=record.source_record_id,
            )
            groups[collision_id].append(record)

        return tuple(tuple(group) for _, group in sorted(groups.items()))

    def map_group(
        self,
        records: Sequence[RawRecord],
        snapshot: SourceSnapshot,
    ) -> TorontoKsiGroupResult:
        if not records:
            raise MappingError(
                "cannot map an empty Toronto KSI collision group",
                mapper=self.version,
                source_record_id=None,
                source_fields=("collision_id",),
            )

        collision_id = self._collision_id(records)
        collision = self._map_collision(
            collision_id=collision_id, records=records, snapshot=snapshot
        )
        vehicles = self._map_vehicles(collision_id=collision_id, records=records, snapshot=snapshot)
        people = tuple(
            self._map_person(collision_id=collision_id, record=record, snapshot=snapshot)
            for record in records
        )

        return TorontoKsiGroupResult(collision=collision, vehicles=vehicles, people=people)

    def _collision_id(self, records: Sequence[RawRecord]) -> str:
        ids = {
            require_text(
                record.raw_data.get("collision_id"),
                field_name="collision_id",
                mapper=self.version,
                source_record_id=record.source_record_id,
            )
            for record in records
        }

        if len(ids) == 1:
            return next(iter(ids))

        raise MappingError(
            "Toronto KSI group contains multiple collision_id values",
            mapper=self.version,
            source_record_id=None,
            source_fields=("collision_id",),
        )

    def _map_collision(
        self,
        *,
        collision_id: str,
        records: Sequence[RawRecord],
        snapshot: SourceSnapshot,
    ) -> MapResult[TrafficCollision]:
        accdate = choose_text(
            records,
            "accdate",
            output_field="occurred_at",
            mapper=self.version,
            hard=True,
        )
        severity = _map_collision_severity(
            choose_text(
                records,
                "acclass",
                output_field="severity",
                mapper=self.version,
                hard=True,
            )
        )
        latitude = choose_float(
            records,
            "latitude",
            output_field="coordinate",
            mapper=self.version,
            hard=True,
        )
        longitude = choose_float(
            records,
            "longitude",
            output_field="coordinate",
            mapper=self.version,
            hard=True,
        )
        stname1 = choose_text(records, "stname1", output_field="road_names", mapper=self.version)
        stname2 = choose_text(records, "stname2", output_field="road_names", mapper=self.version)
        stname3 = choose_text(records, "stname3", output_field="road_names", mapper=self.version)
        accloc = choose_text(
            records, "accloc", output_field="location_description", mapper=self.version
        )
        traffictl = choose_text(
            records, "traffictl", output_field="traffic_control", mapper=self.version
        )
        light = choose_text(records, "light", output_field="lighting", mapper=self.version)
        rdsfcond = choose_text(
            records, "rdsfcond", output_field="road_surface", mapper=self.version
        )
        visible = choose_text(records, "visible", output_field="weather", mapper=self.version)
        per_inv = choose_int(records, "per_inv", output_field="person_count", mapper=self.version)
        road_names = tuple(
            value for value in (stname1.value, stname2.value, stname3.value) if value is not None
        )
        choices = (
            accdate,
            severity,
            latitude,
            longitude,
            stname1,
            stname2,
            stname3,
            accloc,
            traffictl,
            light,
            rdsfcond,
            visible,
            per_inv,
        )
        injury_counts = _injury_counts(records)
        collision = TrafficCollision(
            provenance=self._build_provenance(
                source_record_id=collision_id,
                snapshot=snapshot,
            ),
            collision_id=collision_id,
            occurred_at=_map_occurred_at(accdate),
            severity=require_choice_value(severity),
            address=_map_address(road_names),
            coordinate=_map_coordinate(latitude, longitude),
            locality=MappedField[str](
                value=_LOCALITY,
                quality=FieldQuality.INFERRED,
                source_fields=(_LOCALITY_SOURCE_FIELD,),
            ),
            road_names=_map_road_names(road_names),
            intersection_related=_map_intersection_related(accloc),
            location_description=_map_text_field(accloc, "accloc"),
            weather=_map_category_text(visible, "visible"),
            lighting=_map_category_text(light, "light"),
            road_surface=_map_category_text(rdsfcond, "rdsfcond"),
            road_condition=MappedField[CategoryRef](
                value=None, quality=FieldQuality.UNMAPPED, source_fields=()
            ),
            traffic_control=_map_category_text(traffictl, "traffictl"),
            speed_limit=MappedField[SpeedLimit](
                value=None, quality=FieldQuality.UNMAPPED, source_fields=()
            ),
            fatal_count=_count_field(injury_counts[InjuryOutcome.FATAL], "injury"),
            serious_injury_count=_count_field(injury_counts[InjuryOutcome.SERIOUS], "injury"),
            minor_injury_count=_count_field(injury_counts[InjuryOutcome.MINOR], "injury"),
            possible_injury_count=_count_field(injury_counts[InjuryOutcome.POSSIBLE], "injury"),
            uninjured_count=_count_field(injury_counts[InjuryOutcome.UNINJURED], "injury"),
            unknown_injury_count=_count_field(injury_counts[InjuryOutcome.UNKNOWN], "injury"),
            total_injured_count=_count_field(
                injury_counts[InjuryOutcome.FATAL]
                + injury_counts[InjuryOutcome.SERIOUS]
                + injury_counts[InjuryOutcome.MINOR]
                + injury_counts[InjuryOutcome.POSSIBLE],
                "injury",
            ),
            vehicle_count=_count_field(_vehicle_count(records), "veh_no"),
            person_count=_map_person_count(per_inv, len(records)),
            contributing_factors=MappedField[tuple[ContributingFactor, ...]](
                value=None, quality=FieldQuality.UNMAPPED, source_fields=()
            ),
        )
        report = MappingReport(
            unmapped_source_fields=_unmapped_source_fields(records, _MAPPER_CONSUMED_FIELDS),
            conflicts=collect_conflicts(choices),
        )

        return MapResult[TrafficCollision](record=collision, report=report)

    def _map_vehicles(
        self,
        *,
        collision_id: str,
        records: Sequence[RawRecord],
        snapshot: SourceSnapshot,
    ) -> tuple[MapResult[CollisionVehicle], ...]:
        grouped: defaultdict[str, list[RawRecord]] = defaultdict(list)

        for record in records:
            veh_no = str_or_none(record.raw_data.get("veh_no"))

            if veh_no is not None:
                grouped[veh_no].append(record)

        return tuple(
            self._map_vehicle(
                collision_id=collision_id, veh_no=veh_no, records=group, snapshot=snapshot
            )
            for veh_no, group in sorted(grouped.items())
        )

    def _map_vehicle(
        self,
        *,
        collision_id: str,
        veh_no: str,
        records: Sequence[RawRecord],
        snapshot: SourceSnapshot,
    ) -> MapResult[CollisionVehicle]:
        vehicle_id = _vehicle_id(collision_id, veh_no)
        vehtype = choose_text(records, "vehtype", output_field="category", mapper=self.version)
        initdir = choose_text(
            records, "initdir", output_field="travel_direction", mapper=self.version
        )
        manoeuvre = choose_text(records, "manoeuvre", output_field="maneuver", mapper=self.version)
        vehicle = CollisionVehicle(
            provenance=self._build_provenance(
                source_record_id=vehicle_id,
                snapshot=snapshot,
            ),
            collision_id=collision_id,
            vehicle_id=vehicle_id,
            category=_map_vehicle_category(vehtype),
            road_user_role=_map_vehicle_role(vehtype),
            occupant_count=MappedField[int](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            travel_direction=_map_category_text(initdir, "initdir"),
            maneuver=_map_category_text(manoeuvre, "manoeuvre"),
            damage=MappedField[CategoryRef](
                value=None, quality=FieldQuality.UNMAPPED, source_fields=()
            ),
            contributing_factors=MappedField[tuple[ContributingFactor, ...]](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
        )
        report = MappingReport(
            unmapped_source_fields=_unmapped_source_fields(records, _MAPPER_CONSUMED_FIELDS),
            conflicts=collect_conflicts((vehtype, initdir, manoeuvre)),
        )

        return MapResult[CollisionVehicle](record=vehicle, report=report)

    def _map_person(
        self,
        *,
        collision_id: str,
        record: RawRecord,
        snapshot: SourceSnapshot,
    ) -> MapResult[CollisionPerson]:
        raw = record.raw_data
        per_no = require_text(
            raw.get("per_no"),
            field_name="per_no",
            mapper=self.version,
            source_record_id=record.source_record_id,
        )
        veh_no = str_or_none(raw.get("veh_no"))
        person = CollisionPerson(
            provenance=self._build_provenance(
                source_record_id=record.source_record_id, snapshot=snapshot
            ),
            collision_id=collision_id,
            person_id=f"{collision_id}:{per_no}",
            vehicle_id=_vehicle_id(collision_id, veh_no) if veh_no is not None else None,
            role=_map_person_role(raw),
            injury_outcome=_map_person_injury(raw),
            age=_map_age(raw),
            safety_equipment=_map_source_category(raw, "safequip"),
            position_in_vehicle=MappedField[CategoryRef](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            ejection=MappedField[CategoryRef](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            contributing_factors=_map_person_factors(raw),
        )
        report = MappingReport(
            unmapped_source_fields=tuple(
                sorted(name for name in raw if name not in _MAPPER_CONSUMED_FIELDS)
            )
        )

        return MapResult[CollisionPerson](record=person, report=report)

    def _build_provenance(
        self,
        *,
        source_record_id: str | None,
        snapshot: SourceSnapshot,
    ) -> ProvenanceRef:
        return ProvenanceRef(
            snapshot_id=snapshot.snapshot_id,
            source_id=snapshot.source_id,
            dataset_id=snapshot.dataset_id,
            jurisdiction=snapshot.jurisdiction,
            fetched_at=snapshot.fetched_at,
            mapper=self.version,
            source_record_id=source_record_id,
        )


def _map_occurred_at(choice: GroupChoice[str]) -> MappedField[OccurrenceTime]:
    if choice.value is None:
        return MappedField[OccurrenceTime](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("accdate",),
        )

    parsed = _parse_date(choice.value)

    if parsed is None:
        return MappedField[OccurrenceTime](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("accdate",),
        )

    return MappedField[OccurrenceTime](
        value=OccurrenceTime(
            precision=OccurrenceTimePrecision.DATE,
            date_value=parsed,
            timezone_status=OccurrenceTimezoneStatus.NAMED_LOCAL,
            timezone=_TORONTO_TIMEZONE,
        ),
        quality=FieldQuality.STANDARDIZED,
        source_fields=("accdate",),
    )


def _parse_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _map_collision_severity(
    choice: GroupChoice[str],
) -> GroupChoice[MappedField[CollisionSeverity]]:
    if choice.value is None:
        field = MappedField[CollisionSeverity](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("acclass",),
        )
        return GroupChoice[MappedField[CollisionSeverity]](value=field, conflicts=choice.conflicts)

    mapped = _SEVERITY_MAP.get(choice.value.casefold())

    if mapped is None:
        field = MappedField[CollisionSeverity](
            value=CollisionSeverity.UNKNOWN,
            quality=FieldQuality.INFERRED,
            source_fields=("acclass",),
        )
        return GroupChoice[MappedField[CollisionSeverity]](value=field, conflicts=choice.conflicts)

    field = MappedField[CollisionSeverity](
        value=mapped,
        quality=FieldQuality.STANDARDIZED,
        source_fields=("acclass",),
    )
    return GroupChoice[MappedField[CollisionSeverity]](value=field, conflicts=choice.conflicts)


def _map_coordinate(
    latitude: GroupChoice[float], longitude: GroupChoice[float]
) -> MappedField[Coordinate]:
    if latitude.value is None or longitude.value is None:
        return MappedField[Coordinate](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("latitude", "longitude"),
        )

    try:
        coordinate = Coordinate(latitude=latitude.value, longitude=longitude.value)
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


def _map_address(road_names: tuple[str, ...]) -> MappedField[Address]:
    if not road_names:
        return MappedField[Address](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("stname1", "stname2", "stname3"),
        )

    return MappedField[Address](
        value=Address(
            country=_COUNTRY,
            region=_REGION,
            locality=_LOCALITY,
            street=" / ".join(road_names),
        ),
        quality=FieldQuality.DERIVED,
        source_fields=("stname1", "stname2", "stname3"),
    )


def _map_road_names(road_names: tuple[str, ...]) -> MappedField[tuple[str, ...]]:
    if not road_names:
        return MappedField[tuple[str, ...]](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("stname1", "stname2", "stname3"),
        )

    return MappedField[tuple[str, ...]](
        value=road_names,
        quality=FieldQuality.DERIVED,
        source_fields=("stname1", "stname2", "stname3"),
    )


def _map_intersection_related(accloc: GroupChoice[str]) -> MappedField[bool]:
    if accloc.value is None:
        return MappedField[bool](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("accloc",),
        )

    normalized = accloc.value.strip().casefold()
    intersection_values = frozenset({"at intersection", "intersection", "intersection related"})
    non_intersection_values = frozenset(
        {"non intersection", "non-intersection", "mid-block", "private drive"}
    )

    if normalized in intersection_values:
        return MappedField[bool](
            value=True,
            quality=FieldQuality.STANDARDIZED,
            source_fields=("accloc",),
        )

    if normalized in non_intersection_values:
        return MappedField[bool](
            value=False,
            quality=FieldQuality.STANDARDIZED,
            source_fields=("accloc",),
        )

    return MappedField[bool](
        value=None,
        quality=FieldQuality.UNMAPPED,
        source_fields=(),
    )


def _map_text_field(choice: GroupChoice[str], field_name: str) -> MappedField[str]:
    if choice.value is None:
        return MappedField[str](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=(field_name,),
        )

    return MappedField[str](
        value=choice.value,
        quality=FieldQuality.DIRECT,
        source_fields=(field_name,),
    )


def _map_category_text(choice: GroupChoice[str], field_name: str) -> MappedField[CategoryRef]:
    if choice.value is None:
        return MappedField[CategoryRef](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=(field_name,),
        )

    return MappedField[CategoryRef](
        value=_category(field_name, choice.value),
        quality=FieldQuality.DERIVED,
        source_fields=(field_name,),
    )


def _map_source_category(raw: Mapping[str, Any], field_name: str) -> MappedField[CategoryRef]:
    value = str_or_none(raw.get(field_name))

    if value is None:
        return MappedField[CategoryRef](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=(field_name,),
        )

    return MappedField[CategoryRef](
        value=_category(field_name, value),
        quality=FieldQuality.DERIVED,
        source_fields=(field_name,),
    )


def _category(field_name: str, label: str) -> CategoryRef:
    return CategoryRef(
        code=slugify(label),
        label=label,
        taxonomy_id=f"toronto-ksi-{field_name.replace('_', '-')}",
        taxonomy_version=_TAXONOMY_VERSION,
    )


def _count_field(value: int, source_field: str) -> MappedField[int]:
    return MappedField[int](
        value=value,
        quality=FieldQuality.DERIVED,
        source_fields=(source_field,),
    )


def _map_person_count(per_inv: GroupChoice[int], fallback_count: int) -> MappedField[int]:
    if per_inv.value is not None:
        return MappedField[int](
            value=per_inv.value,
            quality=FieldQuality.DIRECT,
            source_fields=("per_inv",),
        )

    return MappedField[int](
        value=fallback_count,
        quality=FieldQuality.DERIVED,
        source_fields=("per_no",),
    )


def _injury_counts(records: Sequence[RawRecord]) -> Counter[InjuryOutcome]:
    counts: Counter[InjuryOutcome] = Counter()

    for record in records:
        raw = str_or_none(record.raw_data.get("injury"))
        outcome = (
            _INJURY_MAP.get(raw.casefold(), InjuryOutcome.UNKNOWN) if raw else InjuryOutcome.UNKNOWN
        )

        counts[outcome] += 1

    return counts


def _vehicle_count(records: Sequence[RawRecord]) -> int:
    return len(
        {veh_no for record in records if (veh_no := str_or_none(record.raw_data.get("veh_no")))}
    )


def _map_vehicle_category(choice: GroupChoice[str]) -> MappedField[VehicleCategory]:
    value = choice.value

    if value is None:
        return MappedField[VehicleCategory](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("vehtype",),
        )

    normalized = value.casefold()

    if "bicycle" in normalized:
        category = VehicleCategory.BICYCLE
    elif "motorcycle" in normalized:
        category = VehicleCategory.MOTORCYCLE
    elif "truck" in normalized:
        category = VehicleCategory.TRUCK
    elif "bus" in normalized:
        category = VehicleCategory.BUS
    elif "automobile" in normalized or "station wagon" in normalized or "passenger" in normalized:
        category = VehicleCategory.PASSENGER_CAR
    else:
        category = VehicleCategory.UNKNOWN

    return MappedField[VehicleCategory](
        value=category,
        quality=FieldQuality.STANDARDIZED
        if category is not VehicleCategory.UNKNOWN
        else FieldQuality.INFERRED,
        source_fields=("vehtype",),
    )


def _map_vehicle_role(choice: GroupChoice[str]) -> MappedField[RoadUserRole]:
    value = choice.value

    if value is None:
        return MappedField[RoadUserRole](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("vehtype",),
        )

    normalized = value.casefold()

    if "bicycle" in normalized:
        role = RoadUserRole.CYCLIST
    elif "motorcycle" in normalized:
        role = RoadUserRole.MOTORCYCLIST
    else:
        role = RoadUserRole.UNKNOWN

    return MappedField[RoadUserRole](
        value=role,
        quality=FieldQuality.INFERRED,
        source_fields=("vehtype",),
    )


def _map_person_role(raw: Mapping[str, Any]) -> MappedField[RoadUserRole]:
    value = str_or_none(raw.get("road_user"))

    if value is None:
        return MappedField[RoadUserRole](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("road_user",),
        )

    normalized = value.casefold()
    role = RoadUserRole.UNKNOWN

    if "driver" in normalized:
        role = RoadUserRole.DRIVER
    elif "passenger" in normalized:
        role = RoadUserRole.PASSENGER
    elif "pedestrian" in normalized:
        role = RoadUserRole.PEDESTRIAN
    elif "cyclist" in normalized:
        role = RoadUserRole.CYCLIST
    elif "motorcyclist" in normalized or "motorcycle" in normalized:
        role = RoadUserRole.MOTORCYCLIST

    return MappedField[RoadUserRole](
        value=role,
        quality=FieldQuality.STANDARDIZED
        if role is not RoadUserRole.UNKNOWN
        else FieldQuality.INFERRED,
        source_fields=("road_user",),
    )


def _map_person_injury(raw: Mapping[str, Any]) -> MappedField[InjuryOutcome]:
    value = str_or_none(raw.get("injury"))

    if value is None:
        return MappedField[InjuryOutcome](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("injury",),
        )

    mapped = _INJURY_MAP.get(value.casefold(), InjuryOutcome.UNKNOWN)

    return MappedField[InjuryOutcome](
        value=mapped,
        quality=FieldQuality.STANDARDIZED
        if mapped is not InjuryOutcome.UNKNOWN
        else FieldQuality.INFERRED,
        source_fields=("injury",),
    )


def _map_age(raw: Mapping[str, Any]) -> MappedField[int]:
    parsed = int_or_none(raw.get("invage"))

    if parsed is not None:
        return MappedField[int](
            value=parsed,
            quality=FieldQuality.STANDARDIZED,
            source_fields=("invage",),
        )

    if str_or_none(raw.get("invage")) is None:
        return MappedField[int](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("invage",),
        )

    return MappedField[int](value=None, quality=FieldQuality.UNMAPPED, source_fields=())


def _map_person_factors(raw: Mapping[str, Any]) -> MappedField[tuple[ContributingFactor, ...]]:
    factors: list[ContributingFactor] = []

    for field_name in ("drivact", "drivcond", "pedact", "pedcond", "cycact", "cyccond"):
        value = str_or_none(raw.get(field_name))

        if value is not None:
            factors.append(
                ContributingFactor(
                    raw_label=value,
                    rank=len(factors) + 1,
                    category=_category(field_name, value),
                )
            )

    if not factors:
        return MappedField[tuple[ContributingFactor, ...]](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("drivact", "drivcond", "pedact", "pedcond", "cycact", "cyccond"),
        )

    return MappedField[tuple[ContributingFactor, ...]](
        value=tuple(factors),
        quality=FieldQuality.DERIVED,
        source_fields=("drivact", "drivcond", "pedact", "pedcond", "cycact", "cyccond"),
    )


def _vehicle_id(collision_id: str, veh_no: str) -> str:
    return f"{collision_id}:{veh_no}"


def _unmapped_source_fields(
    records: Sequence[RawRecord], consumed: frozenset[str]
) -> tuple[str, ...]:
    return tuple(
        sorted({name for record in records for name in record.raw_data if name not in consumed})
    )
