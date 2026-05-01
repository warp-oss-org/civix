"""Chicago Traffic Crashes - Vehicles mapper."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final

from civix.core.identity.models.identifiers import MapperId
from civix.core.mapping.errors import MappingError
from civix.core.mapping.models.mapper import MappingReport, MapResult
from civix.core.mapping.parsers import int_or_none, require_text, slugify, str_or_none
from civix.core.provenance.models.provenance import MapperVersion, ProvenanceRef
from civix.core.quality.models.fields import FieldQuality, MappedField
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.taxonomy.models.category import CategoryRef
from civix.domains.transportation_safety.models.parties import ContributingFactor, RoadUserRole
from civix.domains.transportation_safety.models.vehicle import CollisionVehicle, VehicleCategory

MAPPER_ID: Final[MapperId] = MapperId("chicago-traffic-vehicles")
MAPPER_VERSION: Final[str] = "0.1.0"

_TAXONOMY_VERSION: Final[str] = "2026-05-01"
MAPPER_CONSUMED_FIELDS: Final[frozenset[str]] = frozenset(
    {"crash_record_id", "crash_unit_id", "vehicle_id"}
)
_PASSENGER_CAR_VALUES: Final[frozenset[str]] = frozenset(
    {"passenger", "passenger car", "sport utility vehicle (suv)"}
)
_TRUCK_VALUES: Final[frozenset[str]] = frozenset(
    {"pickup", "truck - single unit", "tractor w/ semi-trailer", "truck"}
)
_BUS_VALUES: Final[frozenset[str]] = frozenset({"bus over 15 pass.", "bus up to 15 pass."})
_MOTORCYCLE_VALUES: Final[frozenset[str]] = frozenset({"motorcycle", "moped or motorized bike"})
_EMERGENCY_VALUES: Final[frozenset[str]] = frozenset({"ambulance", "fire", "police"})


@dataclass(frozen=True, slots=True)
class ChicagoVehiclesMapper:
    """Maps Chicago crash unit rows to `CollisionVehicle`."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=MAPPER_ID, version=MAPPER_VERSION)

    def __call__(self, record: RawRecord, snapshot: SourceSnapshot) -> MapResult[CollisionVehicle]:
        raw = record.raw_data
        provenance = self._build_provenance(record=record, snapshot=snapshot)

        vehicle = CollisionVehicle(
            provenance=provenance,
            collision_id=require_text(
                raw.get("crash_record_id"),
                field_name="crash_record_id",
                mapper=self.version,
                source_record_id=record.source_record_id,
            ),
            vehicle_id=_map_vehicle_id(raw, mapper=self.version, record=record),
            category=_map_category(raw),
            road_user_role=_map_road_user_role(raw),
            occupant_count=_map_count(raw, "occupant_cnt"),
            travel_direction=_map_source_category(raw, "travel_direction"),
            maneuver=_map_source_category(raw, "maneuver"),
            damage=_map_source_category(raw, "first_contact_point"),
            # Chicago publishes contributing factors on the crash row, not unit rows.
            contributing_factors=MappedField[tuple[ContributingFactor, ...]](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
        )
        report = MappingReport(unmapped_source_fields=_unmapped_source_fields(raw, vehicle))

        return MapResult[CollisionVehicle](record=vehicle, report=report)

    def _build_provenance(self, *, record: RawRecord, snapshot: SourceSnapshot) -> ProvenanceRef:
        return ProvenanceRef(
            snapshot_id=snapshot.snapshot_id,
            source_id=snapshot.source_id,
            dataset_id=snapshot.dataset_id,
            jurisdiction=snapshot.jurisdiction,
            fetched_at=snapshot.fetched_at,
            mapper=self.version,
            source_record_id=record.source_record_id,
        )


def _map_vehicle_id(
    raw: Mapping[str, Any],
    *,
    mapper: MapperVersion,
    record: RawRecord,
) -> str:
    vehicle_id = str_or_none(raw.get("vehicle_id"))

    if vehicle_id is not None:
        return vehicle_id

    crash_unit_id = str_or_none(raw.get("crash_unit_id"))

    if crash_unit_id is not None:
        return crash_unit_id

    raise MappingError(
        "missing required vehicle identifier",
        mapper=mapper,
        source_record_id=record.source_record_id,
        source_fields=("vehicle_id", "crash_unit_id"),
    )


def _map_category(raw: Mapping[str, Any]) -> MappedField[VehicleCategory]:
    unit_type = str_or_none(raw.get("unit_type"))
    vehicle_type = str_or_none(raw.get("vehicle_type"))

    if unit_type is None and vehicle_type is None:
        return MappedField[VehicleCategory](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("unit_type", "vehicle_type"),
        )

    normalized_unit = unit_type.casefold() if unit_type is not None else ""
    normalized_vehicle = vehicle_type.casefold() if vehicle_type is not None else ""

    if normalized_unit == "pedestrian":
        category = VehicleCategory.PEDESTRIAN_UNIT
    elif normalized_unit == "bicycle" or normalized_vehicle == "bicycle":
        category = VehicleCategory.BICYCLE
    elif normalized_vehicle in _MOTORCYCLE_VALUES:
        category = VehicleCategory.MOTORCYCLE
    elif normalized_vehicle in _BUS_VALUES:
        category = VehicleCategory.BUS
    elif normalized_vehicle in _TRUCK_VALUES:
        category = VehicleCategory.TRUCK
    elif normalized_vehicle in _EMERGENCY_VALUES:
        category = VehicleCategory.EMERGENCY_VEHICLE
    elif normalized_vehicle in _PASSENGER_CAR_VALUES:
        category = VehicleCategory.PASSENGER_CAR
    elif "unknown" in {normalized_unit, normalized_vehicle}:
        category = VehicleCategory.UNKNOWN
    else:
        category = VehicleCategory.OTHER

    return MappedField[VehicleCategory](
        value=category,
        quality=FieldQuality.STANDARDIZED,
        source_fields=("unit_type", "vehicle_type"),
    )


def _map_road_user_role(raw: Mapping[str, Any]) -> MappedField[RoadUserRole]:
    unit_type = str_or_none(raw.get("unit_type"))

    if unit_type is None:
        return MappedField[RoadUserRole](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("unit_type",),
        )

    normalized = unit_type.casefold()

    if normalized == "pedestrian":
        role = RoadUserRole.PEDESTRIAN
    elif normalized == "bicycle":
        role = RoadUserRole.CYCLIST
    elif normalized == "driver":
        role = RoadUserRole.DRIVER
    elif normalized in {"parked", "driverless"}:
        role = RoadUserRole.OTHER
    elif normalized == "unknown":
        role = RoadUserRole.UNKNOWN
    else:
        role = RoadUserRole.OTHER

    return MappedField[RoadUserRole](
        value=role,
        quality=FieldQuality.STANDARDIZED,
        source_fields=("unit_type",),
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


def _map_source_category(raw: Mapping[str, Any], field_name: str) -> MappedField[CategoryRef]:
    label = str_or_none(raw.get(field_name))

    if label is None:
        return MappedField[CategoryRef](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=(field_name,),
        )

    return MappedField[CategoryRef](
        value=_category(field_name=field_name, label=label),
        quality=FieldQuality.DERIVED,
        source_fields=(field_name,),
    )


def _unmapped_source_fields(raw: Mapping[str, Any], vehicle: CollisionVehicle) -> tuple[str, ...]:
    consumed: set[str] = set(MAPPER_CONSUMED_FIELDS)

    for field_name in vehicle.__class__.model_fields:
        attr = getattr(vehicle, field_name)

        if isinstance(attr, MappedField):
            consumed.update(attr.source_fields)

    return tuple(sorted(name for name in raw if name not in consumed))


def _category(*, field_name: str, label: str) -> CategoryRef:
    return CategoryRef(
        code=slugify(label),
        label=label,
        taxonomy_id=f"chicago-traffic-vehicles-{field_name.replace('_', '-')}",
        taxonomy_version=_TAXONOMY_VERSION,
    )
