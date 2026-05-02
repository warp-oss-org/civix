"""NYC Motor Vehicle Collisions - Vehicles mapper."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final

from civix.core.identity.models.identifiers import MapperId
from civix.core.mapping.models.mapper import MappingReport, MapResult
from civix.core.mapping.parsers import int_or_none, require_text, slugify, str_or_none
from civix.core.provenance.models.provenance import MapperVersion, ProvenanceRef
from civix.core.quality.models.fields import FieldQuality, MappedField
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.taxonomy.models.category import CategoryRef
from civix.domains.transportation_safety.models.parties import ContributingFactor, RoadUserRole
from civix.domains.transportation_safety.models.vehicle import CollisionVehicle, VehicleCategory

MAPPER_ID: Final[MapperId] = MapperId("nyc-motor-vehicle-collisions-vehicles")
MAPPER_VERSION: Final[str] = "0.1.0"

_TAXONOMY_VERSION: Final[str] = "2026-05-02"
MAPPER_CONSUMED_FIELDS: Final[frozenset[str]] = frozenset(
    {"unique_id", "collision_id", "vehicle_id"}
)
_CONTRIBUTING_FACTOR_FIELDS: Final[tuple[str, ...]] = (
    "contributing_factor_1",
    "contributing_factor_2",
)
_PASSENGER_CAR_VALUES: Final[frozenset[str]] = frozenset(
    {
        "4 dr sedan",
        "passenger vehicle",
        "sedan",
        "sport utility / station wagon",
        "station wagon/sport utility vehicle",
        "taxi",
    }
)
_TRUCK_VALUES: Final[frozenset[str]] = frozenset(
    {
        "box truck",
        "large com veh(6 or more tires)",
        "pick-up truck",
        "small com veh(4 tires)",
        "tractor truck diesel",
        "van",
    }
)
_BUS_VALUES: Final[frozenset[str]] = frozenset({"bus"})
_MOTORCYCLE_VALUES: Final[frozenset[str]] = frozenset({"motorcycle"})
_BICYCLE_VALUES: Final[frozenset[str]] = frozenset({"bicycle", "bike"})
_MICROMOBILITY_VALUES: Final[frozenset[str]] = frozenset({"e-bike", "e-scooter"})


@dataclass(frozen=True, slots=True)
class NycVehiclesMapper:
    """Maps NYC vehicle rows to `CollisionVehicle`."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=MAPPER_ID, version=MAPPER_VERSION)

    def __call__(self, record: RawRecord, snapshot: SourceSnapshot) -> MapResult[CollisionVehicle]:
        raw = record.raw_data
        provenance = self._build_provenance(record=record, snapshot=snapshot)

        vehicle = CollisionVehicle(
            provenance=provenance,
            collision_id=require_text(
                raw.get("collision_id"),
                field_name="collision_id",
                mapper=self.version,
                source_record_id=record.source_record_id,
            ),
            vehicle_id=require_text(
                raw.get("vehicle_id"),
                field_name="vehicle_id",
                mapper=self.version,
                source_record_id=record.source_record_id,
            ),
            category=_map_category(raw),
            road_user_role=_map_road_user_role(raw),
            occupant_count=_map_occupant_count(raw),
            travel_direction=_map_source_category(raw, "travel_direction"),
            maneuver=_map_source_category(raw, "pre_crash"),
            damage=_map_damage(raw),
            contributing_factors=_map_contributing_factors(raw),
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


def _map_category(raw: Mapping[str, Any]) -> MappedField[VehicleCategory]:
    value = str_or_none(raw.get("vehicle_type"))

    if value is None:
        return MappedField[VehicleCategory](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("vehicle_type",),
        )

    normalized = value.casefold()

    if normalized in _BICYCLE_VALUES:
        category = VehicleCategory.BICYCLE
    elif normalized in _MICROMOBILITY_VALUES:
        category = VehicleCategory.MICROMOBILITY
    elif normalized in _MOTORCYCLE_VALUES:
        category = VehicleCategory.MOTORCYCLE
    elif normalized in _BUS_VALUES:
        category = VehicleCategory.BUS
    elif normalized in _TRUCK_VALUES:
        category = VehicleCategory.TRUCK
    elif normalized in _PASSENGER_CAR_VALUES:
        category = VehicleCategory.PASSENGER_CAR
    elif normalized == "unknown":
        category = VehicleCategory.UNKNOWN
    else:
        category = VehicleCategory.OTHER

    return MappedField[VehicleCategory](
        value=category,
        quality=FieldQuality.STANDARDIZED,
        source_fields=("vehicle_type",),
    )


def _map_road_user_role(raw: Mapping[str, Any]) -> MappedField[RoadUserRole]:
    value = str_or_none(raw.get("vehicle_type"))

    if value is None:
        return MappedField[RoadUserRole](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("vehicle_type",),
        )

    normalized = value.casefold()

    if normalized in _BICYCLE_VALUES:
        role = RoadUserRole.CYCLIST
    elif normalized in _MOTORCYCLE_VALUES:
        role = RoadUserRole.MOTORCYCLIST
    elif normalized in _MICROMOBILITY_VALUES:
        role = RoadUserRole.OTHER
    elif normalized == "unknown":
        role = RoadUserRole.UNKNOWN
    else:
        role = RoadUserRole.DRIVER

    return MappedField[RoadUserRole](
        value=role,
        quality=FieldQuality.STANDARDIZED,
        source_fields=("vehicle_type",),
    )


def _map_occupant_count(raw: Mapping[str, Any]) -> MappedField[int]:
    value = int_or_none(raw.get("vehicle_occupants"))

    if value is None or value < 0 or value > 100:
        return MappedField[int](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("vehicle_occupants",),
        )

    return MappedField[int](
        value=value,
        quality=FieldQuality.STANDARDIZED,
        source_fields=("vehicle_occupants",),
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


def _map_damage(raw: Mapping[str, Any]) -> MappedField[CategoryRef]:
    label = str_or_none(raw.get("vehicle_damage"))

    if label is None:
        return MappedField[CategoryRef](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("vehicle_damage",),
        )

    return MappedField[CategoryRef](
        value=_category(field_name="vehicle_damage", label=label),
        quality=FieldQuality.DERIVED,
        source_fields=("vehicle_damage",),
    )


def _map_contributing_factors(
    raw: Mapping[str, Any],
) -> MappedField[tuple[ContributingFactor, ...]]:
    factors: list[ContributingFactor] = []

    for rank, field_name in enumerate(_CONTRIBUTING_FACTOR_FIELDS, start=1):
        label = str_or_none(raw.get(field_name))

        if label is None:
            continue

        factors.append(
            ContributingFactor(
                raw_label=label,
                rank=rank,
                category=_category(field_name=field_name, label=label),
            )
        )

    if not factors:
        return MappedField[tuple[ContributingFactor, ...]](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=_CONTRIBUTING_FACTOR_FIELDS,
        )

    return MappedField[tuple[ContributingFactor, ...]](
        value=tuple(factors),
        quality=FieldQuality.DERIVED,
        source_fields=_CONTRIBUTING_FACTOR_FIELDS,
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
        taxonomy_id=f"nyc-vehicles-{field_name.replace('_', '-')}",
        taxonomy_version=_TAXONOMY_VERSION,
    )
