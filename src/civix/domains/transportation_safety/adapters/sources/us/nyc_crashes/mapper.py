"""NYC Motor Vehicle Collisions - Crashes mapper."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final

from pydantic import ValidationError

from civix.core.identity.models.identifiers import MapperId
from civix.core.mapping.models.mapper import MappingReport, MapResult
from civix.core.mapping.parsers import (
    float_or_none,
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
from civix.domains.transportation_safety.models.collision import (
    CollisionSeverity,
    TrafficCollision,
)
from civix.domains.transportation_safety.models.parties import ContributingFactor
from civix.domains.transportation_safety.models.road import SpeedLimit
from civix.domains.transportation_safety.models.time import (
    OccurrenceTime,
    OccurrenceTimePrecision,
    OccurrenceTimezoneStatus,
)

MAPPER_ID: Final[MapperId] = MapperId("nyc-motor-vehicle-collisions-crashes")
MAPPER_VERSION: Final[str] = "0.1.0"

_TAXONOMY_VERSION: Final[str] = "2026-05-02"
_NYC_TIMEZONE: Final[str] = "America/New_York"
_INFERRED_LOCALITY: Final[str] = "New York City"
_JURISDICTION_LOCALITY_FIELD: Final[str] = "snapshot.jurisdiction.locality"
MAPPER_CONSUMED_FIELDS: Final[frozenset[str]] = frozenset({"collision_id"})
_COORDINATE_SOURCE_FIELDS: Final[tuple[str, ...]] = ("latitude", "longitude")
_ADDRESS_SOURCE_FIELDS: Final[tuple[str, ...]] = (
    "borough",
    "zip_code",
    "on_street_name",
    "off_street_name",
)
_ROAD_NAME_FIELDS: Final[tuple[str, ...]] = (
    "on_street_name",
    "off_street_name",
    "cross_street_name",
)
_CONTRIBUTING_FACTOR_FIELDS: Final[tuple[str, ...]] = (
    "contributing_factor_vehicle_1",
    "contributing_factor_vehicle_2",
    "contributing_factor_vehicle_3",
    "contributing_factor_vehicle_4",
    "contributing_factor_vehicle_5",
)


@dataclass(frozen=True, slots=True)
class NycCrashesMapper:
    """Maps NYC crash event rows to `TrafficCollision`."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=MAPPER_ID, version=MAPPER_VERSION)

    def __call__(self, record: RawRecord, snapshot: SourceSnapshot) -> MapResult[TrafficCollision]:
        raw = record.raw_data
        provenance = self._build_provenance(record=record, snapshot=snapshot)

        collision = TrafficCollision(
            provenance=provenance,
            collision_id=require_text(
                raw.get("collision_id"),
                field_name="collision_id",
                mapper=self.version,
                source_record_id=record.source_record_id,
            ),
            occurred_at=_map_occurred_at(raw),
            severity=_map_severity(raw),
            address=_map_address(raw, snapshot.jurisdiction.country, snapshot.jurisdiction.region),
            coordinate=_map_coordinate(raw),
            locality=MappedField[str](
                value=_INFERRED_LOCALITY,
                quality=FieldQuality.INFERRED,
                source_fields=(_JURISDICTION_LOCALITY_FIELD,),
            ),
            road_names=_map_road_names(raw),
            intersection_related=_map_intersection_related(raw),
            location_description=_map_location_description(raw),
            weather=MappedField[CategoryRef](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            lighting=MappedField[CategoryRef](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
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
            fatal_count=_map_count(raw, "number_of_persons_killed"),
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
            total_injured_count=_map_count(raw, "number_of_persons_injured"),
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
            contributing_factors=_map_contributing_factors(raw),
        )
        report = MappingReport(unmapped_source_fields=_unmapped_source_fields(raw, collision))

        return MapResult[TrafficCollision](record=collision, report=report)

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


def _map_occurred_at(raw: Mapping[str, Any]) -> MappedField[OccurrenceTime]:
    parsed = _parse_datetime(raw.get("crash_date"), raw.get("crash_time"))

    if parsed is None:
        return MappedField[OccurrenceTime](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("crash_date", "crash_time"),
        )

    return MappedField[OccurrenceTime](
        value=OccurrenceTime(
            precision=OccurrenceTimePrecision.DATETIME,
            datetime_value=parsed,
            timezone_status=OccurrenceTimezoneStatus.NAMED_LOCAL,
            timezone=_NYC_TIMEZONE,
        ),
        quality=FieldQuality.STANDARDIZED,
        source_fields=("crash_date", "crash_time"),
    )


def _map_severity(raw: Mapping[str, Any]) -> MappedField[CollisionSeverity]:
    killed = int_or_none(raw.get("number_of_persons_killed"))
    injured = int_or_none(raw.get("number_of_persons_injured"))

    if killed is not None and killed > 0:
        severity = CollisionSeverity.FATAL
    elif injured is not None and injured > 0:
        severity = CollisionSeverity.POSSIBLE_INJURY
    elif killed == 0 and injured == 0:
        severity = CollisionSeverity.PROPERTY_DAMAGE_ONLY
    else:
        severity = CollisionSeverity.UNKNOWN

    return MappedField[CollisionSeverity](
        value=severity,
        quality=FieldQuality.DERIVED,
        source_fields=("number_of_persons_killed", "number_of_persons_injured"),
    )


def _map_address(raw: Mapping[str, Any], country: str, region: str | None) -> MappedField[Address]:
    on_street = str_or_none(raw.get("on_street_name"))
    off_street_address = str_or_none(raw.get("off_street_name"))
    street = off_street_address if off_street_address is not None else on_street
    postal_code = str_or_none(raw.get("zip_code"))
    borough = str_or_none(raw.get("borough"))

    if street is None and postal_code is None and borough is None:
        return MappedField[Address](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=_ADDRESS_SOURCE_FIELDS,
        )

    return MappedField[Address](
        value=Address(
            country=country,
            region=region,
            locality=borough,
            street=street,
            postal_code=postal_code,
        ),
        quality=FieldQuality.DERIVED,
        source_fields=_ADDRESS_SOURCE_FIELDS,
    )


def _map_coordinate(raw: Mapping[str, Any]) -> MappedField[Coordinate]:
    latitude = float_or_none(raw.get("latitude"))
    longitude = float_or_none(raw.get("longitude"))

    if latitude is None or longitude is None or (latitude == 0 and longitude == 0):
        return MappedField[Coordinate](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=_COORDINATE_SOURCE_FIELDS,
        )

    try:
        coordinate = Coordinate(latitude=latitude, longitude=longitude)
    except ValidationError:
        return MappedField[Coordinate](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=_COORDINATE_SOURCE_FIELDS,
        )

    return MappedField[Coordinate](
        value=coordinate,
        quality=FieldQuality.STANDARDIZED,
        source_fields=_COORDINATE_SOURCE_FIELDS,
    )


def _map_road_names(raw: Mapping[str, Any]) -> MappedField[tuple[str, ...]]:
    values = tuple(value for field in _ROAD_NAME_FIELDS if (value := str_or_none(raw.get(field))))

    if not values:
        return MappedField[tuple[str, ...]](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=_ROAD_NAME_FIELDS,
        )

    return MappedField[tuple[str, ...]](
        value=values,
        quality=FieldQuality.DIRECT,
        source_fields=_ROAD_NAME_FIELDS,
    )


def _map_intersection_related(raw: Mapping[str, Any]) -> MappedField[bool]:
    on_street = str_or_none(raw.get("on_street_name"))
    cross_street = str_or_none(raw.get("cross_street_name"))
    off_street_address = str_or_none(raw.get("off_street_name"))

    if on_street is not None and cross_street is not None:
        value = True
    elif off_street_address is not None:
        value = False
    else:
        value = None

    return MappedField[bool](
        value=value,
        quality=FieldQuality.DERIVED if value is not None else FieldQuality.NOT_PROVIDED,
        source_fields=_ROAD_NAME_FIELDS,
    )


def _map_location_description(raw: Mapping[str, Any]) -> MappedField[str]:
    parts = tuple(value for field in _ROAD_NAME_FIELDS if (value := str_or_none(raw.get(field))))

    if not parts:
        return MappedField[str](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=_ROAD_NAME_FIELDS,
        )

    return MappedField[str](
        value=" / ".join(parts),
        quality=FieldQuality.DERIVED,
        source_fields=_ROAD_NAME_FIELDS,
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


def _unmapped_source_fields(raw: Mapping[str, Any], collision: TrafficCollision) -> tuple[str, ...]:
    consumed: set[str] = set()

    for field_name in collision.__class__.model_fields:
        attr = getattr(collision, field_name)

        if isinstance(attr, MappedField):
            consumed.update(attr.source_fields)

    consumed |= MAPPER_CONSUMED_FIELDS

    return tuple(sorted(name for name in raw if name not in consumed))


def _category(*, field_name: str, label: str) -> CategoryRef:
    return CategoryRef(
        code=slugify(label),
        label=label,
        taxonomy_id=f"nyc-crashes-{field_name.replace('_', '-')}",
        taxonomy_version=_TAXONOMY_VERSION,
    )


def _parse_datetime(date_value: object, time_value: object) -> datetime | None:
    date_text = str_or_none(date_value)
    time_text = str_or_none(time_value)

    if date_text is None or time_text is None:
        return None

    try:
        date_part = datetime.fromisoformat(date_text).date()
        time_part = datetime.strptime(time_text, "%H:%M").time()
    except ValueError:
        return None

    return datetime.combine(date_part, time_part)
