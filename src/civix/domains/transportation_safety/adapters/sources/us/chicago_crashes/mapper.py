"""Chicago Traffic Crashes - Crashes mapper."""

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
from civix.domains.transportation_safety.models.road import SpeedLimit, SpeedLimitUnit
from civix.domains.transportation_safety.models.time import (
    OccurrenceTime,
    OccurrenceTimePrecision,
    OccurrenceTimezoneStatus,
)

MAPPER_ID: Final[MapperId] = MapperId("chicago-traffic-crashes")
MAPPER_VERSION: Final[str] = "0.1.0"

_TAXONOMY_VERSION: Final[str] = "2026-05-01"
_CHICAGO_TIMEZONE: Final[str] = "America/Chicago"
_INFERRED_LOCALITY: Final[str] = "Chicago"
_JURISDICTION_LOCALITY_FIELD: Final[str] = "snapshot.jurisdiction.locality"
MAPPER_CONSUMED_FIELDS: Final[frozenset[str]] = frozenset({"crash_record_id"})
_ADDRESS_SOURCE_FIELDS: Final[tuple[str, ...]] = (
    "street_no",
    "street_direction",
    "street_name",
)
_COORDINATE_SOURCE_FIELDS: Final[tuple[str, ...]] = ("latitude", "longitude")
_CONTRIBUTING_FACTOR_FIELDS: Final[tuple[str, ...]] = (
    "prim_contributory_cause",
    "sec_contributory_cause",
)

_SEVERITY_MAP: Final[dict[str, CollisionSeverity]] = {
    "fatal": CollisionSeverity.FATAL,
    "incapacitating injury": CollisionSeverity.SERIOUS_INJURY,
    "nonincapacitating injury": CollisionSeverity.MINOR_INJURY,
    "reported, not evident": CollisionSeverity.POSSIBLE_INJURY,
    "no indication of injury": CollisionSeverity.PROPERTY_DAMAGE_ONLY,
}


@dataclass(frozen=True, slots=True)
class ChicagoCrashesMapper:
    """Maps Chicago crash event rows to `TrafficCollision`."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=MAPPER_ID, version=MAPPER_VERSION)

    def __call__(self, record: RawRecord, snapshot: SourceSnapshot) -> MapResult[TrafficCollision]:
        raw = record.raw_data
        provenance = self._build_provenance(record=record, snapshot=snapshot)

        collision = TrafficCollision(
            provenance=provenance,
            collision_id=require_text(
                raw.get("crash_record_id"),
                field_name="crash_record_id",
                mapper=self.version,
                source_record_id=record.source_record_id,
            ),
            occurred_at=_map_occurred_at(raw),
            severity=_map_severity(raw),
            address=_map_address(raw, snapshot.jurisdiction.country),
            coordinate=_map_coordinate(raw),
            locality=MappedField[str](
                value=_INFERRED_LOCALITY,
                quality=FieldQuality.INFERRED,
                source_fields=(_JURISDICTION_LOCALITY_FIELD,),
            ),
            road_names=_map_road_names(raw),
            intersection_related=_map_yes_no(raw, "intersection_related_i"),
            location_description=_map_location_description(raw),
            weather=_map_category(raw, "weather_condition"),
            lighting=_map_category(raw, "lighting_condition"),
            road_surface=_map_category(raw, "roadway_surface_cond"),
            road_condition=_map_category(raw, "road_defect"),
            traffic_control=_map_category(raw, "traffic_control_device"),
            speed_limit=_map_speed_limit(raw),
            fatal_count=_map_count(raw, "injuries_fatal"),
            serious_injury_count=_map_count(raw, "injuries_incapacitating"),
            minor_injury_count=_map_count(raw, "injuries_non_incapacitating"),
            possible_injury_count=_map_count(raw, "injuries_reported_not_evident"),
            uninjured_count=_map_count(raw, "injuries_no_indication"),
            unknown_injury_count=_map_count(raw, "injuries_unknown"),
            total_injured_count=_map_count(raw, "injuries_total"),
            vehicle_count=_map_count(raw, "num_units"),
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
    parsed = _parse_datetime(raw.get("crash_date"))

    if parsed is None:
        return MappedField[OccurrenceTime](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("crash_date",),
        )

    return MappedField[OccurrenceTime](
        value=OccurrenceTime(
            precision=OccurrenceTimePrecision.DATETIME,
            datetime_value=parsed,
            timezone_status=OccurrenceTimezoneStatus.NAMED_LOCAL,
            timezone=_CHICAGO_TIMEZONE,
        ),
        quality=FieldQuality.STANDARDIZED,
        source_fields=("crash_date",),
    )


def _map_severity(raw: Mapping[str, Any]) -> MappedField[CollisionSeverity]:
    value = str_or_none(raw.get("most_severe_injury"))

    if value is None:
        return MappedField[CollisionSeverity](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("most_severe_injury",),
        )

    mapped = _SEVERITY_MAP.get(value.casefold())

    if mapped is None:
        return MappedField[CollisionSeverity](
            value=CollisionSeverity.UNKNOWN,
            quality=FieldQuality.INFERRED,
            source_fields=("most_severe_injury",),
        )

    return MappedField[CollisionSeverity](
        value=mapped,
        quality=FieldQuality.STANDARDIZED,
        source_fields=("most_severe_injury",),
    )


def _map_address(raw: Mapping[str, Any], country: str) -> MappedField[Address]:
    street = _street(raw)

    if street is None:
        return MappedField[Address](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=_ADDRESS_SOURCE_FIELDS,
        )

    return MappedField[Address](
        value=Address(
            country=country,
            street=street,
        ),
        quality=FieldQuality.DERIVED,
        source_fields=_ADDRESS_SOURCE_FIELDS,
    )


def _map_coordinate(raw: Mapping[str, Any]) -> MappedField[Coordinate]:
    latitude = float_or_none(raw.get("latitude"))
    longitude = float_or_none(raw.get("longitude"))

    if latitude is None or longitude is None:
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
    street_name = str_or_none(raw.get("street_name"))

    if street_name is None:
        return MappedField[tuple[str, ...]](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("street_name",),
        )

    return MappedField[tuple[str, ...]](
        value=(street_name,),
        quality=FieldQuality.DIRECT,
        source_fields=("street_name",),
    )


def _map_yes_no(raw: Mapping[str, Any], field_name: str) -> MappedField[bool]:
    value = str_or_none(raw.get(field_name))

    if value is None:
        return MappedField[bool](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=(field_name,),
        )

    normalized = value.casefold()

    if normalized == "y":
        return MappedField[bool](
            value=True,
            quality=FieldQuality.STANDARDIZED,
            source_fields=(field_name,),
        )

    if normalized == "n":
        return MappedField[bool](
            value=False,
            quality=FieldQuality.STANDARDIZED,
            source_fields=(field_name,),
        )

    return MappedField[bool](
        value=None,
        quality=FieldQuality.NOT_PROVIDED,
        source_fields=(field_name,),
    )


def _map_location_description(raw: Mapping[str, Any]) -> MappedField[str]:
    street = _street(raw)

    if street is None:
        return MappedField[str](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=_ADDRESS_SOURCE_FIELDS,
        )

    return MappedField[str](
        value=street,
        quality=FieldQuality.DERIVED,
        source_fields=_ADDRESS_SOURCE_FIELDS,
    )


def _map_category(raw: Mapping[str, Any], field_name: str) -> MappedField[CategoryRef]:
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


def _map_speed_limit(raw: Mapping[str, Any]) -> MappedField[SpeedLimit]:
    value = int_or_none(raw.get("posted_speed_limit"))

    if value is None or value < 0:
        return MappedField[SpeedLimit](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("posted_speed_limit",),
        )

    return MappedField[SpeedLimit](
        value=SpeedLimit(value=value, unit=SpeedLimitUnit.MILES_PER_HOUR),
        quality=FieldQuality.STANDARDIZED,
        source_fields=("posted_speed_limit",),
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


def _street(raw: Mapping[str, Any]) -> str | None:
    street_no = str_or_none(raw.get("street_no"))
    direction = str_or_none(raw.get("street_direction"))
    street_name = str_or_none(raw.get("street_name"))
    parts = [part for part in (street_no, direction, street_name) if part is not None]

    if not parts:
        return None

    return " ".join(parts)


def _category(*, field_name: str, label: str) -> CategoryRef:
    return CategoryRef(
        code=slugify(label),
        label=label,
        taxonomy_id=f"chicago-traffic-crashes-{field_name.replace('_', '-')}",
        taxonomy_version=_TAXONOMY_VERSION,
    )


def _parse_datetime(value: object) -> datetime | None:
    text = str_or_none(value)

    if text is None:
        return None

    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None
