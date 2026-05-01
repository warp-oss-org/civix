"""Chicago Traffic Crashes - People mapper."""

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
from civix.domains.transportation_safety.models.person import CollisionPerson, InjuryOutcome

MAPPER_ID: Final[MapperId] = MapperId("chicago-traffic-people")
MAPPER_VERSION: Final[str] = "0.1.0"

_TAXONOMY_VERSION: Final[str] = "2026-05-01"
MAPPER_CONSUMED_FIELDS: Final[frozenset[str]] = frozenset(
    {"crash_record_id", "person_id", "vehicle_id"}
)
_INJURY_MAP: Final[dict[str, InjuryOutcome]] = {
    "fatal": InjuryOutcome.FATAL,
    "incapacitating injury": InjuryOutcome.SERIOUS,
    "nonincapacitating injury": InjuryOutcome.MINOR,
    "reported, not evident": InjuryOutcome.POSSIBLE,
    "no indication of injury": InjuryOutcome.UNINJURED,
}


@dataclass(frozen=True, slots=True)
class ChicagoPeopleMapper:
    """Maps Chicago crash person rows to `CollisionPerson`."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=MAPPER_ID, version=MAPPER_VERSION)

    def __call__(self, record: RawRecord, snapshot: SourceSnapshot) -> MapResult[CollisionPerson]:
        raw = record.raw_data
        provenance = self._build_provenance(record=record, snapshot=snapshot)

        person = CollisionPerson(
            provenance=provenance,
            collision_id=require_text(
                raw.get("crash_record_id"),
                field_name="crash_record_id",
                mapper=self.version,
                source_record_id=record.source_record_id,
            ),
            person_id=require_text(
                raw.get("person_id"),
                field_name="person_id",
                mapper=self.version,
                source_record_id=record.source_record_id,
            ),
            vehicle_id=str_or_none(raw.get("vehicle_id")),
            role=_map_role(raw),
            injury_outcome=_map_injury(raw),
            age=_map_count(raw, "age"),
            safety_equipment=_map_source_category(raw, "safety_equipment"),
            position_in_vehicle=_map_source_category(raw, "seat_no"),
            ejection=_map_source_category(raw, "ejection"),
            # Chicago publishes contributing factors on the crash row, not person rows.
            contributing_factors=MappedField[tuple[ContributingFactor, ...]](
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
        )
        report = MappingReport(unmapped_source_fields=_unmapped_source_fields(raw, person))

        return MapResult[CollisionPerson](record=person, report=report)

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


def _map_role(raw: Mapping[str, Any]) -> MappedField[RoadUserRole]:
    value = str_or_none(raw.get("person_type"))

    if value is None:
        return MappedField[RoadUserRole](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("person_type",),
        )

    normalized = value.casefold()

    if normalized == "driver":
        role = RoadUserRole.DRIVER
    elif normalized == "passenger":
        role = RoadUserRole.PASSENGER
    elif normalized == "pedestrian":
        role = RoadUserRole.PEDESTRIAN
    elif normalized in {"bicycle", "pedalcyclist"}:
        role = RoadUserRole.CYCLIST
    elif normalized == "unknown":
        role = RoadUserRole.UNKNOWN
    else:
        role = RoadUserRole.OTHER

    return MappedField[RoadUserRole](
        value=role,
        quality=FieldQuality.STANDARDIZED,
        source_fields=("person_type",),
    )


def _map_injury(raw: Mapping[str, Any]) -> MappedField[InjuryOutcome]:
    value = str_or_none(raw.get("injury_classification"))

    if value is None:
        return MappedField[InjuryOutcome](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("injury_classification",),
        )

    mapped = _INJURY_MAP.get(value.casefold())

    if mapped is None:
        return MappedField[InjuryOutcome](
            value=InjuryOutcome.UNKNOWN,
            quality=FieldQuality.INFERRED,
            source_fields=("injury_classification",),
        )

    return MappedField[InjuryOutcome](
        value=mapped,
        quality=FieldQuality.STANDARDIZED,
        source_fields=("injury_classification",),
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


def _unmapped_source_fields(raw: Mapping[str, Any], person: CollisionPerson) -> tuple[str, ...]:
    consumed: set[str] = set(MAPPER_CONSUMED_FIELDS)

    for field_name in person.__class__.model_fields:
        attr = getattr(person, field_name)

        if isinstance(attr, MappedField):
            consumed.update(attr.source_fields)

    return tuple(sorted(name for name in raw if name not in consumed))


def _category(*, field_name: str, label: str) -> CategoryRef:
    return CategoryRef(
        code=slugify(label),
        label=label,
        taxonomy_id=f"chicago-traffic-people-{field_name.replace('_', '-')}",
        taxonomy_version=_TAXONOMY_VERSION,
    )
