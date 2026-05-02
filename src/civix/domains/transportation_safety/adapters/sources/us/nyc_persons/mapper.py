"""NYC Motor Vehicle Collisions - Person mapper."""

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

MAPPER_ID: Final[MapperId] = MapperId("nyc-motor-vehicle-collisions-persons")
MAPPER_VERSION: Final[str] = "0.1.0"

_TAXONOMY_VERSION: Final[str] = "2026-05-02"
MAPPER_CONSUMED_FIELDS: Final[frozenset[str]] = frozenset(
    {"unique_id", "collision_id", "person_id", "vehicle_id"}
)
_CONTRIBUTING_FACTOR_FIELDS: Final[tuple[str, ...]] = (
    "contributing_factor_1",
    "contributing_factor_2",
)
_INJURY_MAP: Final[dict[str, InjuryOutcome]] = {
    "killed": InjuryOutcome.FATAL,
    "injured": InjuryOutcome.POSSIBLE,
    "unspecified": InjuryOutcome.UNKNOWN,
}


@dataclass(frozen=True, slots=True)
class NycPersonsMapper:
    """Maps NYC person rows to `CollisionPerson`."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=MAPPER_ID, version=MAPPER_VERSION)

    def __call__(self, record: RawRecord, snapshot: SourceSnapshot) -> MapResult[CollisionPerson]:
        raw = record.raw_data
        provenance = self._build_provenance(record=record, snapshot=snapshot)

        person = CollisionPerson(
            provenance=provenance,
            collision_id=require_text(
                raw.get("collision_id"),
                field_name="collision_id",
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
            age=_map_age(raw),
            safety_equipment=_map_source_category(raw, "safety_equipment"),
            position_in_vehicle=_map_source_category(raw, "position_in_vehicle"),
            ejection=_map_source_category(raw, "ejection"),
            contributing_factors=_map_contributing_factors(raw),
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
    ped_role = str_or_none(raw.get("ped_role"))
    person_type = str_or_none(raw.get("person_type"))
    normalized_role = ped_role.casefold() if ped_role is not None else ""
    normalized_type = person_type.casefold() if person_type is not None else ""

    if normalized_role == "driver":
        role = RoadUserRole.DRIVER
        fields = ("ped_role",)
    elif normalized_role == "passenger":
        role = RoadUserRole.PASSENGER
        fields = ("ped_role",)
    elif normalized_role == "pedestrian" or normalized_type == "pedestrian":
        role = RoadUserRole.PEDESTRIAN
        fields = ("ped_role", "person_type")
    elif normalized_type == "bicyclist":
        role = RoadUserRole.CYCLIST
        fields = ("person_type",)
    elif normalized_type == "other motorized":
        role = RoadUserRole.OTHER
        fields = ("person_type",)
    elif person_type is None and ped_role is None:
        return MappedField[RoadUserRole](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("person_type", "ped_role"),
        )
    else:
        role = RoadUserRole.UNKNOWN
        fields = ("person_type", "ped_role")

    return MappedField[RoadUserRole](
        value=role,
        quality=FieldQuality.STANDARDIZED,
        source_fields=fields,
    )


def _map_injury(raw: Mapping[str, Any]) -> MappedField[InjuryOutcome]:
    value = str_or_none(raw.get("person_injury"))

    if value is None:
        return MappedField[InjuryOutcome](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("person_injury",),
        )

    mapped = _INJURY_MAP.get(value.casefold(), InjuryOutcome.UNKNOWN)

    quality = (
        FieldQuality.STANDARDIZED if mapped is not InjuryOutcome.UNKNOWN else FieldQuality.INFERRED
    )

    return MappedField[InjuryOutcome](
        value=mapped,
        quality=quality,
        source_fields=("person_injury",),
    )


def _map_age(raw: Mapping[str, Any]) -> MappedField[int]:
    value = int_or_none(raw.get("person_age"))

    if value is None or value < 0 or value > 130:
        return MappedField[int](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("person_age",),
        )

    return MappedField[int](
        value=value,
        quality=FieldQuality.STANDARDIZED,
        source_fields=("person_age",),
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
        taxonomy_id=f"nyc-persons-{field_name.replace('_', '-')}",
        taxonomy_version=_TAXONOMY_VERSION,
    )
