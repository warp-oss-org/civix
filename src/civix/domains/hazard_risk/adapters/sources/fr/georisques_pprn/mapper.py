"""France Georisques GASPAR PPRN mappers."""

from __future__ import annotations

import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Final, cast

from pydantic import BaseModel

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, MapperId, SourceId
from civix.core.mapping.errors import MappingError
from civix.core.mapping.models.mapper import MappingReport, MapResult
from civix.core.mapping.parsers import require_text, slugify, str_or_none
from civix.core.provenance.models.provenance import MapperVersion, ProvenanceRef
from civix.core.quality.models.fields import FieldQuality, MappedField
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.taxonomy.models.category import CategoryRef
from civix.core.temporal import (
    TemporalPeriod,
    TemporalPeriodPrecision,
    TemporalTimezoneStatus,
)
from civix.domains.hazard_risk.adapters.sources.fr.georisques_pprn.adapter import (
    GEORISQUES_PPRN_DATASET_ID,
)
from civix.domains.hazard_risk.adapters.sources.fr.georisques_pprn.caveats import (
    GEORISQUES_PPRN_METADATA_SOURCE_FIELD,
    georisques_pprn_caveat_categories,
)
from civix.domains.hazard_risk.adapters.sources.fr.georisques_pprn.schema import (
    GEORISQUES_PPRN_TAXONOMY_VERSION,
)
from civix.domains.hazard_risk.models import (
    HazardRiskArea,
    HazardRiskAreaKind,
    HazardRiskHazardType,
    HazardRiskZone,
    HazardRiskZoneStatus,
    SourceIdentifier,
    build_hazard_risk_area_key,
    build_hazard_risk_zone_key,
)

AREA_MAPPER_ID: Final[MapperId] = MapperId("georisques-pprn-commune-area")
ZONE_MAPPER_ID: Final[MapperId] = MapperId("georisques-pprn-regulatory-zone")
AREA_MAPPER_VERSION: Final[str] = "0.1.0"
ZONE_MAPPER_VERSION: Final[str] = "0.1.0"

_AREA_IDENTIFIER_TAXONOMY_ID: Final[str] = "georisques-pprn-area-identifier"
_AREA_KIND_TAXONOMY_ID: Final[str] = "georisques-pprn-area-kind"
_HAZARD_TAXONOMY_ID: Final[str] = "georisques-pprn-hazard"
_STATUS_TAXONOMY_ID: Final[str] = "georisques-pprn-status"
_ZONE_IDENTIFIER_TAXONOMY_ID: Final[str] = "georisques-pprn-zone-identifier"
_ZONE_TAXONOMY_ID: Final[str] = "georisques-pprn-zone"

_EFFECTIVE_PERIOD_FIELDS: Final[tuple[str, ...]] = (
    "APPROBATION",
    "APPLIC_ANTIC",
)
_STATUS_SOURCE_FIELDS: Final[tuple[str, ...]] = ("LIBELLE ETAT", "LIBELLE SOUS-ETAT")
_IGNORED_MAPPING_REPORT_FIELDS: Final[frozenset[str]] = frozenset()
_EFFECTIVE_STATUS_LABELS: Final[frozenset[str]] = frozenset({"opposable", "approuve", "anticipe"})
_IN_PROGRESS_STATUS_LABELS: Final[frozenset[str]] = frozenset({"prescrit", "proroge"})
_CANCELLED_STATUS_LABELS: Final[frozenset[str]] = frozenset(
    {"annule", "deprescrit"}
)
_ABROGATED_STATUS_LABELS: Final[frozenset[str]] = frozenset({"abroge"})


@dataclass(frozen=True, slots=True)
class GeorisquesPprnAreaMapper:
    """Maps one GASPAR PPRN row to the source commune risk area."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=AREA_MAPPER_ID, version=AREA_MAPPER_VERSION)

    def __call__(self, record: RawRecord, snapshot: SourceSnapshot) -> MapResult[HazardRiskArea]:
        _require_pprn_snapshot(snapshot, self.version, record)
        raw = record.raw_data
        commune_id = _required_text(raw, "CODE INSEE COMMUNE", self.version, record)
        area = HazardRiskArea(
            provenance=_build_provenance(record=record, snapshot=snapshot, mapper=self.version),
            area_key=build_hazard_risk_area_key(
                SourceId(str(snapshot.source_id)),
                DatasetId(str(snapshot.dataset_id)),
                commune_id,
            ),
            source_area_identifiers=_map_area_identifiers(raw, self.version, record),
            area_kind=MappedField(
                value=HazardRiskAreaKind.ADMINISTRATIVE_AREA,
                quality=FieldQuality.STANDARDIZED,
                source_fields=("CODE INSEE COMMUNE",),
            ),
            source_area_kind=MappedField(
                value=_category(
                    "commune",
                    label="Commune",
                    taxonomy_id=_AREA_KIND_TAXONOMY_ID,
                ),
                quality=FieldQuality.STANDARDIZED,
                source_fields=("CODE INSEE COMMUNE",),
            ),
            name=MappedField(
                value=_required_text(raw, "NOM COMMUNE", self.version, record),
                quality=FieldQuality.DIRECT,
                source_fields=("NOM COMMUNE",),
            ),
            jurisdiction=_map_jurisdiction(raw, self.version, record),
            administrative_areas=_map_administrative_areas(raw, self.version, record),
            footprint=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            geometry_ref=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            source_hazards=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            source_caveats=_map_source_caveats(),
        )

        return MapResult(record=area, report=_mapping_report(raw, area))


@dataclass(frozen=True, slots=True)
class GeorisquesPprnZoneMapper:
    """Maps one GASPAR PPRN row to regulatory-zone context."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=ZONE_MAPPER_ID, version=ZONE_MAPPER_VERSION)

    def __call__(self, record: RawRecord, snapshot: SourceSnapshot) -> MapResult[HazardRiskZone]:
        _require_pprn_snapshot(snapshot, self.version, record)
        raw = record.raw_data
        source_record_id = _source_record_id(record, self.version)
        zone = HazardRiskZone(
            provenance=_build_provenance(record=record, snapshot=snapshot, mapper=self.version),
            zone_key=build_hazard_risk_zone_key(
                SourceId(str(snapshot.source_id)),
                DatasetId(str(snapshot.dataset_id)),
                source_record_id,
            ),
            source_zone_identifiers=_map_zone_identifiers(raw, self.version, record),
            hazard_type=_map_hazard_type(raw),
            source_hazard=_map_source_hazard(raw),
            source_zone=_map_source_zone(raw),
            status=_map_status(raw),
            source_status=_map_source_status(raw),
            plan_identifier=MappedField(
                value=_required_text(raw, "CODE PROECEDURE", self.version, record),
                quality=FieldQuality.DIRECT,
                source_fields=("CODE PROECEDURE",),
            ),
            plan_name=MappedField(
                value=_required_text(raw, "LIBELLE PROCEDURE", self.version, record),
                quality=FieldQuality.DIRECT,
                source_fields=("LIBELLE PROCEDURE",),
            ),
            effective_period=_map_effective_period(raw, self.version, record),
            footprint=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            geometry_ref=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            source_caveats=_map_source_caveats(),
        )

        return MapResult(record=zone, report=_mapping_report(raw, zone))


def _require_pprn_snapshot(
    snapshot: SourceSnapshot,
    mapper: MapperVersion,
    record: RawRecord,
) -> None:
    if snapshot.dataset_id == GEORISQUES_PPRN_DATASET_ID:
        return

    raise MappingError(
        "Georisques PPRN mapper requires the GASPAR PPRN dataset",
        mapper=mapper,
        source_record_id=record.source_record_id,
        source_fields=("source.dataset_id",),
    )


def _map_area_identifiers(
    raw: Mapping[str, Any],
    mapper: MapperVersion,
    record: RawRecord,
) -> MappedField[tuple[SourceIdentifier, ...]]:
    fields = (
        "CODE INSEE COMMUNE",
        "CODE INSEE DEPARTEMENT",
        "CODE INSEE REGION",
    )
    identifiers = tuple(
        SourceIdentifier(
            value=_required_text(raw, field_name, mapper, record),
            identifier_kind=_category(
                field_name.lower().replace(" ", "-"),
                taxonomy_id=_AREA_IDENTIFIER_TAXONOMY_ID,
            ),
        )
        for field_name in fields
    )

    return MappedField(
        value=identifiers,
        quality=FieldQuality.DIRECT,
        source_fields=fields,
    )


def _map_zone_identifiers(
    raw: Mapping[str, Any],
    mapper: MapperVersion,
    record: RawRecord,
) -> MappedField[tuple[SourceIdentifier, ...]]:
    identifiers: list[SourceIdentifier] = []
    source_fields: list[str] = []

    for field_name in (
        "CODE PROECEDURE",
        "CODE INSEE COMMUNE",
        "CODE RISQUE 1",
        "CODE RISQUE 2",
        "CODE RISQUE 3",
        "CODES REVISES",
        "CODES REVISANTS",
    ):
        value = str_or_none(raw.get(field_name))
        if value is None:
            continue

        identifiers.append(
            SourceIdentifier(
                value=value,
                identifier_kind=_category(
                    field_name.lower().replace(" ", "-"),
                    taxonomy_id=_ZONE_IDENTIFIER_TAXONOMY_ID,
                ),
            )
        )
        source_fields.append(field_name)

    return MappedField(
        value=tuple(identifiers),
        quality=FieldQuality.DIRECT,
        source_fields=tuple(source_fields),
    )


def _map_jurisdiction(
    raw: Mapping[str, Any],
    mapper: MapperVersion,
    record: RawRecord,
) -> MappedField[Jurisdiction]:
    return MappedField(
        value=Jurisdiction(
            country="FR",
            region=_required_text(raw, "CODE INSEE REGION", mapper, record),
            locality=_required_text(raw, "CODE INSEE COMMUNE", mapper, record),
        ),
        quality=FieldQuality.STANDARDIZED,
        source_fields=("CODE INSEE REGION", "CODE INSEE COMMUNE"),
    )


def _map_administrative_areas(
    raw: Mapping[str, Any],
    mapper: MapperVersion,
    record: RawRecord,
) -> MappedField[tuple[str, ...]]:
    return MappedField(
        value=(
            _required_text(raw, "NOM REGION", mapper, record),
            _required_text(raw, "NOM DEPARTEMENT", mapper, record),
            _required_text(raw, "NOM COMMUNE", mapper, record),
        ),
        quality=FieldQuality.DIRECT,
        source_fields=("NOM REGION", "NOM DEPARTEMENT", "NOM COMMUNE"),
    )


def _map_hazard_type(raw: Mapping[str, Any]) -> MappedField[HazardRiskHazardType]:
    return MappedField(
        value=_hazard_type(raw),
        quality=FieldQuality.STANDARDIZED,
        source_fields=("CODE RISQUE 2", "CODE RISQUE 3", "LIBELLE RISQUE 2", "LIBELLE RISQUE 3"),
    )


def _map_source_hazard(raw: Mapping[str, Any]) -> MappedField[CategoryRef]:
    hazard = _source_hazard(raw)
    if hazard is None:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=(
                "CODE RISQUE 2",
                "CODE RISQUE 3",
                "LIBELLE RISQUE 2",
                "LIBELLE RISQUE 3",
            ),
        )

    return MappedField(
        value=hazard,
        quality=FieldQuality.STANDARDIZED,
        source_fields=("CODE RISQUE 2", "CODE RISQUE 3", "LIBELLE RISQUE 2", "LIBELLE RISQUE 3"),
    )


def _source_hazard(raw: Mapping[str, Any]) -> CategoryRef | None:
    risk_code = str_or_none(raw.get("CODE RISQUE 3")) or str_or_none(raw.get("CODE RISQUE 2"))
    risk_label = str_or_none(raw.get("LIBELLE RISQUE 2"))
    risk_detail = str_or_none(raw.get("LIBELLE RISQUE 3"))
    if risk_code is None and risk_label is None and risk_detail is None:
        return None

    label = " - ".join(part for part in (risk_label, risk_detail) if part is not None)
    code = f"risk-{risk_code}" if risk_code is not None else _safe_slug(label)

    return _category(code, label=label or code, taxonomy_id=_HAZARD_TAXONOMY_ID)


def _hazard_type(raw: Mapping[str, Any]) -> HazardRiskHazardType:
    risk_code = str_or_none(raw.get("CODE RISQUE 3")) or str_or_none(raw.get("CODE RISQUE 2"))
    labels = " ".join(
        value.casefold()
        for value in (
            str_or_none(raw.get("LIBELLE RISQUE 2")),
            str_or_none(raw.get("LIBELLE RISQUE 3")),
        )
        if value is not None
    )

    if risk_code == "110" or "inondation" in labels:
        return HazardRiskHazardType.FLOOD

    if risk_code == "160" or "feu de forêt" in labels or "feu de foret" in labels:
        return HazardRiskHazardType.WILDFIRE

    if risk_code == "130" or "séisme" in labels or "seisme" in labels:
        return HazardRiskHazardType.EARTHQUAKE

    if risk_code in {"120", "123", "124", "125", "126", "127"} or "mouvement" in labels:
        return HazardRiskHazardType.LANDSLIDE

    if "cyclone" in labels or "tempête" in labels or "tempete" in labels:
        return HazardRiskHazardType.STORM

    if labels:
        return HazardRiskHazardType.SOURCE_SPECIFIC

    return HazardRiskHazardType.UNKNOWN


def _map_source_zone(raw: Mapping[str, Any]) -> MappedField[CategoryRef]:
    model_code = str_or_none(raw.get("CODE MODELE"))
    risk_code = str_or_none(raw.get("CODE RISQUE 2"))
    risk_detail = str_or_none(raw.get("LIBELLE RISQUE 3"))

    risk_detail_code = str_or_none(raw.get("CODE RISQUE 3"))

    has_source_zone_parts = any(
        value is not None for value in (model_code, risk_code, risk_detail_code, risk_detail)
    )

    if not has_source_zone_parts:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("CODE MODELE", "CODE RISQUE 2", "CODE RISQUE 3", "LIBELLE RISQUE 3"),
        )

    code = "__".join(
        part
        for part in (
            f"model-{_safe_slug(model_code)}" if model_code is not None else None,
            f"risk-{risk_detail_code or risk_code}" if risk_detail_code or risk_code else None,
            f"detail-{_safe_slug(risk_detail)}" if risk_detail is not None else None,
        )
        if part is not None
    )
    label = " - ".join(
        part
        for part in (
            str_or_none(raw.get("LIBELLE MODELE")),
            str_or_none(raw.get("LIBELLE RISQUE 2")),
            risk_detail,
        )
        if part is not None
    )

    return MappedField(
        value=_category(code, label=label, taxonomy_id=_ZONE_TAXONOMY_ID),
        quality=FieldQuality.STANDARDIZED,
        source_fields=(
            "CODE MODELE",
            "LIBELLE MODELE",
            "CODE RISQUE 2",
            "CODE RISQUE 3",
            "LIBELLE RISQUE 2",
            "LIBELLE RISQUE 3",
        ),
    )


def _map_status(raw: Mapping[str, Any]) -> MappedField[HazardRiskZoneStatus]:
    return MappedField(
        value=_status(raw),
        quality=FieldQuality.STANDARDIZED,
        source_fields=_STATUS_SOURCE_FIELDS,
    )


def _status(raw: Mapping[str, Any]) -> HazardRiskZoneStatus:
    state = _normalized_text(raw.get("LIBELLE ETAT"))
    substate = _normalized_text(raw.get("LIBELLE SOUS-ETAT"))

    if substate in _ABROGATED_STATUS_LABELS:
        return HazardRiskZoneStatus.ABROGATED

    if substate in _CANCELLED_STATUS_LABELS:
        return HazardRiskZoneStatus.CANCELLED

    if state == "caduque":
        return HazardRiskZoneStatus.ABROGATED

    labels = tuple(label for label in (state, substate) if label is not None)

    if any(label in _EFFECTIVE_STATUS_LABELS for label in labels):
        return HazardRiskZoneStatus.EFFECTIVE

    if any(label in _IN_PROGRESS_STATUS_LABELS for label in labels):
        return HazardRiskZoneStatus.IN_PROGRESS

    return HazardRiskZoneStatus.UNKNOWN


def _map_source_status(raw: Mapping[str, Any]) -> MappedField[CategoryRef]:
    source_labels = tuple(
        label
        for label in (
            str_or_none(raw.get("LIBELLE ETAT")),
            str_or_none(raw.get("LIBELLE SOUS-ETAT")),
        )
        if label is not None
    )
    if not source_labels:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=_STATUS_SOURCE_FIELDS,
        )

    label = " - ".join(source_labels)

    return MappedField(
        value=_category(label, label=label, taxonomy_id=_STATUS_TAXONOMY_ID),
        quality=FieldQuality.STANDARDIZED,
        source_fields=_STATUS_SOURCE_FIELDS,
    )


def _normalized_text(value: object) -> str | None:
    text = str_or_none(value)
    if text is None:
        return None

    decomposed = unicodedata.normalize("NFKD", text.casefold())

    return "".join(character for character in decomposed if not unicodedata.combining(character))


def _map_effective_period(
    raw: Mapping[str, Any],
    mapper: MapperVersion,
    record: RawRecord,
) -> MappedField[TemporalPeriod]:
    for field_name in _EFFECTIVE_PERIOD_FIELDS:
        parsed = _parse_date(raw.get(field_name), field_name, mapper, record)
        if parsed is None:
            continue

        return MappedField(
            value=TemporalPeriod(
                precision=TemporalPeriodPrecision.DATE,
                date_value=parsed,
                timezone_status=TemporalTimezoneStatus.UNKNOWN,
            ),
            quality=FieldQuality.STANDARDIZED,
            source_fields=(field_name,),
        )

    return MappedField(
        value=None,
        quality=FieldQuality.NOT_PROVIDED,
        source_fields=_EFFECTIVE_PERIOD_FIELDS,
    )


def _parse_date(
    value: object,
    field_name: str,
    mapper: MapperVersion,
    record: RawRecord,
) -> date | None:
    text = str_or_none(value)
    if text is None:
        return None

    try:
        if len(text) == 10:
            return date.fromisoformat(text)

        if len(text) == 19 and text[10] == " ":
            return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").date()
    except ValueError as e:
        raise MappingError(
            f"invalid date source field {field_name!r}",
            mapper=mapper,
            source_record_id=record.source_record_id,
            source_fields=(field_name,),
        ) from e

    raise MappingError(
        f"unsupported date source field {field_name!r}",
        mapper=mapper,
        source_record_id=record.source_record_id,
        source_fields=(field_name,),
    )


def _map_source_caveats() -> MappedField[tuple[CategoryRef, ...]]:
    return MappedField(
        value=georisques_pprn_caveat_categories(),
        quality=FieldQuality.STANDARDIZED,
        source_fields=(GEORISQUES_PPRN_METADATA_SOURCE_FIELD,),
    )


def _required_text(
    raw: Mapping[str, Any],
    field_name: str,
    mapper: MapperVersion,
    record: RawRecord,
) -> str:
    return require_text(
        raw.get(field_name),
        field_name=field_name,
        mapper=mapper,
        source_record_id=record.source_record_id,
    )


def _source_record_id(record: RawRecord, mapper: MapperVersion) -> str:
    if record.source_record_id is not None:
        return record.source_record_id

    raise MappingError(
        "Georisques PPRN raw record is missing source_record_id",
        mapper=mapper,
        source_record_id=None,
        source_fields=("source.record_id",),
    )


def _category(
    code: str,
    *,
    taxonomy_id: str,
    label: str | None = None,
) -> CategoryRef:
    normalized_code = _safe_slug(code)

    return CategoryRef(
        code=normalized_code,
        label=label if label is not None else normalized_code.replace("-", " ").title(),
        taxonomy_id=taxonomy_id,
        taxonomy_version=GEORISQUES_PPRN_TAXONOMY_VERSION,
    )


def _safe_slug(value: str) -> str:
    return slugify(value) or "unknown"


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


def _mapping_report(raw: Mapping[str, Any], record: BaseModel) -> MappingReport:
    consumed: set[str] = set()
    _collect_source_fields(record, consumed)
    reportable_fields = set(raw) - _IGNORED_MAPPING_REPORT_FIELDS

    return MappingReport(unmapped_source_fields=tuple(sorted(reportable_fields - consumed)))


def _collect_source_fields(value: object, consumed: set[str]) -> None:
    if isinstance(value, MappedField):
        consumed.update(value.source_fields)
        return

    if isinstance(value, BaseModel):
        for field_name in value.__class__.model_fields:
            _collect_source_fields(getattr(value, field_name), consumed)
        return

    if isinstance(value, tuple):
        for item in cast(tuple[object, ...], value):
            _collect_source_fields(item, consumed)
