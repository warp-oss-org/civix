"""FEMA National Flood Hazard Layer mappers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final, cast

from pydantic import BaseModel

from civix.core.identity.models.identifiers import DatasetId, MapperId, SourceId
from civix.core.mapping.errors import MappingError
from civix.core.mapping.models.mapper import MappingReport, MapResult
from civix.core.mapping.parsers import require_text, slugify, str_or_none
from civix.core.provenance.models.provenance import MapperVersion, ProvenanceRef
from civix.core.quality.models.fields import FieldQuality, MappedField
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.spatial.models.geometry import GeometryRef, GeometryType
from civix.core.taxonomy.models.category import CategoryRef
from civix.domains.hazard_risk.adapters.sources.us.fema_nfhl.adapter import (
    FEMA_NFHL_FLOOD_HAZARD_ZONES_DATASET_ID,
    FEMA_NFHL_FLOOD_HAZARD_ZONES_LAYER_NAME,
    FEMA_NFHL_FLOOD_HAZARD_ZONES_SERVICE_URL,
    FEMA_NFHL_FLOOD_HAZARD_ZONES_SOURCE_CRS,
)
from civix.domains.hazard_risk.adapters.sources.us.fema_nfhl.caveats import (
    FEMA_NFHL_METADATA_SOURCE_FIELD,
    fema_nfhl_caveat_categories,
)
from civix.domains.hazard_risk.adapters.sources.us.fema_nfhl.schema import (
    FEMA_NFHL_TAXONOMY_VERSION,
)
from civix.domains.hazard_risk.models import (
    HazardRiskHazardType,
    HazardRiskZone,
    HazardRiskZoneStatus,
    SourceIdentifier,
    build_hazard_risk_zone_key,
)

ZONE_MAPPER_ID: Final[MapperId] = MapperId("fema-nfhl-zone")
ZONE_MAPPER_VERSION: Final[str] = "0.1.0"

_HAZARD_TAXONOMY_ID: Final[str] = "fema-nfhl-hazard"
_IDENTIFIER_TAXONOMY_ID: Final[str] = "fema-nfhl-zone-identifier"
_LAYER_STATUS_TAXONOMY_ID: Final[str] = "fema-nfhl-layer-status"
_ZONE_TAXONOMY_ID: Final[str] = "fema-nfhl-zone"
_SOURCE_STATUS_FIELD: Final[str] = "source.layer_status"
_IGNORED_MAPPING_REPORT_FIELDS: Final[frozenset[str]] = frozenset({"OBJECTID"})


@dataclass(frozen=True, slots=True)
class FemaNfhlZoneMapper:
    """Maps one FEMA NFHL Flood Hazard Zone row to a hazard-risk zone."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=ZONE_MAPPER_ID, version=ZONE_MAPPER_VERSION)

    def __call__(self, record: RawRecord, snapshot: SourceSnapshot) -> MapResult[HazardRiskZone]:
        _require_effective_nfhl_snapshot(snapshot, self.version, record)
        raw = record.raw_data
        flood_area_id = _required_text(raw, "FLD_AR_ID", self.version, record)
        dfirm_id = _required_text(raw, "DFIRM_ID", self.version, record)
        zone = HazardRiskZone(
            provenance=_build_provenance(record=record, snapshot=snapshot, mapper=self.version),
            zone_key=build_hazard_risk_zone_key(
                SourceId(str(snapshot.source_id)),
                DatasetId(str(snapshot.dataset_id)),
                flood_area_id,
            ),
            source_zone_identifiers=_map_zone_identifiers(raw),
            hazard_type=MappedField(
                value=HazardRiskHazardType.FLOOD,
                quality=FieldQuality.STANDARDIZED,
                source_fields=("FLD_ZONE",),
            ),
            source_hazard=MappedField(
                value=_category("flood", label="Flood", taxonomy_id=_HAZARD_TAXONOMY_ID),
                quality=FieldQuality.STANDARDIZED,
                source_fields=("FLD_ZONE",),
            ),
            source_zone=_map_source_zone(raw, self.version, record),
            status=MappedField(
                value=HazardRiskZoneStatus.EFFECTIVE,
                quality=FieldQuality.STANDARDIZED,
                source_fields=(_SOURCE_STATUS_FIELD,),
            ),
            source_status=MappedField(
                value=_category(
                    "effective",
                    label="Effective NFHL",
                    taxonomy_id=_LAYER_STATUS_TAXONOMY_ID,
                ),
                quality=FieldQuality.STANDARDIZED,
                source_fields=(_SOURCE_STATUS_FIELD,),
            ),
            plan_identifier=MappedField(
                value=dfirm_id,
                quality=FieldQuality.DIRECT,
                source_fields=("DFIRM_ID",),
            ),
            plan_name=MappedField(
                value=f"FEMA NFHL DFIRM {dfirm_id}",
                quality=FieldQuality.DERIVED,
                source_fields=("DFIRM_ID",),
            ),
            effective_period=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            footprint=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            geometry_ref=_map_geometry_ref(raw, self.version, record),
            source_caveats=_map_source_caveats(),
        )

        return MapResult(record=zone, report=_mapping_report(raw, zone))


def _require_effective_nfhl_snapshot(
    snapshot: SourceSnapshot,
    mapper: MapperVersion,
    record: RawRecord,
) -> None:
    if snapshot.dataset_id == FEMA_NFHL_FLOOD_HAZARD_ZONES_DATASET_ID:
        return

    raise MappingError(
        "FEMA NFHL zone mapper requires the effective Flood Hazard Zones dataset",
        mapper=mapper,
        source_record_id=record.source_record_id,
        source_fields=(_SOURCE_STATUS_FIELD,),
    )


def _map_zone_identifiers(raw: Mapping[str, Any]) -> MappedField[tuple[SourceIdentifier, ...]]:
    identifiers: list[SourceIdentifier] = []
    source_fields: list[str] = []

    for field_name in ("FLD_AR_ID", "DFIRM_ID", "GFID", "GlobalID"):
        value = str_or_none(raw.get(field_name))
        if value is None:
            continue

        identifiers.append(
            SourceIdentifier(
                value=value,
                identifier_kind=_category(
                    field_name.lower(),
                    taxonomy_id=_IDENTIFIER_TAXONOMY_ID,
                ),
            )
        )
        source_fields.append(field_name)

    return MappedField(
        value=tuple(identifiers),
        quality=FieldQuality.DIRECT,
        source_fields=tuple(source_fields),
    )


def _map_source_zone(
    raw: Mapping[str, Any],
    mapper: MapperVersion,
    record: RawRecord,
) -> MappedField[CategoryRef]:
    flood_zone = _required_text(raw, "FLD_ZONE", mapper, record)
    sfha_flag = _sfha_flag(raw, mapper, record)
    subtype = str_or_none(raw.get("ZONE_SUBTY"))
    subtype_code = slugify(subtype) if subtype is not None else "none"
    code = f"fld-zone-{slugify(flood_zone)}__sfha-{sfha_flag}__subtype-{subtype_code}"
    label_parts = [f"FEMA NFHL Zone {flood_zone}", f"SFHA {sfha_flag.upper()}"]

    if subtype is not None:
        label_parts.append(subtype.title())

    return MappedField(
        value=CategoryRef(
            code=code,
            label=" - ".join(label_parts),
            taxonomy_id=_ZONE_TAXONOMY_ID,
            taxonomy_version=FEMA_NFHL_TAXONOMY_VERSION,
        ),
        quality=FieldQuality.STANDARDIZED,
        source_fields=("FLD_ZONE", "SFHA_TF", "ZONE_SUBTY"),
    )


def _sfha_flag(
    raw: Mapping[str, Any],
    mapper: MapperVersion,
    record: RawRecord,
) -> str:
    value = _required_text(raw, "SFHA_TF", mapper, record).casefold()
    if value in {"y", "n"}:
        return value

    raise MappingError(
        f"unrecognized FEMA NFHL SFHA_TF value {value!r}",
        mapper=mapper,
        source_record_id=record.source_record_id,
        source_fields=("SFHA_TF",),
    )


def _map_geometry_ref(
    raw: Mapping[str, Any],
    mapper: MapperVersion,
    record: RawRecord,
) -> MappedField[GeometryRef]:
    flood_area_id = _required_text(raw, "FLD_AR_ID", mapper, record)
    dfirm_id = _required_text(raw, "DFIRM_ID", mapper, record)

    return MappedField(
        value=GeometryRef(
            geometry_type=GeometryType.POLYGON,
            uri=FEMA_NFHL_FLOOD_HAZARD_ZONES_SERVICE_URL,
            layer_name=FEMA_NFHL_FLOOD_HAZARD_ZONES_LAYER_NAME,
            geometry_id=flood_area_id,
            source_crs=FEMA_NFHL_FLOOD_HAZARD_ZONES_SOURCE_CRS,
            query_keys=(("FLD_AR_ID", flood_area_id), ("DFIRM_ID", dfirm_id)),
        ),
        quality=FieldQuality.STANDARDIZED,
        source_fields=("FLD_AR_ID", "DFIRM_ID"),
    )


def _map_source_caveats() -> MappedField[tuple[CategoryRef, ...]]:
    return MappedField(
        value=fema_nfhl_caveat_categories(),
        quality=FieldQuality.STANDARDIZED,
        source_fields=(FEMA_NFHL_METADATA_SOURCE_FIELD, "SOURCE_CIT"),
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


def _category(
    code: str,
    *,
    taxonomy_id: str,
    label: str | None = None,
) -> CategoryRef:
    return CategoryRef(
        code=slugify(code),
        label=label if label is not None else code.replace("_", " ").replace("-", " ").title(),
        taxonomy_id=taxonomy_id,
        taxonomy_version=FEMA_NFHL_TAXONOMY_VERSION,
    )


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
