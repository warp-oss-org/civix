"""Public Safety Canada FIFRA mappers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any, Final, cast

from pydantic import BaseModel

from civix.core.identity.models.identifiers import DatasetId, MapperId, SourceId
from civix.core.mapping.errors import MappingError
from civix.core.mapping.models.mapper import MappingReport, MapResult
from civix.core.mapping.parsers import require_text, slugify
from civix.core.provenance.models.provenance import MapperVersion, ProvenanceRef
from civix.core.quality.models.fields import FieldQuality, MappedField
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.spatial.models.geometry import GeometryRef, GeometryType
from civix.core.taxonomy.models.category import CategoryRef
from civix.core.temporal import (
    TemporalPeriod,
    TemporalPeriodPrecision,
    TemporalTimezoneStatus,
)
from civix.domains.hazard_risk.adapters.sources.ca.ps_fifra.adapter import (
    PS_FIFRA_DATASET_ID,
)
from civix.domains.hazard_risk.adapters.sources.ca.ps_fifra.caveats import (
    PS_FIFRA_METADATA_SOURCE_FIELD,
    ps_fifra_caveat_categories,
)
from civix.domains.hazard_risk.adapters.sources.ca.ps_fifra.schema import (
    PS_FIFRA_TAXONOMY_VERSION,
)
from civix.domains.hazard_risk.models import (
    HazardRiskHazardType,
    HazardRiskZone,
    HazardRiskZoneStatus,
    SourceIdentifier,
    build_hazard_risk_zone_key,
)

ZONE_MAPPER_ID: Final[MapperId] = MapperId("ps-fifra-zone")
ZONE_MAPPER_VERSION: Final[str] = "0.1.0"

_HAZARD_TAXONOMY_ID: Final[str] = "ps-fifra-flood-type"
_IDENTIFIER_TAXONOMY_ID: Final[str] = "ps-fifra-zone-identifier"
_STATUS_TAXONOMY_ID: Final[str] = "ps-fifra-status"
_ZONE_TAXONOMY_ID: Final[str] = "ps-fifra-flood-risk-rating"


@dataclass(frozen=True, slots=True)
class PsFifraZoneMapper:
    """Maps one FIFRA row to a classified hazard-risk zone."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=ZONE_MAPPER_ID, version=ZONE_MAPPER_VERSION)

    def __call__(self, record: RawRecord, snapshot: SourceSnapshot) -> MapResult[HazardRiskZone]:
        _require_fifra_snapshot(snapshot, self.version, record)
        raw = record.raw_data
        area_id = _required_text(raw, "area_id", self.version, record)
        zone = HazardRiskZone(
            provenance=_build_provenance(record=record, snapshot=snapshot, mapper=self.version),
            zone_key=build_hazard_risk_zone_key(
                SourceId(str(snapshot.source_id)),
                DatasetId(str(snapshot.dataset_id)),
                area_id,
            ),
            source_zone_identifiers=_map_zone_identifiers(raw, self.version, record),
            hazard_type=MappedField(
                value=HazardRiskHazardType.FLOOD,
                quality=FieldQuality.STANDARDIZED,
                source_fields=("flood_type",),
            ),
            source_hazard=MappedField(
                value=_source_hazard(raw, self.version, record),
                quality=FieldQuality.STANDARDIZED,
                source_fields=("flood_type",),
            ),
            source_zone=_map_source_zone(raw, self.version, record),
            status=MappedField(
                value=HazardRiskZoneStatus.EFFECTIVE,
                quality=FieldQuality.STANDARDIZED,
                source_fields=("publication_version",),
            ),
            source_status=MappedField(
                value=_category("public-screening-area", taxonomy_id=_STATUS_TAXONOMY_ID),
                quality=FieldQuality.STANDARDIZED,
                source_fields=("publication_version",),
            ),
            plan_identifier=MappedField(
                value=_required_text(raw, "publication_version", self.version, record),
                quality=FieldQuality.DIRECT,
                source_fields=("publication_version",),
            ),
            plan_name=MappedField(
                value="Public Safety Canada Federally Identified Flood Risk Areas",
                quality=FieldQuality.STANDARDIZED,
                source_fields=("publication_version",),
            ),
            effective_period=_map_effective_period(raw, self.version, record),
            footprint=MappedField(value=None, quality=FieldQuality.UNMAPPED, source_fields=()),
            geometry_ref=_map_geometry_ref(raw, self.version, record),
            source_caveats=_map_source_caveats(),
        )

        return MapResult(record=zone, report=_mapping_report(raw, zone))


def _require_fifra_snapshot(
    snapshot: SourceSnapshot,
    mapper: MapperVersion,
    record: RawRecord,
) -> None:
    if snapshot.dataset_id == PS_FIFRA_DATASET_ID:
        return

    raise MappingError(
        "FIFRA mapper requires the Federally Identified Flood Risk Areas dataset",
        mapper=mapper,
        source_record_id=record.source_record_id,
        source_fields=("source.dataset_id",),
    )


def _map_zone_identifiers(
    raw: Mapping[str, Any],
    mapper: MapperVersion,
    record: RawRecord,
) -> MappedField[tuple[SourceIdentifier, ...]]:
    fields = ("area_id", "province_code")

    return MappedField(
        value=tuple(
            SourceIdentifier(
                value=_required_text(raw, field_name, mapper, record),
                identifier_kind=_category(field_name, taxonomy_id=_IDENTIFIER_TAXONOMY_ID),
            )
            for field_name in fields
        ),
        quality=FieldQuality.DIRECT,
        source_fields=fields,
    )


def _map_source_zone(
    raw: Mapping[str, Any],
    mapper: MapperVersion,
    record: RawRecord,
) -> MappedField[CategoryRef]:
    rating = _required_text(raw, "flood_risk_rating", mapper, record)

    return MappedField(
        value=_category(rating, taxonomy_id=_ZONE_TAXONOMY_ID, label=rating),
        quality=FieldQuality.STANDARDIZED,
        source_fields=("flood_risk_rating",),
    )


def _map_effective_period(
    raw: Mapping[str, Any],
    mapper: MapperVersion,
    record: RawRecord,
) -> MappedField[TemporalPeriod]:
    return MappedField(
        value=TemporalPeriod(
            precision=TemporalPeriodPrecision.DATE,
            date_value=date.fromisoformat(_required_text(raw, "publication_date", mapper, record)),
            timezone_status=TemporalTimezoneStatus.UNKNOWN,
        ),
        quality=FieldQuality.DIRECT,
        source_fields=("publication_date",),
    )


def _map_geometry_ref(
    raw: Mapping[str, Any],
    mapper: MapperVersion,
    record: RawRecord,
) -> MappedField[GeometryRef]:
    geometry_id = _required_text(raw, "geometry_id", mapper, record)

    return MappedField(
        value=GeometryRef(
            geometry_type=GeometryType.POLYGON,
            uri=_required_text(raw, "geometry_uri", mapper, record),
            layer_name=_required_text(raw, "geometry_layer", mapper, record),
            geometry_id=geometry_id,
            source_crs=_required_text(raw, "source_crs", mapper, record),
            query_keys=(("area_id", _required_text(raw, "area_id", mapper, record)),),
        ),
        quality=FieldQuality.STANDARDIZED,
        source_fields=("geometry_uri", "geometry_layer", "geometry_id", "source_crs", "area_id"),
    )


def _source_hazard(raw: Mapping[str, Any], mapper: MapperVersion, record: RawRecord) -> CategoryRef:
    flood_type = _required_text(raw, "flood_type", mapper, record)

    return _category(flood_type, taxonomy_id=_HAZARD_TAXONOMY_ID, label=flood_type)


def _map_source_caveats() -> MappedField[tuple[CategoryRef, ...]]:
    return MappedField(
        value=ps_fifra_caveat_categories(),
        quality=FieldQuality.STANDARDIZED,
        source_fields=(PS_FIFRA_METADATA_SOURCE_FIELD,),
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


def _category(code: str, *, taxonomy_id: str, label: str | None = None) -> CategoryRef:
    return CategoryRef(
        code=slugify(code),
        label=label if label is not None else code.replace("_", " ").replace("-", " ").title(),
        taxonomy_id=taxonomy_id,
        taxonomy_version=PS_FIFRA_TAXONOMY_VERSION,
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

    return MappingReport(unmapped_source_fields=tuple(sorted(set(raw) - consumed)))


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
