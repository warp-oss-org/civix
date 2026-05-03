"""British Geological Survey GeoSure Basic mappers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Final, cast

from pydantic import BaseModel

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, MapperId, SourceId
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
from civix.domains.hazard_risk.adapters.sources.gb.bgs_geosure_basic.adapter import (
    BGS_GEOSURE_BASIC_DATASET_ID,
)
from civix.domains.hazard_risk.adapters.sources.gb.bgs_geosure_basic.caveats import (
    BGS_GEOSURE_BASIC_METADATA_SOURCE_FIELD,
    bgs_geosure_basic_caveat_categories,
)
from civix.domains.hazard_risk.adapters.sources.gb.bgs_geosure_basic.schema import (
    BGS_GEOSURE_BASIC_TAXONOMY_VERSION,
)
from civix.domains.hazard_risk.models import (
    CategoryScoreMeasure,
    HazardRiskArea,
    HazardRiskAreaKind,
    HazardRiskHazardType,
    HazardRiskScore,
    HazardRiskScoreDirection,
    HazardRiskScoreType,
    NumericScoreMeasure,
    ScoreScale,
    SourceIdentifier,
    build_hazard_risk_area_key,
)

AREA_MAPPER_ID: Final[MapperId] = MapperId("bgs-geosure-basic-area")
SCORES_MAPPER_ID: Final[MapperId] = MapperId("bgs-geosure-basic-scores")
AREA_MAPPER_VERSION: Final[str] = "0.1.0"
SCORES_MAPPER_VERSION: Final[str] = "0.1.0"
METHODOLOGY_LABEL: Final[str] = "BGS GeoSure Basic"

_AREA_IDENTIFIER_TAXONOMY_ID: Final[str] = "bgs-geosure-basic-area-identifier"
_AREA_KIND_TAXONOMY_ID: Final[str] = "bgs-geosure-basic-area-kind"
_HAZARD_TAXONOMY_ID: Final[str] = "bgs-geosure-basic-theme"
_RATING_TAXONOMY_ID: Final[str] = "bgs-geosure-basic-rating"
_SCORE_FIELD_TAXONOMY_ID: Final[str] = "bgs-geosure-basic-score-field"
_SCORE_UNIT_TAXONOMY_ID: Final[str] = "bgs-geosure-basic-score-unit"
_SCORE_SCALE_SOURCE_FIELD: Final[str] = "source.methodology.score_scale"


@dataclass(frozen=True, slots=True)
class BgsGeosureBasicAreaMapper:
    """Maps one GeoSure Basic row to a hazard-risk area."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=AREA_MAPPER_ID, version=AREA_MAPPER_VERSION)

    def __call__(self, record: RawRecord, snapshot: SourceSnapshot) -> MapResult[HazardRiskArea]:
        _require_geosure_snapshot(snapshot, self.version, record)
        raw = record.raw_data
        hex_id = _required_text(raw, "hex_id", self.version, record)
        area = HazardRiskArea(
            provenance=_build_provenance(record=record, snapshot=snapshot, mapper=self.version),
            area_key=build_hazard_risk_area_key(
                SourceId(str(snapshot.source_id)),
                DatasetId(str(snapshot.dataset_id)),
                hex_id,
            ),
            source_area_identifiers=_map_area_identifiers(raw, self.version, record),
            area_kind=MappedField(
                value=HazardRiskAreaKind.RISK_INDEX_AREA,
                quality=FieldQuality.STANDARDIZED,
                source_fields=("hex_id",),
            ),
            source_area_kind=MappedField(
                value=_category("geosure-basic-hex", taxonomy_id=_AREA_KIND_TAXONOMY_ID),
                quality=FieldQuality.STANDARDIZED,
                source_fields=("hex_id",),
            ),
            name=MappedField(
                value=_required_text(raw, "area_name", self.version, record),
                quality=FieldQuality.DIRECT,
                source_fields=("area_name",),
            ),
            jurisdiction=MappedField(
                value=Jurisdiction(
                    country="GB",
                    region=_required_text(raw, "country_part", self.version, record),
                ),
                quality=FieldQuality.STANDARDIZED,
                source_fields=("country_part",),
            ),
            administrative_areas=MappedField(
                value=(_required_text(raw, "country_part", self.version, record),),
                quality=FieldQuality.DIRECT,
                source_fields=("country_part",),
            ),
            footprint=MappedField(value=None, quality=FieldQuality.UNMAPPED, source_fields=()),
            geometry_ref=_map_geometry_ref(raw, self.version, record),
            source_hazards=MappedField(
                value=(_source_hazard(raw, self.version, record),),
                quality=FieldQuality.STANDARDIZED,
                source_fields=("geohazard_theme",),
            ),
            source_caveats=_map_source_caveats(),
        )

        return MapResult(record=area, report=_mapping_report(raw, area))


@dataclass(frozen=True, slots=True)
class BgsGeosureBasicScoresMapper:
    """Maps one GeoSure Basic row to geohazard susceptibility score facts."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=SCORES_MAPPER_ID, version=SCORES_MAPPER_VERSION)

    def __call__(
        self,
        record: RawRecord,
        snapshot: SourceSnapshot,
    ) -> MapResult[tuple[HazardRiskScore, ...]]:
        _require_geosure_snapshot(snapshot, self.version, record)
        raw = record.raw_data
        scores = (
            _map_rating_score(raw=raw, record=record, snapshot=snapshot, mapper=self.version),
            _map_numeric_score(raw=raw, record=record, snapshot=snapshot, mapper=self.version),
        )

        return MapResult(record=scores, report=_mapping_report(raw, scores))


def _map_rating_score(
    *,
    raw: Mapping[str, Any],
    record: RawRecord,
    snapshot: SourceSnapshot,
    mapper: MapperVersion,
) -> HazardRiskScore:
    rating = _required_text(raw, "susceptibility_rating", mapper, record)

    return _build_score(
        raw=raw,
        record=record,
        snapshot=snapshot,
        mapper=mapper,
        field_name="susceptibility_rating",
        score_type=HazardRiskScoreType.RATING,
        score_measure=CategoryScoreMeasure(
            value=_category(rating, taxonomy_id=_RATING_TAXONOMY_ID, label=rating)
        ),
        score_unit=_category("rating", taxonomy_id=_SCORE_UNIT_TAXONOMY_ID, label="Rating"),
        score_scale=MappedField(value=None, quality=FieldQuality.UNMAPPED, source_fields=()),
    )


def _map_numeric_score(
    *,
    raw: Mapping[str, Any],
    record: RawRecord,
    snapshot: SourceSnapshot,
    mapper: MapperVersion,
) -> HazardRiskScore:
    field_name = "susceptibility_score"

    try:
        measure = NumericScoreMeasure(value=Decimal(str(raw[field_name])))
    except (KeyError, InvalidOperation, ValueError) as e:
        raise MappingError(
            "invalid BGS GeoSure Basic susceptibility score",
            mapper=mapper,
            source_record_id=record.source_record_id,
            source_fields=(field_name,),
        ) from e

    return _build_score(
        raw=raw,
        record=record,
        snapshot=snapshot,
        mapper=mapper,
        field_name=field_name,
        score_type=HazardRiskScoreType.PER_HAZARD_SCORE,
        score_measure=measure,
        score_unit=_category("score", taxonomy_id=_SCORE_UNIT_TAXONOMY_ID, label="Score"),
        score_scale=MappedField(
            value=ScoreScale(minimum=Decimal("1"), maximum=Decimal("3")),
            quality=FieldQuality.STANDARDIZED,
            source_fields=(_SCORE_SCALE_SOURCE_FIELD,),
        ),
    )


def _build_score(
    *,
    raw: Mapping[str, Any],
    record: RawRecord,
    snapshot: SourceSnapshot,
    mapper: MapperVersion,
    field_name: str,
    score_type: HazardRiskScoreType,
    score_measure: NumericScoreMeasure | CategoryScoreMeasure,
    score_unit: CategoryRef,
    score_scale: MappedField[ScoreScale],
) -> HazardRiskScore:
    hex_id = _required_text(raw, "hex_id", mapper, record)

    return HazardRiskScore(
        provenance=_build_provenance(record=record, snapshot=snapshot, mapper=mapper),
        score_id=f"{hex_id}:{field_name}",
        area_key=build_hazard_risk_area_key(
            SourceId(str(snapshot.source_id)),
            DatasetId(str(snapshot.dataset_id)),
            hex_id,
        ),
        hazard_type=MappedField(
            value=_hazard_type(raw, mapper, record),
            quality=FieldQuality.STANDARDIZED,
            source_fields=("geohazard_theme",),
        ),
        source_hazard=MappedField(
            value=_source_hazard(raw, mapper, record),
            quality=FieldQuality.STANDARDIZED,
            source_fields=("geohazard_theme",),
        ),
        score_type=MappedField(
            value=score_type,
            quality=FieldQuality.STANDARDIZED,
            source_fields=(field_name,),
        ),
        source_score_type=MappedField(
            value=_category(field_name, taxonomy_id=_SCORE_FIELD_TAXONOMY_ID),
            quality=FieldQuality.STANDARDIZED,
            source_fields=(field_name,),
        ),
        score_measure=MappedField(
            value=score_measure,
            quality=FieldQuality.STANDARDIZED,
            source_fields=(field_name,),
        ),
        score_unit=MappedField(
            value=score_unit,
            quality=FieldQuality.STANDARDIZED,
            source_fields=(field_name,),
        ),
        score_scale=score_scale,
        score_direction=MappedField(
            value=HazardRiskScoreDirection.HIGHER_IS_HIGHER_RISK,
            quality=FieldQuality.STANDARDIZED,
            source_fields=(field_name,),
        ),
        methodology_label=MappedField(
            value=METHODOLOGY_LABEL,
            quality=FieldQuality.STANDARDIZED,
            source_fields=("publication_version",),
        ),
        methodology_version=MappedField(
            value=_required_text(raw, "publication_version", mapper, record),
            quality=FieldQuality.DIRECT,
            source_fields=("publication_version",),
        ),
        methodology_url=MappedField(
            value=_required_text(raw, "product_url", mapper, record),
            quality=FieldQuality.DIRECT,
            source_fields=("product_url",),
        ),
        publication_vintage=_map_publication_vintage(raw, mapper, record),
        effective_period=MappedField(value=None, quality=FieldQuality.UNMAPPED, source_fields=()),
        source_caveats=_map_source_caveats(),
    )


def _require_geosure_snapshot(
    snapshot: SourceSnapshot,
    mapper: MapperVersion,
    record: RawRecord,
) -> None:
    if snapshot.dataset_id == BGS_GEOSURE_BASIC_DATASET_ID:
        return

    raise MappingError(
        "BGS GeoSure Basic mapper requires the GeoSure Basic dataset",
        mapper=mapper,
        source_record_id=record.source_record_id,
        source_fields=("source.dataset_id",),
    )


def _map_area_identifiers(
    raw: Mapping[str, Any],
    mapper: MapperVersion,
    record: RawRecord,
) -> MappedField[tuple[SourceIdentifier, ...]]:
    return MappedField(
        value=(
            SourceIdentifier(
                value=_required_text(raw, "hex_id", mapper, record),
                identifier_kind=_category("hex_id", taxonomy_id=_AREA_IDENTIFIER_TAXONOMY_ID),
            ),
        ),
        quality=FieldQuality.DIRECT,
        source_fields=("hex_id",),
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
            query_keys=(("hex_id", _required_text(raw, "hex_id", mapper, record)),),
        ),
        quality=FieldQuality.STANDARDIZED,
        source_fields=("geometry_uri", "geometry_layer", "geometry_id", "source_crs", "hex_id"),
    )


def _map_publication_vintage(
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


def _hazard_type(
    raw: Mapping[str, Any],
    mapper: MapperVersion,
    record: RawRecord,
) -> HazardRiskHazardType:
    theme = _required_text(raw, "geohazard_theme", mapper, record).casefold()
    if theme == "landslides":
        return HazardRiskHazardType.LANDSLIDE

    return HazardRiskHazardType.SOURCE_SPECIFIC


def _source_hazard(raw: Mapping[str, Any], mapper: MapperVersion, record: RawRecord) -> CategoryRef:
    theme = _required_text(raw, "geohazard_theme", mapper, record)

    return _category(theme, taxonomy_id=_HAZARD_TAXONOMY_ID, label=theme)


def _map_source_caveats() -> MappedField[tuple[CategoryRef, ...]]:
    return MappedField(
        value=bgs_geosure_basic_caveat_categories(),
        quality=FieldQuality.STANDARDIZED,
        source_fields=(BGS_GEOSURE_BASIC_METADATA_SOURCE_FIELD,),
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
        taxonomy_version=BGS_GEOSURE_BASIC_TAXONOMY_VERSION,
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


def _mapping_report(
    raw: Mapping[str, Any],
    record: BaseModel | tuple[BaseModel, ...],
) -> MappingReport:
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
