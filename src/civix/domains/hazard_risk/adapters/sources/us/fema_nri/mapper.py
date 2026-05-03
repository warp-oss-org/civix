"""FEMA National Risk Index mappers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Final, cast

from pydantic import BaseModel

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, MapperId, SourceId
from civix.core.mapping.errors import MappingError
from civix.core.mapping.models.mapper import MappingReport, MapResult
from civix.core.mapping.parsers import require_text, slugify, str_or_none
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
from civix.domains.hazard_risk.adapters.sources.us.fema_nri.adapter import (
    FEMA_NRI_TRACTS_LAYER_NAME,
    FEMA_NRI_TRACTS_SERVICE_URL,
    FEMA_NRI_TRACTS_SOURCE_CRS,
)
from civix.domains.hazard_risk.adapters.sources.us.fema_nri.caveats import (
    FEMA_NRI_METADATA_SOURCE_FIELD,
    fema_nri_caveat_categories,
)
from civix.domains.hazard_risk.adapters.sources.us.fema_nri.schema import (
    FEMA_NRI_TAXONOMY_VERSION,
    NRI_HAZARD_PREFIXES,
    NRI_VERSION_METADATA,
    NriVersionMetadata,
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

AREA_MAPPER_ID: Final[MapperId] = MapperId("fema-nri-tract-area")
SCORES_MAPPER_ID: Final[MapperId] = MapperId("fema-nri-tract-scores")
AREA_MAPPER_VERSION: Final[str] = "0.1.0"
SCORES_MAPPER_VERSION: Final[str] = "0.1.0"
METHODOLOGY_LABEL: Final[str] = "FEMA National Risk Index"
METHODOLOGY_URL: Final[str] = (
    "https://www.fema.gov/sites/default/files/documents/"
    "fema_national-risk-index_technical-documentation.pdf"
)

_AREA_IDENTIFIER_TAXONOMY_ID: Final[str] = "fema-nri-area-identifier"
_AREA_KIND_TAXONOMY_ID: Final[str] = "fema-nri-area-kind"
_HAZARD_TAXONOMY_ID: Final[str] = "fema-nri-hazard"
_SCORE_FIELD_TAXONOMY_ID: Final[str] = "fema-nri-score-field"
_SCORE_UNIT_TAXONOMY_ID: Final[str] = "fema-nri-score-unit"
_RATING_TAXONOMY_ID: Final[str] = "fema-nri-rating"
_SCORE_SCALE_SOURCE_FIELD: Final[str] = "source.methodology.score_scale"
_ScoreSpec = tuple[
    str,
    HazardRiskHazardType,
    str,
    HazardRiskScoreType,
    str,
    HazardRiskScoreDirection,
    bool,
]

_HAZARD_LABELS: Final[dict[str, str]] = {
    "AVLN": "Avalanche",
    "CFLD": "Coastal Flooding",
    "CWAV": "Cold Wave",
    "DRGT": "Drought",
    "ERQK": "Earthquake",
    "HAIL": "Hail",
    "HRCN": "Hurricane",
    "HWAV": "Heat Wave",
    "IFLD": "Inland Flooding",
    "ISTM": "Ice Storm",
    "LNDS": "Landslide",
    "LTNG": "Lightning",
    "SWND": "Strong Wind",
    "TRND": "Tornado",
    "TSUN": "Tsunami",
    "VLCN": "Volcanic Activity",
    "WFIR": "Wildfire",
    "WNTW": "Winter Weather",
}
_HAZARD_TYPES: Final[dict[str, HazardRiskHazardType]] = {
    "AVLN": HazardRiskHazardType.SOURCE_SPECIFIC,
    "CFLD": HazardRiskHazardType.COASTAL,
    "CWAV": HazardRiskHazardType.WINTER_WEATHER,
    "DRGT": HazardRiskHazardType.DROUGHT,
    "ERQK": HazardRiskHazardType.EARTHQUAKE,
    "HAIL": HazardRiskHazardType.STORM,
    "HRCN": HazardRiskHazardType.STORM,
    "HWAV": HazardRiskHazardType.HEAT,
    "IFLD": HazardRiskHazardType.FLOOD,
    "ISTM": HazardRiskHazardType.WINTER_WEATHER,
    "LNDS": HazardRiskHazardType.LANDSLIDE,
    "LTNG": HazardRiskHazardType.STORM,
    "SWND": HazardRiskHazardType.WIND,
    "TRND": HazardRiskHazardType.WIND,
    "TSUN": HazardRiskHazardType.SOURCE_SPECIFIC,
    "VLCN": HazardRiskHazardType.SOURCE_SPECIFIC,
    "WFIR": HazardRiskHazardType.WILDFIRE,
    "WNTW": HazardRiskHazardType.WINTER_WEATHER,
}
_CORE_SCORE_SPECS: Final[tuple[_ScoreSpec, ...]] = (
    (
        "RISK_SCORE",
        HazardRiskHazardType.MULTI_HAZARD,
        "all-hazards",
        HazardRiskScoreType.COMPOSITE_INDEX,
        "score",
        HazardRiskScoreDirection.HIGHER_IS_HIGHER_RISK,
        True,
    ),
    (
        "RISK_RATNG",
        HazardRiskHazardType.MULTI_HAZARD,
        "all-hazards",
        HazardRiskScoreType.RATING,
        "rating",
        HazardRiskScoreDirection.HIGHER_IS_HIGHER_RISK,
        False,
    ),
    (
        "RISK_SPCTL",
        HazardRiskHazardType.MULTI_HAZARD,
        "all-hazards",
        HazardRiskScoreType.PERCENTILE,
        "percentile",
        HazardRiskScoreDirection.HIGHER_IS_HIGHER_RISK,
        True,
    ),
    (
        "EAL_SCORE",
        HazardRiskHazardType.MULTI_HAZARD,
        "all-hazards",
        HazardRiskScoreType.EXPECTED_ANNUAL_LOSS,
        "score",
        HazardRiskScoreDirection.HIGHER_IS_HIGHER_RISK,
        True,
    ),
    (
        "EAL_RATNG",
        HazardRiskHazardType.MULTI_HAZARD,
        "all-hazards",
        HazardRiskScoreType.RATING,
        "rating",
        HazardRiskScoreDirection.HIGHER_IS_HIGHER_RISK,
        False,
    ),
    (
        "EAL_VALT",
        HazardRiskHazardType.MULTI_HAZARD,
        "all-hazards",
        HazardRiskScoreType.EXPECTED_ANNUAL_LOSS,
        "usd",
        HazardRiskScoreDirection.HIGHER_IS_HIGHER_RISK,
        True,
    ),
    (
        "SOVI_SCORE",
        HazardRiskHazardType.MULTI_HAZARD,
        "all-hazards",
        HazardRiskScoreType.SOCIAL_VULNERABILITY,
        "score",
        HazardRiskScoreDirection.HIGHER_IS_HIGHER_RISK,
        True,
    ),
    (
        "SOVI_RATNG",
        HazardRiskHazardType.MULTI_HAZARD,
        "all-hazards",
        HazardRiskScoreType.RATING,
        "rating",
        HazardRiskScoreDirection.HIGHER_IS_HIGHER_RISK,
        False,
    ),
    (
        "RESL_SCORE",
        HazardRiskHazardType.MULTI_HAZARD,
        "all-hazards",
        HazardRiskScoreType.COMMUNITY_RESILIENCE,
        "score",
        HazardRiskScoreDirection.HIGHER_IS_BETTER,
        True,
    ),
    (
        "RESL_RATNG",
        HazardRiskHazardType.MULTI_HAZARD,
        "all-hazards",
        HazardRiskScoreType.RATING,
        "rating",
        HazardRiskScoreDirection.HIGHER_IS_BETTER,
        False,
    ),
)


@dataclass(frozen=True, slots=True)
class FemaNriAreaMapper:
    """Maps one FEMA NRI tract row to a hazard-risk area."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=AREA_MAPPER_ID, version=AREA_MAPPER_VERSION)

    def __call__(self, record: RawRecord, snapshot: SourceSnapshot) -> MapResult[HazardRiskArea]:
        raw = record.raw_data
        tract_fips = _required_text(raw, "TRACTFIPS", self.version, record)
        area = HazardRiskArea(
            provenance=_build_provenance(record=record, snapshot=snapshot, mapper=self.version),
            area_key=build_hazard_risk_area_key(
                SourceId(str(snapshot.source_id)),
                DatasetId(str(snapshot.dataset_id)),
                tract_fips,
            ),
            source_area_identifiers=_map_area_identifiers(raw, self.version, record),
            area_kind=MappedField(
                value=HazardRiskAreaKind.CENSUS_UNIT,
                quality=FieldQuality.STANDARDIZED,
                source_fields=("TRACTFIPS",),
            ),
            source_area_kind=MappedField(
                value=_category("census-tract", taxonomy_id=_AREA_KIND_TAXONOMY_ID),
                quality=FieldQuality.STANDARDIZED,
                source_fields=("TRACTFIPS",),
            ),
            name=_map_area_name(raw, self.version, record),
            jurisdiction=_map_jurisdiction(raw, self.version, record),
            administrative_areas=_map_administrative_areas(raw, self.version, record),
            footprint=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            geometry_ref=_map_geometry_ref(raw, self.version, record),
            source_hazards=MappedField(
                value=tuple(_source_hazard(prefix) for prefix in NRI_HAZARD_PREFIXES),
                quality=FieldQuality.STANDARDIZED,
                source_fields=tuple(f"{prefix}_RISKR" for prefix in NRI_HAZARD_PREFIXES),
            ),
            source_caveats=_map_source_caveats(),
        )

        return MapResult(record=area, report=_mapping_report(raw, area))


@dataclass(frozen=True, slots=True)
class FemaNriScoresMapper:
    """Maps one FEMA NRI tract row to child hazard-risk score facts."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=SCORES_MAPPER_ID, version=SCORES_MAPPER_VERSION)

    def __call__(
        self,
        record: RawRecord,
        snapshot: SourceSnapshot,
    ) -> MapResult[tuple[HazardRiskScore, ...]]:
        raw = record.raw_data
        scores: list[HazardRiskScore] = []

        for spec in _CORE_SCORE_SPECS:
            score = _map_score_field(
                raw=raw,
                record=record,
                snapshot=snapshot,
                mapper=self.version,
                spec=spec,
            )
            if score is not None:
                scores.append(score)

        for prefix in NRI_HAZARD_PREFIXES:
            numeric_field = f"{prefix}_RISKS"
            rating_field = f"{prefix}_RISKR"
            numeric_score = _map_score_field(
                raw=raw,
                record=record,
                snapshot=snapshot,
                mapper=self.version,
                spec=(
                    numeric_field,
                    _hazard_type(prefix),
                    prefix,
                    HazardRiskScoreType.PER_HAZARD_SCORE,
                    "score",
                    HazardRiskScoreDirection.HIGHER_IS_HIGHER_RISK,
                    True,
                ),
            )
            rating_score = _map_score_field(
                raw=raw,
                record=record,
                snapshot=snapshot,
                mapper=self.version,
                spec=(
                    rating_field,
                    _hazard_type(prefix),
                    prefix,
                    HazardRiskScoreType.RATING,
                    "rating",
                    HazardRiskScoreDirection.HIGHER_IS_HIGHER_RISK,
                    False,
                ),
            )

            if numeric_score is not None:
                scores.append(numeric_score)

            if rating_score is not None:
                scores.append(rating_score)

        normalized = tuple(scores)

        return MapResult(record=normalized, report=_mapping_report(raw, normalized))


def _map_score_field(
    *,
    raw: Mapping[str, Any],
    record: RawRecord,
    snapshot: SourceSnapshot,
    mapper: MapperVersion,
    spec: _ScoreSpec,
) -> HazardRiskScore | None:
    field_name, hazard_type, source_hazard_code, score_type, unit_code, direction, numeric = spec
    source_value = raw.get(field_name)

    measure = _score_measure(source_value, field_name, mapper, record, numeric=numeric)
    if measure is None:
        return None

    tract_fips = _required_text(raw, "TRACTFIPS", mapper, record)
    area_key = build_hazard_risk_area_key(
        SourceId(str(snapshot.source_id)),
        DatasetId(str(snapshot.dataset_id)),
        tract_fips,
    )
    version_metadata = _version_metadata(raw, mapper, record)
    actual_direction = (
        HazardRiskScoreDirection.NOT_APPLICABLE
        if str_or_none(source_value) == "Not Applicable"
        else direction
    )

    return HazardRiskScore(
        provenance=_build_provenance(record=record, snapshot=snapshot, mapper=mapper),
        score_id=f"{tract_fips}:{field_name.lower()}",
        area_key=area_key,
        hazard_type=MappedField(
            value=hazard_type,
            quality=FieldQuality.STANDARDIZED,
            source_fields=(field_name,),
        ),
        source_hazard=MappedField(
            value=_source_hazard(source_hazard_code),
            quality=FieldQuality.STANDARDIZED,
            source_fields=(field_name,),
        ),
        score_type=MappedField(
            value=score_type,
            quality=FieldQuality.STANDARDIZED,
            source_fields=(field_name,),
        ),
        source_score_type=MappedField(
            value=_category(field_name.lower(), taxonomy_id=_SCORE_FIELD_TAXONOMY_ID),
            quality=FieldQuality.STANDARDIZED,
            source_fields=(field_name,),
        ),
        score_measure=MappedField(
            value=measure,
            quality=FieldQuality.STANDARDIZED,
            source_fields=(field_name,),
        ),
        score_unit=MappedField(
            value=_score_unit(unit_code),
            quality=FieldQuality.STANDARDIZED,
            source_fields=(field_name,),
        ),
        score_scale=_score_scale(unit_code),
        score_direction=MappedField(
            value=actual_direction,
            quality=FieldQuality.STANDARDIZED,
            source_fields=(field_name, "NRI_VER"),
        ),
        methodology_label=MappedField(
            value=METHODOLOGY_LABEL,
            quality=FieldQuality.STANDARDIZED,
            source_fields=("NRI_VER",),
        ),
        methodology_version=MappedField(
            value=version_metadata.methodology_version,
            quality=FieldQuality.STANDARDIZED,
            source_fields=("NRI_VER",),
        ),
        methodology_url=MappedField(
            value=METHODOLOGY_URL,
            quality=FieldQuality.STANDARDIZED,
            source_fields=("NRI_VER",),
        ),
        publication_vintage=MappedField(
            value=TemporalPeriod(
                precision=TemporalPeriodPrecision.MONTH,
                year_value=version_metadata.publication_year,
                month_value=version_metadata.publication_month,
                timezone_status=TemporalTimezoneStatus.UNKNOWN,
            ),
            quality=FieldQuality.STANDARDIZED,
            source_fields=("NRI_VER",),
        ),
        effective_period=MappedField(
            value=None,
            quality=FieldQuality.UNMAPPED,
            source_fields=(),
        ),
        source_caveats=_map_source_caveats(),
    )


def _score_measure(
    value: object,
    field_name: str,
    mapper: MapperVersion,
    record: RawRecord,
    *,
    numeric: bool,
) -> NumericScoreMeasure | CategoryScoreMeasure | None:
    text = str_or_none(value)
    if text is None:
        return None

    if not numeric:
        return CategoryScoreMeasure(
            value=CategoryRef(
                code=slugify(text),
                label=text,
                taxonomy_id=_RATING_TAXONOMY_ID,
                taxonomy_version=FEMA_NRI_TAXONOMY_VERSION,
            )
        )

    try:
        return NumericScoreMeasure(value=Decimal(str(value)))
    except (InvalidOperation, ValueError) as e:
        raise MappingError(
            f"invalid decimal value for source field {field_name!r}",
            mapper=mapper,
            source_record_id=record.source_record_id,
            source_fields=(field_name,),
        ) from e


def _map_area_identifiers(
    raw: Mapping[str, Any],
    mapper: MapperVersion,
    record: RawRecord,
) -> MappedField[tuple[SourceIdentifier, ...]]:
    identifiers = tuple(
        SourceIdentifier(
            value=_required_text(raw, field_name, mapper, record),
            identifier_kind=_category(
                field_name.lower(),
                taxonomy_id=_AREA_IDENTIFIER_TAXONOMY_ID,
            ),
        )
        for field_name in ("TRACTFIPS", "NRI_ID", "STATEFIPS", "COUNTYFIPS", "STCOFIPS")
    )

    return MappedField(
        value=identifiers,
        quality=FieldQuality.DIRECT,
        source_fields=("TRACTFIPS", "NRI_ID", "STATEFIPS", "COUNTYFIPS", "STCOFIPS"),
    )


def _map_area_name(
    raw: Mapping[str, Any],
    mapper: MapperVersion,
    record: RawRecord,
) -> MappedField[str]:
    state = _required_text(raw, "STATE", mapper, record)
    county = _required_text(raw, "COUNTY", mapper, record)
    tract = _required_text(raw, "TRACT", mapper, record)

    return MappedField(
        value=f"{state} {county} County Census Tract {tract}",
        quality=FieldQuality.DERIVED,
        source_fields=("STATE", "COUNTY", "TRACT"),
    )


def _map_jurisdiction(
    raw: Mapping[str, Any],
    mapper: MapperVersion,
    record: RawRecord,
) -> MappedField[Jurisdiction]:
    return MappedField(
        value=Jurisdiction(country="US", region=_required_text(raw, "STATEABBRV", mapper, record)),
        quality=FieldQuality.STANDARDIZED,
        source_fields=("STATEABBRV",),
    )


def _map_administrative_areas(
    raw: Mapping[str, Any],
    mapper: MapperVersion,
    record: RawRecord,
) -> MappedField[tuple[str, ...]]:
    return MappedField(
        value=(
            _required_text(raw, "STATE", mapper, record),
            _required_text(raw, "COUNTY", mapper, record),
            _required_text(raw, "TRACT", mapper, record),
        ),
        quality=FieldQuality.DIRECT,
        source_fields=("STATE", "COUNTY", "TRACT"),
    )


def _map_geometry_ref(
    raw: Mapping[str, Any],
    mapper: MapperVersion,
    record: RawRecord,
) -> MappedField[GeometryRef]:
    nri_id = _required_text(raw, "NRI_ID", mapper, record)

    return MappedField(
        value=GeometryRef(
            geometry_type=GeometryType.POLYGON,
            uri=FEMA_NRI_TRACTS_SERVICE_URL,
            layer_name=FEMA_NRI_TRACTS_LAYER_NAME,
            geometry_id=nri_id,
            source_crs=FEMA_NRI_TRACTS_SOURCE_CRS,
            query_keys=(("NRI_ID", nri_id),),
        ),
        quality=FieldQuality.STANDARDIZED,
        source_fields=("NRI_ID",),
    )


def _version_metadata(
    raw: Mapping[str, Any],
    mapper: MapperVersion,
    record: RawRecord,
) -> NriVersionMetadata:
    source_version = _required_text(raw, "NRI_VER", mapper, record)
    metadata = NRI_VERSION_METADATA.get(source_version)

    if metadata is not None:
        return metadata

    raise MappingError(
        f"unrecognized FEMA NRI version {source_version!r}",
        mapper=mapper,
        source_record_id=record.source_record_id,
        source_fields=("NRI_VER",),
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


def _hazard_type(prefix: str) -> HazardRiskHazardType:
    return _HAZARD_TYPES[prefix]


def _source_hazard(code: str) -> CategoryRef:
    if code == "all-hazards":
        return _category("all-hazards", label="All Hazards", taxonomy_id=_HAZARD_TAXONOMY_ID)

    return _category(code.lower(), label=_HAZARD_LABELS[code], taxonomy_id=_HAZARD_TAXONOMY_ID)


def _score_unit(code: str) -> CategoryRef:
    labels = {
        "score": "Score Points",
        "rating": "Rating Category",
        "percentile": "Percentile",
        "usd": "U.S. Dollars",
    }

    return _category(code, label=labels[code], taxonomy_id=_SCORE_UNIT_TAXONOMY_ID)


def _score_scale(unit_code: str) -> MappedField[ScoreScale]:
    if unit_code not in {"score", "percentile"}:
        return MappedField(value=None, quality=FieldQuality.UNMAPPED, source_fields=())

    return MappedField(
        value=ScoreScale(minimum=Decimal("0"), maximum=Decimal("100")),
        quality=FieldQuality.STANDARDIZED,
        source_fields=(_SCORE_SCALE_SOURCE_FIELD,),
    )


def _map_source_caveats() -> MappedField[tuple[CategoryRef, ...]]:
    return MappedField(
        value=fema_nri_caveat_categories(),
        quality=FieldQuality.STANDARDIZED,
        source_fields=(FEMA_NRI_METADATA_SOURCE_FIELD,),
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
        taxonomy_version=FEMA_NRI_TAXONOMY_VERSION,
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
