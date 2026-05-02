"""France TMJA road-traffic mappers."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from typing import Any, Final, cast

from pydantic import BaseModel

from civix.core.identity.models.identifiers import MapperId
from civix.core.mapping.errors import MappingError
from civix.core.mapping.models.mapper import MappingReport, MapResult
from civix.core.mapping.parsers import int_or_none, require_text, str_or_none
from civix.core.provenance.models.provenance import MapperVersion, ProvenanceRef
from civix.core.quality.models.fields import FieldQuality, MappedField
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.spatial.models.geometry import SpatialFootprint
from civix.core.spatial.models.location import Address
from civix.core.taxonomy.models.category import CategoryRef
from civix.core.temporal import TemporalPeriod, TemporalPeriodPrecision, TemporalTimezoneStatus
from civix.domains.mobility_observations.models.common import (
    AggregationWindow,
    CountMetricType,
    CountUnit,
    MeasurementMethod,
    MobilitySiteKind,
    MovementType,
    ObservationDirection,
    TravelMode,
)
from civix.domains.mobility_observations.models.count import MobilityCountObservation
from civix.domains.mobility_observations.models.site import MobilityObservationSite

SITE_MAPPER_ID: Final[MapperId] = MapperId("fr-tmja-road-segments")
COUNT_MAPPER_ID: Final[MapperId] = MapperId("fr-tmja-counts")
MAPPER_VERSION: Final[str] = "0.1.0"

DATASET_CONTEXT_FIELD: Final[str] = "snapshot.dataset_id"
SOURCE_CAVEATS_FIELD: Final[str] = "source.caveats"
PROJECTED_COORDINATE_FIELDS: Final[tuple[str, ...]] = ("xD", "yD", "zD", "xF", "yF", "zF")

CAVEAT_TAXONOMY_ID: Final[str] = "fr-tmja-caveats"
TAXONOMY_VERSION: Final[str] = "2025-08-18"

ANNUAL_AVERAGE_CAVEAT_CODE: Final[str] = "tmja-annual-average-not-raw-count"
ANNUAL_AVERAGE_CAVEAT_LABEL: Final[str] = (
    "TMJA is annual-average daily traffic, not a raw observed daily count."
)
BIDIRECTIONAL_CAVEAT_CODE: Final[str] = "tmja-bidirectional-all-directions"
BIDIRECTIONAL_CAVEAT_LABEL: Final[str] = (
    "The source defines TMJA as all directions combined for each road section."
)
RRNC_COVERAGE_CAVEAT_CODE: Final[str] = "rrnc-concessioned-national-network-only"
RRNC_COVERAGE_CAVEAT_LABEL: Final[str] = (
    "The selected TMJA_RRNc_2024 resource covers the concessioned national road network."
)
RATIO_PL_RAW_CAVEAT_CODE: Final[str] = "ratio-pl-left-raw-not-derived-truck-count"
RATIO_PL_RAW_CAVEAT_LABEL: Final[str] = (
    "ratio_PL is preserved as a raw heavy-goods-vehicle percentage and is not mapped into "
    "a derived truck count."
)
PROJECTED_COORDINATES_CAVEAT_CODE: Final[str] = (
    "projected-coordinates-left-raw-reprojection-deferred"
)
PROJECTED_COORDINATES_CAVEAT_LABEL: Final[str] = (
    "Projected source coordinates are preserved raw; normalized WGS84 line geometry is deferred."
)

_SITE_ID_FIELDS: Final[tuple[str, ...]] = ("route", "cumulD", "cumulF", "cote")


class FrTmjaRoadSegmentSiteMapper:
    """Maps one TMJA road-section row to a mobility observation site."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=SITE_MAPPER_ID, version=MAPPER_VERSION)

    def __call__(
        self,
        record: RawRecord,
        snapshot: SourceSnapshot,
    ) -> MapResult[MobilityObservationSite]:
        raw = record.raw_data
        site = MobilityObservationSite(
            provenance=_provenance(record=record, snapshot=snapshot, mapper=self.version),
            site_id=_site_id(
                raw,
                mapper=self.version,
                source_record_id=record.source_record_id,
            ),
            kind=MappedField(
                value=MobilitySiteKind.ROAD_SEGMENT,
                quality=FieldQuality.INFERRED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            footprint=_unmapped_projected_footprint(),
            address=MappedField(
                value=Address(country="FR"),
                quality=FieldQuality.INFERRED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            road_names=_road_names(raw),
            direction=MappedField(
                value=ObservationDirection.BIDIRECTIONAL,
                quality=FieldQuality.INFERRED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            movement_type=MappedField[MovementType](
                value=None,
                quality=FieldQuality.NOT_PROVIDED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            active_period=_year_period(raw),
            measurement_method=_measurement_method(raw),
            source_caveats=_site_caveats(),
        )

        return MapResult(record=site, report=_mapping_report(raw, site))


class FrTmjaCountMapper:
    """Maps one TMJA row to one annual-average count observation."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=COUNT_MAPPER_ID, version=MAPPER_VERSION)

    def __call__(
        self,
        record: RawRecord,
        snapshot: SourceSnapshot,
    ) -> MapResult[MobilityCountObservation]:
        raw = record.raw_data
        site_id = _site_id(
            raw,
            mapper=self.version,
            source_record_id=record.source_record_id,
        )
        year = _require_year(
            raw,
            mapper=self.version,
            source_record_id=record.source_record_id,
        )
        observation = MobilityCountObservation(
            provenance=_provenance(record=record, snapshot=snapshot, mapper=self.version),
            observation_id=f"{site_id}:{year}:TMJA",
            site_id=site_id,
            period=MappedField(
                value=_period_from_year(year),
                quality=FieldQuality.STANDARDIZED,
                source_fields=("anneeMesureTrafic",),
            ),
            travel_mode=MappedField(
                value=TravelMode.VEHICLE,
                quality=FieldQuality.INFERRED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            direction=MappedField(
                value=ObservationDirection.BIDIRECTIONAL,
                quality=FieldQuality.INFERRED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            movement_type=MappedField[MovementType](
                value=None,
                quality=FieldQuality.NOT_PROVIDED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            measurement_method=_measurement_method(raw),
            aggregation_window=MappedField(
                value=AggregationWindow.ANNUAL_AVERAGE_DAILY,
                quality=FieldQuality.INFERRED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            metric_type=MappedField(
                value=CountMetricType.TMJA,
                quality=FieldQuality.INFERRED,
                source_fields=("TMJA",),
            ),
            unit=MappedField(
                value=CountUnit.VEHICLES_PER_DAY,
                quality=FieldQuality.INFERRED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            value=MappedField(
                value=_decimal_required_nonnegative(
                    raw,
                    "TMJA",
                    mapper=self.version,
                    source_record_id=record.source_record_id,
                ),
                quality=FieldQuality.DIRECT,
                source_fields=("TMJA",),
            ),
            source_caveats=_observation_caveats(),
        )

        return MapResult(record=observation, report=_mapping_report(raw, observation))


def _provenance(
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


def _site_id(
    raw: Mapping[str, Any],
    *,
    mapper: MapperVersion,
    source_record_id: str | None,
) -> str:
    parts = tuple(
        require_text(
            raw.get(field_name),
            field_name=field_name,
            mapper=mapper,
            source_record_id=source_record_id,
        )
        for field_name in _SITE_ID_FIELDS
    )

    return ":".join(parts)


def _road_names(raw: Mapping[str, Any]) -> MappedField[tuple[str, ...]]:
    route = str_or_none(raw.get("route"))

    if route is None:
        return MappedField[tuple[str, ...]](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("route",),
        )

    return MappedField(value=(route,), quality=FieldQuality.DIRECT, source_fields=("route",))


def _unmapped_projected_footprint() -> MappedField[SpatialFootprint]:
    return MappedField[SpatialFootprint](
        value=None,
        quality=FieldQuality.UNMAPPED,
        source_fields=(),
    )


def _year_period(raw: Mapping[str, Any]) -> MappedField[TemporalPeriod]:
    year = int_or_none(raw.get("anneeMesureTrafic"))

    if year is None or year < 1:
        return MappedField[TemporalPeriod](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("anneeMesureTrafic",),
        )

    return MappedField(
        value=_period_from_year(year),
        quality=FieldQuality.STANDARDIZED,
        source_fields=("anneeMesureTrafic",),
    )


def _period_from_year(year: int) -> TemporalPeriod:
    return TemporalPeriod(
        precision=TemporalPeriodPrecision.YEAR,
        year_value=year,
        timezone_status=TemporalTimezoneStatus.UNKNOWN,
    )


def _require_year(
    raw: Mapping[str, Any],
    *,
    mapper: MapperVersion,
    source_record_id: str | None,
) -> int:
    year = int_or_none(raw.get("anneeMesureTrafic"))

    if year is None or year < 1:
        raise MappingError(
            "invalid year source field 'anneeMesureTrafic'",
            mapper=mapper,
            source_record_id=source_record_id,
            source_fields=("anneeMesureTrafic",),
        )

    return year


def _measurement_method(raw: Mapping[str, Any]) -> MappedField[MeasurementMethod]:
    method_code = str_or_none(raw.get("typeComptageTrafic"))

    if method_code == "1":
        return MappedField(
            value=MeasurementMethod.AUTOMATED_COUNTER,
            quality=FieldQuality.STANDARDIZED,
            source_fields=("typeComptageTrafic", "typeComptageTrafic_lib"),
        )

    if method_code is None:
        return MappedField[MeasurementMethod](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("typeComptageTrafic", "typeComptageTrafic_lib"),
        )

    return MappedField(
        value=MeasurementMethod.OTHER,
        quality=FieldQuality.INFERRED,
        source_fields=("typeComptageTrafic", "typeComptageTrafic_lib"),
    )


def _decimal_required_nonnegative(
    raw: Mapping[str, Any],
    field_name: str,
    *,
    mapper: MapperVersion,
    source_record_id: str | None,
) -> Decimal:
    text = str_or_none(raw.get(field_name))

    if text is None:
        raise MappingError(
            f"missing required source field {field_name!r}",
            mapper=mapper,
            source_record_id=source_record_id,
            source_fields=(field_name,),
        )

    try:
        value = Decimal(text.replace(",", "."))
    except InvalidOperation as e:
        raise MappingError(
            f"invalid numeric source field {field_name!r}",
            mapper=mapper,
            source_record_id=source_record_id,
            source_fields=(field_name,),
        ) from e

    if value < 0:
        raise MappingError(
            f"negative numeric source field {field_name!r}",
            mapper=mapper,
            source_record_id=source_record_id,
            source_fields=(field_name,),
        )

    return value


def _site_caveats() -> MappedField[tuple[CategoryRef, ...]]:
    return MappedField(
        value=(
            _caveat(RRNC_COVERAGE_CAVEAT_CODE),
            _caveat(PROJECTED_COORDINATES_CAVEAT_CODE),
        ),
        quality=FieldQuality.INFERRED,
        source_fields=(SOURCE_CAVEATS_FIELD,),
    )


def _observation_caveats() -> MappedField[tuple[CategoryRef, ...]]:
    return MappedField(
        value=(
            _caveat(ANNUAL_AVERAGE_CAVEAT_CODE),
            _caveat(BIDIRECTIONAL_CAVEAT_CODE),
            _caveat(RRNC_COVERAGE_CAVEAT_CODE),
            _caveat(RATIO_PL_RAW_CAVEAT_CODE),
        ),
        quality=FieldQuality.INFERRED,
        source_fields=(SOURCE_CAVEATS_FIELD,),
    )


def _caveat(code: str) -> CategoryRef:
    return CategoryRef(
        taxonomy_id=CAVEAT_TAXONOMY_ID,
        taxonomy_version=TAXONOMY_VERSION,
        code=code,
        label=_caveat_label(code),
    )


def _caveat_label(code: str) -> str:
    if code == ANNUAL_AVERAGE_CAVEAT_CODE:
        return ANNUAL_AVERAGE_CAVEAT_LABEL

    if code == BIDIRECTIONAL_CAVEAT_CODE:
        return BIDIRECTIONAL_CAVEAT_LABEL

    if code == RRNC_COVERAGE_CAVEAT_CODE:
        return RRNC_COVERAGE_CAVEAT_LABEL

    if code == RATIO_PL_RAW_CAVEAT_CODE:
        return RATIO_PL_RAW_CAVEAT_LABEL

    if code == PROJECTED_COORDINATES_CAVEAT_CODE:
        return PROJECTED_COORDINATES_CAVEAT_LABEL

    return code


def _mapping_report(
    raw: Mapping[str, Any],
    record: BaseModel,
) -> MappingReport:
    consumed: set[str] = set()
    _collect_source_fields(record, consumed)
    unmapped_source_fields = tuple(sorted(set(raw) - consumed))

    return MappingReport(unmapped_source_fields=unmapped_source_fields)


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
