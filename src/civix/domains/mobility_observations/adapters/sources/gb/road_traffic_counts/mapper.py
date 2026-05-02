"""Great Britain DfT road-traffic-counts mappers.

Two mappers in one slice mirror the Toronto TMC pattern:

- `GbDftCountPointSiteMapper` turns a count-point row into one
  `MobilityObservationSite` (kind=TRAFFIC_COUNT_POINT).
- `GbDftAadfCountMapper` turns one AADF-by-direction row into a tuple of
  `MobilityCountObservation`s, one per supported vehicle-class column.

Helpers are inlined here rather than promoted to a `_gb_common.py`:
this is the only GB slice today, and the existing regional commons
(`_chicago_common.py`, `_nyc_common.py`, `_toronto_common.py`) explicitly
document that duplication is preferred over premature promotion.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Final, cast

from pydantic import BaseModel, ValidationError

from civix.core.identity.models.identifiers import MapperId
from civix.core.mapping.errors import MappingError
from civix.core.mapping.models.mapper import MappingReport, MapResult
from civix.core.mapping.parsers import float_or_none, int_or_none, require_text, str_or_none
from civix.core.provenance.models.provenance import MapperVersion, ProvenanceRef
from civix.core.quality.models.fields import FieldQuality, MappedField
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.spatial.models.geometry import SpatialFootprint
from civix.core.spatial.models.location import Address, Coordinate
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

SITE_MAPPER_ID: Final[MapperId] = MapperId("gb-dft-road-traffic-count-points")
COUNT_MAPPER_ID: Final[MapperId] = MapperId("gb-dft-road-traffic-aadf-counts")
MAPPER_VERSION: Final[str] = "0.1.0"

DATASET_CONTEXT_FIELD: Final[str] = "snapshot.dataset_id"
SOURCE_CAVEATS_FIELD: Final[str] = "source.caveats"

CAVEAT_TAXONOMY_ID: Final[str] = "gb-dft-road-traffic-caveats"
ESTIMATION_DETAIL_TAXONOMY_ID: Final[str] = "gb-dft-estimation-method-detailed"
TAXONOMY_VERSION: Final[str] = "2026-05-02"

ALL_MOTOR_VEHICLES_FIELD: Final[str] = "all_motor_vehicles"
MAJOR_ROADS_CAVEAT_CODE: Final[str] = "major-roads-only-endpoint"
MAJOR_ROADS_CAVEAT_LABEL: Final[str] = (
    "DfT API endpoints used here serve Major roads only; minor-road coverage requires "
    "the bulk CSV release."
)
HGV_SUBCLASS_CAVEAT_CODE: Final[str] = "hgv-subclasses-rolled-up-to-all-hgvs"
HGV_SUBCLASS_CAVEAT_LABEL: Final[str] = (
    "DfT publishes per-axle HGV sub-class counts; this mapper emits only all_hgvs to "
    "avoid double-counting under naive aggregation."
)
TWO_WHEELED_CAVEAT_CODE: Final[str] = "two-wheeled-motor-vehicles-not-modeled"
TWO_WHEELED_CAVEAT_LABEL: Final[str] = (
    "two-wheeled motor vehicles (motorcycles/scooters) have no normalized TravelMode; "
    "emitted as OTHER."
)
LGV_CAVEAT_CODE: Final[str] = "lgvs-are-vans-not-trucks"
LGV_CAVEAT_LABEL: Final[str] = (
    "DfT light goods vehicles (LGVs) are vans, not trucks; emitted as OTHER to avoid "
    "collapsing them into TRUCK alongside HGVs."
)

# DfT publishes "C" for the combined-direction (undivided) link rows alongside the
# four cardinal codes. Treated as bidirectional.
_DIRECTION_MAP: Final[dict[str, ObservationDirection]] = {
    "n": ObservationDirection.NORTHBOUND,
    "s": ObservationDirection.SOUTHBOUND,
    "e": ObservationDirection.EASTBOUND,
    "w": ObservationDirection.WESTBOUND,
    "c": ObservationDirection.BIDIRECTIONAL,
}

_HGV_SUBCLASS_FIELDS: Final[tuple[str, ...]] = (
    "hgvs_2_rigid_axle",
    "hgvs_3_rigid_axle",
    "hgvs_4_or_more_rigid_axle",
    "hgvs_3_or_4_articulated_axle",
    "hgvs_5_articulated_axle",
    "hgvs_6_articulated_axle",
)


@dataclass(frozen=True, slots=True)
class _AadfVehicleClassColumn:
    source_field: str
    travel_mode: TravelMode
    caveat_codes: tuple[str, ...] = ()


# Order is fixed so observation tuples and tests are deterministic.
AADF_VEHICLE_CLASS_COLUMNS: Final[tuple[_AadfVehicleClassColumn, ...]] = (
    _AadfVehicleClassColumn(source_field="pedal_cycles", travel_mode=TravelMode.BICYCLE),
    _AadfVehicleClassColumn(
        source_field="two_wheeled_motor_vehicles",
        travel_mode=TravelMode.OTHER,
        caveat_codes=(TWO_WHEELED_CAVEAT_CODE,),
    ),
    _AadfVehicleClassColumn(source_field="cars_and_taxis", travel_mode=TravelMode.PASSENGER_CAR),
    _AadfVehicleClassColumn(source_field="buses_and_coaches", travel_mode=TravelMode.BUS),
    _AadfVehicleClassColumn(
        source_field="lgvs",
        travel_mode=TravelMode.OTHER,
        caveat_codes=(LGV_CAVEAT_CODE,),
    ),
    _AadfVehicleClassColumn(
        source_field="all_hgvs",
        travel_mode=TravelMode.TRUCK,
        caveat_codes=(HGV_SUBCLASS_CAVEAT_CODE,),
    ),
)
_AADF_SUPPORTED_FIELD_NAMES: Final[frozenset[str]] = frozenset(
    column.source_field for column in AADF_VEHICLE_CLASS_COLUMNS
)


@dataclass(frozen=True, slots=True)
class _EstimationMethodDetailMapping:
    detail_value: str
    method: MeasurementMethod


# Pinned against fixture rows; values come from real DfT records sampled across
# multiple pages of the AADF endpoint on 2026-05-02. Unknown detail values fall
# back to INFERRED based on `estimation_method` ("Counted" vs "Estimated").
ESTIMATION_METHOD_DETAIL_MAPPINGS: Final[tuple[_EstimationMethodDetailMapping, ...]] = (
    _EstimationMethodDetailMapping(
        detail_value="Manual count",
        method=MeasurementMethod.MANUAL_COUNT,
    ),
    _EstimationMethodDetailMapping(
        detail_value="Estimated using AADF from previous year on this link",
        method=MeasurementMethod.ANNUALIZED_ESTIMATE,
    ),
    _EstimationMethodDetailMapping(
        detail_value="Estimated from nearby links",
        method=MeasurementMethod.ANNUALIZED_ESTIMATE,
    ),
)
_DETAIL_LOOKUP: Final[dict[str, MeasurementMethod]] = {
    mapping.detail_value: mapping.method for mapping in ESTIMATION_METHOD_DETAIL_MAPPINGS
}


@dataclass(frozen=True, slots=True)
class GbDftCountPointSiteMapper:
    """Maps DfT count-point rows to observation sites."""

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
            site_id=_text_id(
                raw,
                "count_point_id",
                mapper=self.version,
                source_record_id=record.source_record_id,
            ),
            kind=MappedField(
                value=MobilitySiteKind.TRAFFIC_COUNT_POINT,
                quality=FieldQuality.INFERRED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            footprint=_point_footprint(
                raw=raw,
                latitude_field="latitude",
                longitude_field="longitude",
            ),
            address=_address(raw),
            road_names=_road_names(raw),
            direction=MappedField[ObservationDirection](
                value=None,
                quality=FieldQuality.NOT_PROVIDED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            movement_type=MappedField[MovementType](
                value=None,
                quality=FieldQuality.NOT_PROVIDED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            active_period=_active_year_period(raw),
            measurement_method=MappedField[MeasurementMethod](
                value=None,
                quality=FieldQuality.NOT_PROVIDED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            source_caveats=_site_caveats(),
        )

        return MapResult(record=site, report=_mapping_report(raw, site))


@dataclass(frozen=True, slots=True)
class GbDftAadfCountMapper:
    """Maps one DfT AADF-by-direction row to one observation per vehicle class.

    `all_motor_vehicles` is intentionally not emitted: it is the published sum of
    the class columns, and emitting it alongside per-class observations would
    double-count under naive aggregation. The HGV per-axle sub-class columns are
    likewise not emitted; only `all_hgvs` is, with a caveat noting the rollup.
    """

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=COUNT_MAPPER_ID, version=MAPPER_VERSION)

    def __call__(
        self,
        record: RawRecord,
        snapshot: SourceSnapshot,
    ) -> MapResult[tuple[MobilityCountObservation, ...]]:
        raw = record.raw_data
        count_point_id = _text_id(
            raw,
            "count_point_id",
            mapper=self.version,
            source_record_id=record.source_record_id,
        )
        year = _require_year(
            raw,
            "year",
            mapper=self.version,
            source_record_id=record.source_record_id,
        )
        period = TemporalPeriod(
            precision=TemporalPeriodPrecision.YEAR,
            year_value=year,
            timezone_status=TemporalTimezoneStatus.UNKNOWN,
        )
        direction = _map_direction(raw.get("direction_of_travel"), "direction_of_travel")
        method = _measurement_method(
            raw,
            mapper=self.version,
            source_record_id=record.source_record_id,
        )
        detail_caveat = _estimation_detail_caveat(raw)
        observations = tuple(
            observation
            for column in AADF_VEHICLE_CLASS_COLUMNS
            if (
                observation := self._map_column(
                    column=column,
                    raw=raw,
                    record=record,
                    snapshot=snapshot,
                    count_point_id=count_point_id,
                    year=year,
                    period=period,
                    direction=direction,
                    method=method,
                    detail_caveat=detail_caveat,
                )
            )
            is not None
        )

        return MapResult(record=observations, report=_mapping_report(raw, observations))

    def _map_column(
        self,
        *,
        column: _AadfVehicleClassColumn,
        raw: Mapping[str, Any],
        record: RawRecord,
        snapshot: SourceSnapshot,
        count_point_id: str,
        year: int,
        period: TemporalPeriod,
        direction: MappedField[ObservationDirection],
        method: MappedField[MeasurementMethod],
        detail_caveat: CategoryRef | None,
    ) -> MobilityCountObservation | None:
        value = _decimal_optional_nonnegative(
            raw,
            column.source_field,
            mapper=self.version,
            source_record_id=record.source_record_id,
        )

        if value is None:
            return None

        return MobilityCountObservation(
            provenance=_provenance(record=record, snapshot=snapshot, mapper=self.version),
            observation_id=f"{count_point_id}:{year}:{_direction_key(raw)}:{column.source_field}",
            site_id=count_point_id,
            period=MappedField[TemporalPeriod](
                value=period,
                quality=FieldQuality.STANDARDIZED,
                source_fields=("year",),
            ),
            travel_mode=MappedField(
                value=column.travel_mode,
                quality=FieldQuality.STANDARDIZED,
                source_fields=(column.source_field,),
            ),
            direction=direction,
            movement_type=MappedField[MovementType](
                value=None,
                quality=FieldQuality.NOT_PROVIDED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            measurement_method=method,
            aggregation_window=MappedField(
                value=AggregationWindow.ANNUAL_AVERAGE_DAILY,
                quality=FieldQuality.INFERRED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            metric_type=MappedField(
                value=CountMetricType.AADF,
                quality=FieldQuality.INFERRED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            unit=MappedField(
                value=CountUnit.VEHICLES_PER_DAY,
                quality=FieldQuality.INFERRED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            value=MappedField[Decimal](
                value=value,
                quality=FieldQuality.DIRECT,
                source_fields=(column.source_field,),
            ),
            source_caveats=_observation_caveats(column=column, detail_caveat=detail_caveat),
        )


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


def _text_id(
    raw: Mapping[str, Any],
    field_name: str,
    *,
    mapper: MapperVersion,
    source_record_id: str | None,
) -> str:
    return require_text(
        raw.get(field_name),
        field_name=field_name,
        mapper=mapper,
        source_record_id=source_record_id,
    )


def _require_year(
    raw: Mapping[str, Any],
    field_name: str,
    *,
    mapper: MapperVersion,
    source_record_id: str | None,
) -> int:
    parsed = int_or_none(raw.get(field_name))

    if parsed is None or parsed < 1:
        raise MappingError(
            f"invalid year source field {field_name!r}",
            mapper=mapper,
            source_record_id=source_record_id,
            source_fields=(field_name,),
        )

    return parsed


def _decimal_optional_nonnegative(
    raw: Mapping[str, Any],
    field_name: str,
    *,
    mapper: MapperVersion,
    source_record_id: str | None,
) -> Decimal | None:
    text = str_or_none(raw.get(field_name))

    if text is None:
        return None

    try:
        value = Decimal(text)
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


def _map_direction(raw_value: object, source_field: str) -> MappedField[ObservationDirection]:
    value = str_or_none(raw_value)

    if value is None:
        return MappedField[ObservationDirection](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=(source_field,),
        )

    mapped = _DIRECTION_MAP.get(value.casefold())

    if mapped is None:
        return MappedField[ObservationDirection](
            value=ObservationDirection.SOURCE_SPECIFIC,
            quality=FieldQuality.DERIVED,
            source_fields=(source_field,),
        )

    return MappedField[ObservationDirection](
        value=mapped,
        quality=FieldQuality.STANDARDIZED,
        source_fields=(source_field,),
    )


def _direction_key(raw: Mapping[str, Any]) -> str:
    raw_direction = str_or_none(raw.get("direction_of_travel"))

    return raw_direction or "unknown"


def _measurement_method(
    raw: Mapping[str, Any],
    *,
    mapper: MapperVersion,
    source_record_id: str | None,
) -> MappedField[MeasurementMethod]:
    detail = str_or_none(raw.get("estimation_method_detailed"))
    high_level = str_or_none(raw.get("estimation_method"))

    if detail is not None and detail in _DETAIL_LOOKUP:
        return MappedField(
            value=_DETAIL_LOOKUP[detail],
            quality=FieldQuality.STANDARDIZED,
            source_fields=("estimation_method_detailed",),
        )

    if high_level is None:
        raise MappingError(
            "missing required source field 'estimation_method'",
            mapper=mapper,
            source_record_id=source_record_id,
            source_fields=("estimation_method",),
        )

    fallback = _high_level_fallback(high_level)

    if fallback is None:
        raise MappingError(
            f"unrecognized estimation_method {high_level!r}",
            mapper=mapper,
            source_record_id=source_record_id,
            source_fields=("estimation_method",),
        )

    return MappedField(
        value=fallback,
        quality=FieldQuality.INFERRED,
        source_fields=("estimation_method",),
    )


def _high_level_fallback(value: str) -> MeasurementMethod | None:
    if value == "Counted":
        return MeasurementMethod.MANUAL_COUNT

    if value == "Estimated":
        return MeasurementMethod.ANNUALIZED_ESTIMATE

    return None


def _estimation_detail_caveat(raw: Mapping[str, Any]) -> CategoryRef | None:
    detail = str_or_none(raw.get("estimation_method_detailed"))

    if detail is None:
        return None

    return CategoryRef(
        taxonomy_id=ESTIMATION_DETAIL_TAXONOMY_ID,
        taxonomy_version=TAXONOMY_VERSION,
        code=detail,
        label=detail,
    )


def _site_caveats() -> MappedField[tuple[CategoryRef, ...]]:
    return MappedField(
        value=(
            _caveat(
                code=MAJOR_ROADS_CAVEAT_CODE,
                label=MAJOR_ROADS_CAVEAT_LABEL,
            ),
        ),
        quality=FieldQuality.INFERRED,
        source_fields=(SOURCE_CAVEATS_FIELD,),
    )


def _observation_caveats(
    *,
    column: _AadfVehicleClassColumn,
    detail_caveat: CategoryRef | None,
) -> MappedField[tuple[CategoryRef, ...]]:
    column_caveats = tuple(
        _caveat(code=code, label=_caveat_label(code)) for code in column.caveat_codes
    )

    if detail_caveat is None:
        return MappedField(
            value=column_caveats,
            quality=FieldQuality.INFERRED,
            source_fields=(SOURCE_CAVEATS_FIELD,),
        )

    return MappedField(
        value=(*column_caveats, detail_caveat),
        quality=FieldQuality.INFERRED,
        source_fields=("estimation_method_detailed", SOURCE_CAVEATS_FIELD),
    )


def _caveat(*, code: str, label: str) -> CategoryRef:
    return CategoryRef(
        taxonomy_id=CAVEAT_TAXONOMY_ID,
        taxonomy_version=TAXONOMY_VERSION,
        code=code,
        label=label,
    )


def _caveat_label(code: str) -> str:
    if code == TWO_WHEELED_CAVEAT_CODE:
        return TWO_WHEELED_CAVEAT_LABEL

    if code == LGV_CAVEAT_CODE:
        return LGV_CAVEAT_LABEL

    if code == HGV_SUBCLASS_CAVEAT_CODE:
        return HGV_SUBCLASS_CAVEAT_LABEL

    return code


def _point_footprint(
    *,
    raw: Mapping[str, Any],
    latitude_field: str,
    longitude_field: str,
) -> MappedField[SpatialFootprint]:
    latitude = float_or_none(raw.get(latitude_field))
    longitude = float_or_none(raw.get(longitude_field))
    source_fields = (latitude_field, longitude_field)

    if latitude is None or longitude is None:
        return MappedField[SpatialFootprint](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=source_fields,
        )

    try:
        footprint = SpatialFootprint(
            point=Coordinate(latitude=latitude, longitude=longitude),
        )
    except ValidationError:
        return MappedField[SpatialFootprint](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=source_fields,
        )

    return MappedField(
        value=footprint,
        quality=FieldQuality.STANDARDIZED,
        source_fields=source_fields,
    )


def _address(raw: Mapping[str, Any]) -> MappedField[Address]:
    road_name = str_or_none(raw.get("road_name"))

    return MappedField(
        value=Address(country="GB", street=road_name),
        quality=FieldQuality.DERIVED,
        source_fields=("road_name",),
    )


def _road_names(raw: Mapping[str, Any]) -> MappedField[tuple[str, ...]]:
    source_fields = ("road_name", "start_junction_road_name", "end_junction_road_name")
    parts = tuple(
        text
        for field_name in source_fields
        if (text := str_or_none(raw.get(field_name))) is not None
    )

    if not parts:
        return MappedField[tuple[str, ...]](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=source_fields,
        )

    return MappedField(value=parts, quality=FieldQuality.DERIVED, source_fields=source_fields)


def _active_year_period(raw: Mapping[str, Any]) -> MappedField[TemporalPeriod]:
    year = int_or_none(raw.get("aadf_year"))

    if year is None or year < 1:
        return MappedField[TemporalPeriod](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("aadf_year",),
        )

    return MappedField(
        value=TemporalPeriod(
            precision=TemporalPeriodPrecision.YEAR,
            year_value=year,
            timezone_status=TemporalTimezoneStatus.UNKNOWN,
        ),
        quality=FieldQuality.STANDARDIZED,
        source_fields=("aadf_year",),
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
