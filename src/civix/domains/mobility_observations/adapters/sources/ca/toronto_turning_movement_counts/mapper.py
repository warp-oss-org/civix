"""Toronto turning-movement-count mappers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Final

from civix.core.identity.models.identifiers import MapperId
from civix.core.mapping.models.mapper import MapResult
from civix.core.mapping.parsers import require_text, str_or_none
from civix.core.provenance.models.provenance import MapperVersion
from civix.core.quality.models.fields import FieldQuality, MappedField
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.spatial.models.location import Address
from civix.core.taxonomy.models.category import CategoryRef
from civix.domains.mobility_observations.adapters.sources.ca._toronto_common import (
    DATASET_CONTEXT_FIELD,
    SOURCE_CAVEATS_FIELD,
    build_provenance,
    category,
    decimal_optional_nonnegative,
    mapping_report,
    point_footprint,
    source_date_period,
    source_datetime_interval,
    text_id,
)
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

COUNT_MAPPER_ID: Final[MapperId] = MapperId("toronto-turning-movement-counts")
SITE_MAPPER_ID: Final[MapperId] = MapperId("toronto-turning-movement-sites")
MAPPER_VERSION: Final[str] = "0.1.0"
_CAVEAT_TAXONOMY_ID: Final[str] = "toronto-turning-movement-caveats"
_TAXONOMY_VERSION: Final[str] = "2026-05-02"


@dataclass(frozen=True, slots=True)
class _TmcCountColumn:
    source_field: str
    travel_mode: TravelMode
    direction: ObservationDirection
    movement_type: MovementType
    caveat_codes: tuple[str, ...] = ()


_APPROACH_DIRECTIONS: Final[dict[str, ObservationDirection]] = {
    "n": ObservationDirection.SOUTHBOUND,
    "s": ObservationDirection.NORTHBOUND,
    "e": ObservationDirection.WESTBOUND,
    "w": ObservationDirection.EASTBOUND,
}
_VEHICLE_MODE_FIELDS: Final[tuple[tuple[str, TravelMode], ...]] = (
    ("cars", TravelMode.PASSENGER_CAR),
    ("truck", TravelMode.TRUCK),
    ("bus", TravelMode.BUS),
)
_MOVEMENT_FIELDS: Final[tuple[tuple[str, MovementType], ...]] = (
    ("r", MovementType.RIGHT_TURN),
    ("t", MovementType.THROUGH),
    ("l", MovementType.LEFT_TURN),
)
_BICYCLE_CAVEAT_CODE: Final[str] = "post-2023-bicycle-definition"

TMC_COUNT_COLUMNS: Final[tuple[_TmcCountColumn, ...]] = (
    *(
        _TmcCountColumn(
            source_field=f"{approach}_appr_{mode_code}_{movement_code}",
            travel_mode=mode,
            direction=direction,
            movement_type=movement,
        )
        for approach, direction in _APPROACH_DIRECTIONS.items()
        for mode_code, mode in _VEHICLE_MODE_FIELDS
        for movement_code, movement in _MOVEMENT_FIELDS
    ),
    *(
        _TmcCountColumn(
            source_field=f"{approach}_appr_peds",
            travel_mode=TravelMode.PEDESTRIAN,
            direction=ObservationDirection.SOURCE_SPECIFIC,
            movement_type=MovementType.CROSSING,
        )
        for approach in _APPROACH_DIRECTIONS
    ),
    *(
        _TmcCountColumn(
            source_field=f"{approach}_appr_bike",
            travel_mode=TravelMode.BICYCLE,
            direction=direction,
            movement_type=MovementType.ENTERING,
            caveat_codes=(_BICYCLE_CAVEAT_CODE,),
        )
        for approach, direction in _APPROACH_DIRECTIONS.items()
    ),
)


@dataclass(frozen=True, slots=True)
class TorontoTmcSiteMapper:
    """Maps Toronto TMC summary rows to observation sites."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=SITE_MAPPER_ID, version=MAPPER_VERSION)

    def __call__(
        self,
        record: RawRecord,
        snapshot: SourceSnapshot,
    ) -> MapResult[MobilityObservationSite]:
        raw = record.raw_data
        # Summary aggregate volume fields stay raw context; observations come from 15-minute rows.
        site = MobilityObservationSite(
            provenance=build_provenance(record=record, snapshot=snapshot, mapper=self.version),
            site_id=text_id(
                raw,
                "count_id",
                mapper=self.version,
                source_record_id=record.source_record_id,
            ),
            kind=MappedField(
                value=MobilitySiteKind.INTERSECTION,
                quality=FieldQuality.INFERRED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            footprint=point_footprint(
                raw=raw,
                latitude_field="latitude",
                longitude_field="longitude",
            ),
            address=_address(raw, snapshot),
            road_names=_road_names(raw),
            direction=MappedField(value=None, quality=FieldQuality.UNMAPPED, source_fields=()),
            movement_type=MappedField(
                value=MovementType.ALL_MOVEMENTS,
                quality=FieldQuality.INFERRED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            active_period=MappedField(
                value=source_date_period(
                    raw,
                    "count_date",
                    mapper=self.version,
                    source_record_id=record.source_record_id,
                ),
                quality=FieldQuality.STANDARDIZED,
                source_fields=("count_date",),
            ),
            measurement_method=MappedField(
                value=MeasurementMethod.OTHER,
                quality=FieldQuality.INFERRED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            source_caveats=_summary_caveats(raw),
        )

        return MapResult(record=site, report=mapping_report(raw, site))


@dataclass(frozen=True, slots=True)
class TorontoTmcRawCountMapper:
    """Maps one Toronto TMC raw row to zero or more count observations."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=COUNT_MAPPER_ID, version=MAPPER_VERSION)

    def __call__(
        self,
        record: RawRecord,
        snapshot: SourceSnapshot,
    ) -> MapResult[tuple[MobilityCountObservation, ...]]:
        raw = record.raw_data
        count_id = text_id(
            raw,
            "count_id",
            mapper=self.version,
            source_record_id=record.source_record_id,
        )
        observations = tuple(
            observation
            for spec in TMC_COUNT_COLUMNS
            if (
                observation := self._map_column(
                    spec=spec,
                    count_id=count_id,
                    record=record,
                    snapshot=snapshot,
                )
            )
            is not None
        )

        return MapResult(record=observations, report=mapping_report(raw, observations))

    def _map_column(
        self,
        *,
        spec: _TmcCountColumn,
        count_id: str,
        record: RawRecord,
        snapshot: SourceSnapshot,
    ) -> MobilityCountObservation | None:
        raw = record.raw_data
        value = decimal_optional_nonnegative(
            raw,
            spec.source_field,
            mapper=self.version,
            source_record_id=record.source_record_id,
        )

        if value is None:
            return None

        return MobilityCountObservation(
            provenance=build_provenance(record=record, snapshot=snapshot, mapper=self.version),
            observation_id=_observation_id(
                raw,
                count_id=count_id,
                source_field=spec.source_field,
                mapper=self.version,
                source_record_id=record.source_record_id,
            ),
            site_id=count_id,
            period=MappedField(
                value=source_datetime_interval(
                    raw,
                    start_field="start_time",
                    end_field="end_time",
                    mapper=self.version,
                    source_record_id=record.source_record_id,
                ),
                quality=FieldQuality.STANDARDIZED,
                source_fields=("start_time", "end_time"),
            ),
            travel_mode=MappedField(
                value=spec.travel_mode,
                quality=FieldQuality.STANDARDIZED,
                source_fields=(spec.source_field,),
            ),
            direction=MappedField(
                value=spec.direction,
                quality=FieldQuality.STANDARDIZED,
                source_fields=(spec.source_field,),
            ),
            movement_type=MappedField(
                value=spec.movement_type,
                quality=FieldQuality.STANDARDIZED,
                source_fields=(spec.source_field,),
            ),
            measurement_method=MappedField(
                value=MeasurementMethod.OTHER,
                quality=FieldQuality.INFERRED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            aggregation_window=MappedField(
                value=AggregationWindow.RAW_INTERVAL,
                quality=FieldQuality.INFERRED,
                source_fields=("start_time", "end_time"),
            ),
            metric_type=MappedField(
                value=CountMetricType.RAW_COUNT,
                quality=FieldQuality.INFERRED,
                source_fields=(spec.source_field,),
            ),
            unit=MappedField(
                value=CountUnit.COUNT,
                quality=FieldQuality.INFERRED,
                source_fields=(spec.source_field,),
            ),
            value=MappedField[Decimal](
                value=value,
                quality=FieldQuality.DIRECT,
                source_fields=(spec.source_field,),
            ),
            source_caveats=_raw_count_caveats(spec),
        )


def _observation_id(
    raw: Mapping[str, Any],
    *,
    count_id: str,
    source_field: str,
    mapper: MapperVersion,
    source_record_id: str | None,
) -> str:
    start_time = require_text(
        raw.get("start_time"),
        field_name="start_time",
        mapper=mapper,
        source_record_id=source_record_id,
    )

    return f"{count_id}:{start_time}:{source_field}"


def _address(raw: Mapping[str, Any], snapshot: SourceSnapshot) -> MappedField[Address]:
    return MappedField(
        value=Address(
            country=snapshot.jurisdiction.country,
            region=snapshot.jurisdiction.region,
            locality=snapshot.jurisdiction.locality,
            street=str_or_none(raw.get("location_name")),
        ),
        quality=FieldQuality.DERIVED,
        source_fields=("location_name",),
    )


def _road_names(raw: Mapping[str, Any]) -> MappedField[tuple[str, ...]]:
    location_name = str_or_none(raw.get("location_name"))

    if location_name is None:
        return MappedField(
            value=None, quality=FieldQuality.NOT_PROVIDED, source_fields=("location_name",)
        )

    names = tuple(part.strip() for part in location_name.split("/") if part.strip())

    return MappedField(value=names, quality=FieldQuality.DERIVED, source_fields=("location_name",))


def _summary_caveats(raw: Mapping[str, Any]) -> MappedField[tuple[CategoryRef, ...]]:
    caveats = [
        _caveat("ad-hoc-coverage", "Ad hoc collection; coverage is not comprehensive."),
        _caveat("method-not-row-level", "Collection method is not published at row level."),
    ]
    count_duration = str_or_none(raw.get("count_duration"))

    if count_duration is not None:
        caveats.append(
            _caveat(f"count-duration-{count_duration}", f"Count duration {count_duration}")
        )

    return MappedField(
        value=tuple(caveats),
        quality=FieldQuality.INFERRED,
        source_fields=("count_duration", SOURCE_CAVEATS_FIELD),
    )


def _raw_count_caveats(spec: _TmcCountColumn) -> MappedField[tuple[CategoryRef, ...]]:
    caveats = (
        _caveat("ad-hoc-coverage", "Ad hoc collection; coverage is not comprehensive."),
        *(_caveat(code, _caveat_label(code)) for code in spec.caveat_codes),
    )

    return MappedField(
        value=caveats,
        quality=FieldQuality.INFERRED,
        source_fields=(spec.source_field, SOURCE_CAVEATS_FIELD),
    )


def _caveat(code: str, label: str) -> CategoryRef:
    return category(
        taxonomy_id=_CAVEAT_TAXONOMY_ID,
        taxonomy_version=_TAXONOMY_VERSION,
        code=code,
        label=label,
    )


def _caveat_label(code: str) -> str:
    if code == _BICYCLE_CAVEAT_CODE:
        return "Post-September-2023 bicycle counts may include crosswalk-area bicycle movement."

    return code
