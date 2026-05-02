"""Chicago Traffic Tracker segments mappers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final

from civix.core.identity.models.identifiers import MapperId
from civix.core.mapping.models.mapper import MapResult
from civix.core.mapping.parsers import str_or_none
from civix.core.provenance.models.provenance import MapperVersion
from civix.core.quality.models.fields import FieldQuality, MappedField
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.spatial.models.location import Address
from civix.core.taxonomy.models.category import CategoryRef
from civix.core.temporal import TemporalPeriod
from civix.domains.mobility_observations.adapters.sources.us._chicago_common import (
    CAVEAT_TAXONOMY_ID,
    CAVEAT_TAXONOMY_VERSION,
    DATASET_CONTEXT_FIELD,
    REFRESH_NOT_INTERVAL_CAVEAT_CODE,
    REFRESH_NOT_INTERVAL_CAVEAT_LABEL,
    SOURCE_CAVEATS_FIELD,
    build_provenance,
    category,
    chicago_local_datetime_period,
    decimal_traffic_estimate,
    line_footprint,
    map_direction,
    mapping_report,
    text_id,
)
from civix.domains.mobility_observations.models.common import (
    AggregationWindow,
    MeasurementMethod,
    MobilitySiteKind,
    SpeedMetricType,
    SpeedUnit,
    TravelMode,
)
from civix.domains.mobility_observations.models.site import MobilityObservationSite
from civix.domains.mobility_observations.models.speed import (
    MobilitySpeedMetric,
    MobilitySpeedObservation,
)

SITE_MAPPER_ID: Final[MapperId] = MapperId("chicago-traffic-tracker-segment-sites")
SPEED_MAPPER_ID: Final[MapperId] = MapperId("chicago-traffic-tracker-segment-speeds")
MAPPER_VERSION: Final[str] = "0.1.0"

_START_LAT_FIELD: Final[str] = "_lif_lat"
_START_LON_FIELD: Final[str] = "_lif_lon"
_END_LAT_FIELD: Final[str] = "_lit_lat"
_END_LON_FIELD: Final[str] = "_lit_lon"
_DIRECTION_FIELD: Final[str] = "_direction"


@dataclass(frozen=True, slots=True)
class ChicagoTrafficTrackerSegmentSiteMapper:
    """Maps Chicago Traffic Tracker segment rows to road-segment sites."""

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
            provenance=build_provenance(record=record, snapshot=snapshot, mapper=self.version),
            site_id=text_id(
                raw,
                "segmentid",
                mapper=self.version,
                source_record_id=record.source_record_id,
            ),
            kind=MappedField(
                value=MobilitySiteKind.ROAD_SEGMENT,
                quality=FieldQuality.INFERRED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            footprint=line_footprint(
                raw=raw,
                start_lat_field=_START_LAT_FIELD,
                start_lon_field=_START_LON_FIELD,
                end_lat_field=_END_LAT_FIELD,
                end_lon_field=_END_LON_FIELD,
                mapper=self.version,
                source_record_id=record.source_record_id,
            ),
            address=_address(raw, snapshot),
            road_names=_road_names(raw),
            direction=map_direction(raw.get(_DIRECTION_FIELD), _DIRECTION_FIELD),
            movement_type=MappedField(value=None, quality=FieldQuality.UNMAPPED, source_fields=()),
            active_period=MappedField(value=None, quality=FieldQuality.UNMAPPED, source_fields=()),
            measurement_method=MappedField(
                value=MeasurementMethod.BUS_GPS_ESTIMATE,
                quality=FieldQuality.INFERRED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            source_caveats=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
        )

        return MapResult(record=site, report=mapping_report(raw, site))


@dataclass(frozen=True, slots=True)
class ChicagoTrafficTrackerSegmentSpeedMapper:
    """Maps Chicago Traffic Tracker segment rows to speed observations."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=SPEED_MAPPER_ID, version=MAPPER_VERSION)

    def __call__(
        self,
        record: RawRecord,
        snapshot: SourceSnapshot,
    ) -> MapResult[MobilitySpeedObservation]:
        raw = record.raw_data
        segment_id = text_id(
            raw,
            "segmentid",
            mapper=self.version,
            source_record_id=record.source_record_id,
        )
        period = chicago_local_datetime_period(
            raw,
            "_last_updt",
            mapper=self.version,
            source_record_id=record.source_record_id,
        )
        observation = MobilitySpeedObservation(
            provenance=build_provenance(record=record, snapshot=snapshot, mapper=self.version),
            observation_id=_observation_id(segment_id=segment_id, period=period),
            site_id=segment_id,
            period=MappedField(
                value=period,
                quality=FieldQuality.STANDARDIZED,
                source_fields=("_last_updt",),
            ),
            travel_mode=MappedField(
                value=TravelMode.MIXED_TRAFFIC,
                quality=FieldQuality.INFERRED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            direction=map_direction(raw.get(_DIRECTION_FIELD), _DIRECTION_FIELD),
            movement_type=MappedField(value=None, quality=FieldQuality.UNMAPPED, source_fields=()),
            measurement_method=MappedField(
                value=MeasurementMethod.BUS_GPS_ESTIMATE,
                quality=FieldQuality.INFERRED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            aggregation_window=MappedField(
                value=AggregationWindow.SOURCE_SPECIFIC,
                quality=FieldQuality.INFERRED,
                source_fields=("_last_updt",),
            ),
            metrics=(
                MobilitySpeedMetric(
                    metric_type=MappedField(
                        value=SpeedMetricType.OBSERVED_SPEED,
                        quality=FieldQuality.INFERRED,
                        source_fields=("_traffic",),
                    ),
                    unit=MappedField(
                        value=SpeedUnit.MILES_PER_HOUR,
                        quality=FieldQuality.INFERRED,
                        source_fields=("_traffic",),
                    ),
                    value=decimal_traffic_estimate(
                        raw,
                        "_traffic",
                        mapper=self.version,
                        source_record_id=record.source_record_id,
                    ),
                ),
            ),
            source_caveats=_speed_caveats(),
        )

        return MapResult(record=observation, report=mapping_report(raw, observation))


def _observation_id(*, segment_id: str, period: TemporalPeriod) -> str:
    assert period.datetime_value is not None

    return f"{segment_id}:{period.datetime_value.isoformat()}"


def _address(raw: Mapping[str, Any], snapshot: SourceSnapshot) -> MappedField[Address]:
    street = str_or_none(raw.get("street"))

    return MappedField(
        value=Address(
            country=snapshot.jurisdiction.country,
            region=snapshot.jurisdiction.region,
            locality=snapshot.jurisdiction.locality,
            street=street,
        ),
        quality=FieldQuality.DERIVED,
        source_fields=("street",),
    )


def _road_names(raw: Mapping[str, Any]) -> MappedField[tuple[str, ...]]:
    source_fields = ("street", "_fromst", "_tost")
    names = tuple(
        value for field_name in source_fields if (value := str_or_none(raw.get(field_name)))
    )

    if not names:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=source_fields,
        )

    return MappedField(value=names, quality=FieldQuality.DIRECT, source_fields=source_fields)


def _speed_caveats() -> MappedField[tuple[CategoryRef, ...]]:
    return MappedField(
        value=(
            category(
                taxonomy_id=CAVEAT_TAXONOMY_ID,
                taxonomy_version=CAVEAT_TAXONOMY_VERSION,
                code=REFRESH_NOT_INTERVAL_CAVEAT_CODE,
                label=REFRESH_NOT_INTERVAL_CAVEAT_LABEL,
            ),
        ),
        quality=FieldQuality.INFERRED,
        source_fields=(SOURCE_CAVEATS_FIELD,),
    )
