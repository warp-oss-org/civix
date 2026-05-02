"""Chicago Traffic Tracker regions mappers."""

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
    REGIONAL_ROLLUP_CAVEAT_CODE,
    REGIONAL_ROLLUP_CAVEAT_LABEL,
    SOURCE_CAVEATS_FIELD,
    bounding_box_footprint,
    build_provenance,
    category,
    chicago_local_datetime_period,
    decimal_traffic_estimate,
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

SITE_MAPPER_ID: Final[MapperId] = MapperId("chicago-traffic-tracker-region-sites")
SPEED_MAPPER_ID: Final[MapperId] = MapperId("chicago-traffic-tracker-region-speeds")
MAPPER_VERSION: Final[str] = "0.1.0"

_WEST_FIELD: Final[str] = "west"
_EAST_FIELD: Final[str] = "east"
_SOUTH_FIELD: Final[str] = "south"
_NORTH_FIELD: Final[str] = "north"


@dataclass(frozen=True, slots=True)
class ChicagoTrafficTrackerRegionSiteMapper:
    """Maps Chicago Traffic Tracker region rows to region sites."""

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
                "_region_id",
                mapper=self.version,
                source_record_id=record.source_record_id,
            ),
            kind=MappedField(
                value=MobilitySiteKind.REGION,
                quality=FieldQuality.INFERRED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            footprint=bounding_box_footprint(
                raw=raw,
                west_field=_WEST_FIELD,
                east_field=_EAST_FIELD,
                south_field=_SOUTH_FIELD,
                north_field=_NORTH_FIELD,
                mapper=self.version,
                source_record_id=record.source_record_id,
            ),
            address=_address(snapshot),
            road_names=_region_descriptors(raw),
            direction=MappedField(value=None, quality=FieldQuality.UNMAPPED, source_fields=()),
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
class ChicagoTrafficTrackerRegionSpeedMapper:
    """Maps Chicago Traffic Tracker region rows to speed observations."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=SPEED_MAPPER_ID, version=MAPPER_VERSION)

    def __call__(
        self,
        record: RawRecord,
        snapshot: SourceSnapshot,
    ) -> MapResult[MobilitySpeedObservation]:
        raw = record.raw_data
        region_id = text_id(
            raw,
            "_region_id",
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
            observation_id=_observation_id(region_id=region_id, period=period),
            site_id=region_id,
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
            direction=MappedField(value=None, quality=FieldQuality.UNMAPPED, source_fields=()),
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
                        source_fields=("current_speed",),
                    ),
                    unit=MappedField(
                        value=SpeedUnit.MILES_PER_HOUR,
                        quality=FieldQuality.INFERRED,
                        source_fields=("current_speed",),
                    ),
                    value=decimal_traffic_estimate(
                        raw,
                        "current_speed",
                        mapper=self.version,
                        source_record_id=record.source_record_id,
                    ),
                ),
            ),
            source_caveats=_speed_caveats(),
        )

        return MapResult(record=observation, report=mapping_report(raw, observation))


def _observation_id(*, region_id: str, period: TemporalPeriod) -> str:
    assert period.datetime_value is not None

    return f"{region_id}:{period.datetime_value.isoformat()}"


def _address(snapshot: SourceSnapshot) -> MappedField[Address]:
    return MappedField(
        value=Address(
            country=snapshot.jurisdiction.country,
            region=snapshot.jurisdiction.region,
            locality=snapshot.jurisdiction.locality,
        ),
        quality=FieldQuality.DERIVED,
        source_fields=(DATASET_CONTEXT_FIELD,),
    )


def _region_descriptors(raw: Mapping[str, Any]) -> MappedField[tuple[str, ...]]:
    source_fields = ("region", "description")
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
                code=REGIONAL_ROLLUP_CAVEAT_CODE,
                label=REGIONAL_ROLLUP_CAVEAT_LABEL,
            ),
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
