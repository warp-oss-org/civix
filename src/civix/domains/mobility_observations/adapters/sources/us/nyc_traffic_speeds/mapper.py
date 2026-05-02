"""NYC DOT Traffic Speeds mapper."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Final

from civix.core.identity.models.identifiers import MapperId
from civix.core.mapping.models.mapper import MapResult
from civix.core.provenance.models.provenance import MapperVersion
from civix.core.quality.models.fields import FieldQuality, MappedField
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.domains.mobility_observations.adapters.sources.us._nyc_common import (
    DATASET_CONTEXT_FIELD,
    build_provenance,
    decimal_required_nonnegative,
    mapping_report,
    source_datetime_period,
    text_id,
)
from civix.domains.mobility_observations.models.common import (
    AggregationWindow,
    MeasurementMethod,
    SpeedMetricType,
    SpeedUnit,
    TravelMode,
)
from civix.domains.mobility_observations.models.speed import (
    MobilitySpeedMetric,
    MobilitySpeedObservation,
)

MAPPER_ID: Final[MapperId] = MapperId("nyc-traffic-speeds")
MAPPER_VERSION: Final[str] = "0.1.0"


@dataclass(frozen=True, slots=True)
class NycTrafficSpeedsMapper:
    """Maps NYC traffic speed rows to speed observations."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=MAPPER_ID, version=MAPPER_VERSION)

    def __call__(
        self,
        record: RawRecord,
        snapshot: SourceSnapshot,
    ) -> MapResult[MobilitySpeedObservation]:
        raw = record.raw_data
        link_id = text_id(
            raw,
            "LINK_ID",
            mapper=self.version,
            source_record_id=record.source_record_id,
        )
        data_as_of = text_id(
            raw,
            "DATA_AS_OF",
            mapper=self.version,
            source_record_id=record.source_record_id,
        )
        observation = MobilitySpeedObservation(
            provenance=build_provenance(record=record, snapshot=snapshot, mapper=self.version),
            observation_id=f"{link_id}:{data_as_of}",
            site_id=link_id,
            period=MappedField(
                value=source_datetime_period(
                    raw,
                    "DATA_AS_OF",
                    mapper=self.version,
                    source_record_id=record.source_record_id,
                ),
                quality=FieldQuality.STANDARDIZED,
                source_fields=("DATA_AS_OF",),
            ),
            travel_mode=MappedField(
                value=TravelMode.MIXED_TRAFFIC,
                quality=FieldQuality.INFERRED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            direction=MappedField(
                value=None,
                quality=FieldQuality.NOT_PROVIDED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            movement_type=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            measurement_method=MappedField(
                value=MeasurementMethod.SENSOR_FEED,
                quality=FieldQuality.INFERRED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            aggregation_window=MappedField(
                value=AggregationWindow.RAW_INTERVAL,
                quality=FieldQuality.INFERRED,
                source_fields=("DATA_AS_OF",),
            ),
            metrics=(
                _metric(
                    raw,
                    "SPEED",
                    SpeedMetricType.OBSERVED_SPEED,
                    SpeedUnit.MILES_PER_HOUR,
                    mapper=self.version,
                    source_record_id=record.source_record_id,
                ),
                _metric(
                    raw,
                    "TRAVEL_TIME",
                    SpeedMetricType.TRAVEL_TIME,
                    SpeedUnit.SECONDS,
                    mapper=self.version,
                    source_record_id=record.source_record_id,
                ),
            ),
            source_caveats=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
        )

        return MapResult(record=observation, report=mapping_report(raw, observation))


def _metric(
    raw: Mapping[str, object],
    source_field: str,
    metric_type: SpeedMetricType,
    unit: SpeedUnit,
    *,
    mapper: MapperVersion,
    source_record_id: str | None,
) -> MobilitySpeedMetric:
    value = decimal_required_nonnegative(
        raw,
        source_field,
        mapper=mapper,
        source_record_id=source_record_id,
    )

    return MobilitySpeedMetric(
        metric_type=MappedField(
            value=metric_type,
            quality=FieldQuality.INFERRED,
            source_fields=(source_field,),
        ),
        unit=MappedField(
            value=unit,
            quality=FieldQuality.INFERRED,
            source_fields=(source_field,),
        ),
        value=MappedField[Decimal](
            value=value,
            quality=FieldQuality.DIRECT,
            source_fields=(source_field,),
        ),
    )
