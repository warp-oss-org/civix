"""NYC Bicycle and Pedestrian Counts mappers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Final

from civix.core.identity.models.identifiers import MapperId
from civix.core.mapping.models.mapper import MapResult
from civix.core.mapping.parsers import slugify, str_or_none
from civix.core.provenance.models.provenance import MapperVersion
from civix.core.quality.models.fields import FieldQuality, MappedField
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.taxonomy.models.category import CategoryRef
from civix.domains.mobility_observations.adapters.sources.us._nyc_common import (
    DATASET_CONTEXT_FIELD,
    build_provenance,
    decimal_required_nonnegative,
    map_direction,
    mapping_report,
    optional_id_part,
    point_footprint,
    source_datetime_period,
    text_id,
)
from civix.domains.mobility_observations.models.common import (
    AggregationWindow,
    CountMetricType,
    CountUnit,
    MeasurementMethod,
    MobilitySiteKind,
    TravelMode,
)
from civix.domains.mobility_observations.models.count import MobilityCountObservation
from civix.domains.mobility_observations.models.site import MobilityObservationSite

COUNT_MAPPER_ID: Final[MapperId] = MapperId("nyc-bicycle-pedestrian-counts")
SENSOR_MAPPER_ID: Final[MapperId] = MapperId("nyc-bicycle-pedestrian-sensors")
MAPPER_VERSION: Final[str] = "0.1.0"
_STATUS_TAXONOMY_ID: Final[str] = "nyc-bicycle-pedestrian-status"
_TAXONOMY_VERSION: Final[str] = "2026-05-02"


@dataclass(frozen=True, slots=True)
class NycBicyclePedestrianCountMapper:
    """Maps NYC bicycle/pedestrian count rows to count observations."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=COUNT_MAPPER_ID, version=MAPPER_VERSION)

    def __call__(
        self,
        record: RawRecord,
        snapshot: SourceSnapshot,
    ) -> MapResult[MobilityCountObservation]:
        raw = record.raw_data
        value = decimal_required_nonnegative(
            raw,
            "counts",
            mapper=self.version,
            source_record_id=record.source_record_id,
        )
        observation = MobilityCountObservation(
            provenance=build_provenance(record=record, snapshot=snapshot, mapper=self.version),
            observation_id=_count_observation_id(
                raw,
                mapper=self.version,
                source_record_id=record.source_record_id,
            ),
            site_id=text_id(
                raw,
                "sensor_id",
                mapper=self.version,
                source_record_id=record.source_record_id,
            ),
            period=MappedField(
                value=source_datetime_period(
                    raw,
                    "timestamp",
                    mapper=self.version,
                    source_record_id=record.source_record_id,
                ),
                quality=FieldQuality.STANDARDIZED,
                source_fields=("timestamp",),
            ),
            travel_mode=_map_travel_mode(raw.get("travelMode")),
            direction=map_direction(raw.get("direction"), "direction"),
            movement_type=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            measurement_method=MappedField(
                value=MeasurementMethod.AUTOMATED_COUNTER,
                quality=FieldQuality.INFERRED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            aggregation_window=_map_granularity(raw.get("granularity")),
            metric_type=MappedField(
                value=CountMetricType.RAW_COUNT,
                quality=FieldQuality.INFERRED,
                source_fields=("counts",),
            ),
            unit=MappedField(
                value=CountUnit.COUNT,
                quality=FieldQuality.INFERRED,
                source_fields=("counts",),
            ),
            value=MappedField[Decimal](
                value=value,
                quality=FieldQuality.DIRECT,
                source_fields=("counts",),
            ),
            source_caveats=_source_caveats(raw.get("status"), "status"),
        )

        return MapResult(record=observation, report=mapping_report(raw, observation))


@dataclass(frozen=True, slots=True)
class NycBicyclePedestrianSensorMapper:
    """Maps NYC bicycle/pedestrian sensor rows to site records."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=SENSOR_MAPPER_ID, version=MAPPER_VERSION)

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
                "sensor_id",
                mapper=self.version,
                source_record_id=record.source_record_id,
            ),
            kind=MappedField(
                value=MobilitySiteKind.COUNTER,
                quality=FieldQuality.INFERRED,
                source_fields=("sensor_id",),
            ),
            footprint=point_footprint(
                raw=raw,
                latitude_field="latitude",
                longitude_field="longitude",
            ),
            address=MappedField(value=None, quality=FieldQuality.UNMAPPED, source_fields=()),
            road_names=MappedField(value=None, quality=FieldQuality.UNMAPPED, source_fields=()),
            direction=map_direction(raw.get("direction"), "direction"),
            movement_type=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            active_period=MappedField(value=None, quality=FieldQuality.UNMAPPED, source_fields=()),
            measurement_method=MappedField(
                value=MeasurementMethod.AUTOMATED_COUNTER,
                quality=FieldQuality.INFERRED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            source_caveats=_source_caveats(raw.get("status"), "status"),
        )

        return MapResult(record=site, report=mapping_report(raw, site))


def _count_observation_id(
    raw: Mapping[str, Any],
    *,
    mapper: MapperVersion,
    source_record_id: str | None,
) -> str:
    required_fields = ("sensor_id", "flowID", "travelMode", "timestamp", "granularity")
    values = (
        *(
            text_id(raw, field_name, mapper=mapper, source_record_id=source_record_id)
            for field_name in required_fields
        ),
        optional_id_part(raw, "direction", "unknown-direction"),
    )

    return ":".join(values)


def _map_travel_mode(raw_value: object) -> MappedField[TravelMode]:
    value = str_or_none(raw_value)

    if value is None:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("travelMode",),
        )

    normalized = value.casefold()

    if normalized in {"bicycle", "bike"}:
        return MappedField(
            value=TravelMode.BICYCLE,
            quality=FieldQuality.STANDARDIZED,
            source_fields=("travelMode",),
        )

    if normalized in {"pedestrian", "ped"}:
        return MappedField(
            value=TravelMode.PEDESTRIAN,
            quality=FieldQuality.STANDARDIZED,
            source_fields=("travelMode",),
        )

    return MappedField(value=None, quality=FieldQuality.UNMAPPED, source_fields=())


def _map_granularity(raw_value: object) -> MappedField[AggregationWindow]:
    value = str_or_none(raw_value)

    if value is None:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("granularity",),
        )

    normalized = value.casefold()
    mapped = {
        "15 minutes": AggregationWindow.RAW_INTERVAL,
        "15-minute": AggregationWindow.RAW_INTERVAL,
        "hourly": AggregationWindow.HOURLY,
        "daily": AggregationWindow.DAILY,
    }.get(normalized, AggregationWindow.SOURCE_SPECIFIC)

    return MappedField(
        value=mapped,
        quality=FieldQuality.STANDARDIZED,
        source_fields=("granularity",),
    )


def _source_caveats(raw_value: object, source_field: str) -> MappedField[tuple[CategoryRef, ...]]:
    value = str_or_none(raw_value)

    if value is None:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=(source_field,),
        )

    return MappedField(
        value=(
            CategoryRef(
                code=slugify(value),
                label=value,
                taxonomy_id=_STATUS_TAXONOMY_ID,
                taxonomy_version=_TAXONOMY_VERSION,
            ),
        ),
        quality=FieldQuality.DERIVED,
        source_fields=(source_field,),
    )
