"""NYC Automated Traffic Volume Counts mappers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Final

from civix.core.identity.models.identifiers import MapperId
from civix.core.mapping.models.mapper import MapResult
from civix.core.mapping.parsers import str_or_none
from civix.core.provenance.models.provenance import MapperVersion
from civix.core.quality.models.fields import FieldQuality, MappedField
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.spatial.models.location import Address
from civix.domains.mobility_observations.adapters.sources.us._nyc_common import (
    DATASET_CONTEXT_FIELD,
    build_provenance,
    date_parts_period,
    decimal_required_nonnegative,
    map_direction,
    map_wkt_footprint,
    mapping_report,
    optional_id_part,
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

COUNT_MAPPER_ID: Final[MapperId] = MapperId("nyc-traffic-volume-counts")
SITE_MAPPER_ID: Final[MapperId] = MapperId("nyc-traffic-volume-sites")
MAPPER_VERSION: Final[str] = "0.1.0"


@dataclass(frozen=True, slots=True)
class NycTrafficVolumeCountMapper:
    """Maps NYC traffic volume rows to count observations."""

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
            "Vol",
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
                "SegmentID",
                mapper=self.version,
                source_record_id=record.source_record_id,
            ),
            period=MappedField(
                value=date_parts_period(
                    raw,
                    mapper=self.version,
                    source_record_id=record.source_record_id,
                ),
                quality=FieldQuality.STANDARDIZED,
                source_fields=("Yr", "M", "D", "HH", "MM"),
            ),
            travel_mode=MappedField(
                value=TravelMode.VEHICLE,
                quality=FieldQuality.INFERRED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            direction=map_direction(raw.get("Direction"), "Direction"),
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
            aggregation_window=MappedField(
                value=AggregationWindow.RAW_INTERVAL,
                quality=FieldQuality.INFERRED,
                source_fields=("HH", "MM"),
            ),
            metric_type=MappedField(
                value=CountMetricType.RAW_COUNT,
                quality=FieldQuality.INFERRED,
                source_fields=("Vol",),
            ),
            unit=MappedField(
                value=CountUnit.COUNT,
                quality=FieldQuality.INFERRED,
                source_fields=("Vol",),
            ),
            value=MappedField[Decimal](
                value=value,
                quality=FieldQuality.DIRECT,
                source_fields=("Vol",),
            ),
            source_caveats=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
        )

        return MapResult(record=observation, report=mapping_report(raw, observation))


@dataclass(frozen=True, slots=True)
class NycTrafficVolumeSiteMapper:
    """Maps NYC traffic volume rows to road-segment site records."""

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
                "SegmentID",
                mapper=self.version,
                source_record_id=record.source_record_id,
            ),
            kind=MappedField(
                value=MobilitySiteKind.ROAD_SEGMENT,
                quality=FieldQuality.INFERRED,
                source_fields=("SegmentID",),
            ),
            footprint=map_wkt_footprint(raw.get("WktGeom"), "WktGeom"),
            address=MappedField(
                value=Address(
                    country=snapshot.jurisdiction.country,
                    region=snapshot.jurisdiction.region,
                    locality=str_or_none(raw.get("Boro")),
                    street=str_or_none(raw.get("street")),
                ),
                quality=FieldQuality.DERIVED,
                source_fields=("Boro", "street"),
            ),
            road_names=_road_names(raw),
            direction=map_direction(raw.get("Direction"), "Direction"),
            movement_type=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            active_period=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            measurement_method=MappedField(
                value=MeasurementMethod.AUTOMATED_COUNTER,
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


def _count_observation_id(
    raw: Mapping[str, Any],
    *,
    mapper: MapperVersion,
    source_record_id: str | None,
) -> str:
    required_fields = ("RequestID", "SegmentID", "Yr", "M", "D", "HH", "MM")
    values = (
        *(
            text_id(raw, field_name, mapper=mapper, source_record_id=source_record_id)
            for field_name in required_fields
        ),
        optional_id_part(raw, "Direction", "unknown-direction"),
    )

    return ":".join(values)


def _road_names(raw: Mapping[str, Any]) -> MappedField[tuple[str, ...]]:
    source_fields = ("street", "fromSt", "toSt")
    values = tuple(
        value for field_name in source_fields if (value := str_or_none(raw.get(field_name)))
    )

    if not values:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=source_fields,
        )

    return MappedField(value=values, quality=FieldQuality.DIRECT, source_fields=source_fields)
