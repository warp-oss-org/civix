"""Toronto permanent bicycle counter mappers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from typing import Any, Final

from civix.core.identity.models.identifiers import MapperId
from civix.core.mapping.errors import MappingError
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
    active_date_period,
    build_provenance,
    category,
    decimal_optional_nonnegative,
    map_direction,
    mapping_report,
    point_footprint,
    source_datetime_duration_interval,
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

COUNT_MAPPER_ID: Final[MapperId] = MapperId("toronto-bicycle-counter-15min")
SITE_MAPPER_ID: Final[MapperId] = MapperId("toronto-bicycle-counter-sites")
MAPPER_VERSION: Final[str] = "0.1.0"
_CAVEAT_TAXONOMY_ID: Final[str] = "toronto-bicycle-counter-caveats"
_TECHNOLOGY_TAXONOMY_ID: Final[str] = "toronto-bicycle-counter-technology"
_TAXONOMY_VERSION: Final[str] = "2026-05-02"
_FIFTEEN_MINUTES: Final[timedelta] = timedelta(minutes=15)


@dataclass(frozen=True, slots=True)
class TorontoBicycleCounterSiteMapper:
    """Maps Toronto permanent bicycle counter location rows to sites."""

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
                "location_dir_id",
                mapper=self.version,
                source_record_id=record.source_record_id,
            ),
            kind=MappedField(
                value=MobilitySiteKind.COUNTER,
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
            direction=map_direction(raw.get("direction"), "direction"),
            movement_type=MappedField(value=None, quality=FieldQuality.UNMAPPED, source_fields=()),
            active_period=active_date_period(raw),
            measurement_method=MappedField(
                value=MeasurementMethod.AUTOMATED_COUNTER,
                quality=FieldQuality.INFERRED,
                source_fields=("technology",),
            ),
            source_caveats=_site_caveats(raw),
        )

        return MapResult(record=site, report=mapping_report(raw, site))


@dataclass(frozen=True, slots=True)
class TorontoBicycleCounter15MinMapper:
    """Maps Toronto permanent bicycle counter 15-minute rows to count observations."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=COUNT_MAPPER_ID, version=MAPPER_VERSION)

    def __call__(
        self,
        record: RawRecord,
        snapshot: SourceSnapshot,
    ) -> MapResult[MobilityCountObservation]:
        raw = record.raw_data
        location_dir_id = text_id(
            raw,
            "location_dir_id",
            mapper=self.version,
            source_record_id=record.source_record_id,
        )
        value = decimal_optional_nonnegative(
            raw,
            "bin_volume",
            mapper=self.version,
            source_record_id=record.source_record_id,
        )

        if value is None:
            raise MappingError(
                "missing required source field 'bin_volume'",
                mapper=self.version,
                source_record_id=record.source_record_id,
                source_fields=("bin_volume",),
            )

        observation = MobilityCountObservation(
            provenance=build_provenance(record=record, snapshot=snapshot, mapper=self.version),
            observation_id=_observation_id(
                raw,
                location_dir_id=location_dir_id,
                mapper=self.version,
                source_record_id=record.source_record_id,
            ),
            site_id=location_dir_id,
            period=MappedField(
                value=source_datetime_duration_interval(
                    raw,
                    "datetime_bin",
                    duration=_FIFTEEN_MINUTES,
                    mapper=self.version,
                    source_record_id=record.source_record_id,
                ),
                quality=FieldQuality.STANDARDIZED,
                source_fields=("datetime_bin",),
            ),
            travel_mode=MappedField(
                value=TravelMode.MICROMOBILITY,
                quality=FieldQuality.INFERRED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            direction=MappedField(value=None, quality=FieldQuality.UNMAPPED, source_fields=()),
            movement_type=MappedField(value=None, quality=FieldQuality.UNMAPPED, source_fields=()),
            measurement_method=MappedField(
                value=MeasurementMethod.AUTOMATED_COUNTER,
                quality=FieldQuality.INFERRED,
                source_fields=(DATASET_CONTEXT_FIELD,),
            ),
            aggregation_window=MappedField(
                value=AggregationWindow.RAW_INTERVAL,
                quality=FieldQuality.INFERRED,
                source_fields=("datetime_bin",),
            ),
            metric_type=MappedField(
                value=CountMetricType.RAW_COUNT,
                quality=FieldQuality.INFERRED,
                source_fields=("bin_volume",),
            ),
            unit=MappedField(
                value=CountUnit.COUNT,
                quality=FieldQuality.INFERRED,
                source_fields=("bin_volume",),
            ),
            value=MappedField[Decimal](
                value=value,
                quality=FieldQuality.DIRECT,
                source_fields=("bin_volume",),
            ),
            source_caveats=_count_caveats(),
        )

        return MapResult(record=observation, report=mapping_report(raw, observation))


def _observation_id(
    raw: Mapping[str, Any],
    *,
    location_dir_id: str,
    mapper: MapperVersion,
    source_record_id: str | None,
) -> str:
    datetime_bin = require_text(
        raw.get("datetime_bin"),
        field_name="datetime_bin",
        mapper=mapper,
        source_record_id=source_record_id,
    )

    return f"{location_dir_id}:{datetime_bin}"


def _address(raw: Mapping[str, Any], snapshot: SourceSnapshot) -> MappedField[Address]:
    return MappedField(
        value=Address(
            country=snapshot.jurisdiction.country,
            region=snapshot.jurisdiction.region,
            locality=snapshot.jurisdiction.locality,
            street=str_or_none(raw.get("linear_name_full")),
        ),
        quality=FieldQuality.DERIVED,
        source_fields=("linear_name_full",),
    )


def _road_names(raw: Mapping[str, Any]) -> MappedField[tuple[str, ...]]:
    source_fields = ("linear_name_full", "side_street")
    names = tuple(
        value for field_name in source_fields if (value := str_or_none(raw.get(field_name)))
    )

    if not names:
        return MappedField(
            value=None, quality=FieldQuality.NOT_PROVIDED, source_fields=source_fields
        )

    return MappedField(value=names, quality=FieldQuality.DIRECT, source_fields=source_fields)


def _site_caveats(raw: Mapping[str, Any]) -> MappedField[tuple[CategoryRef, ...]]:
    technology = str_or_none(raw.get("technology"))
    caveats = [
        _caveat("detector-zone-only", "Counts only include riders in the detector zone."),
        _caveat("retroactive-calibration", "Detector calibration may be updated retroactively."),
    ]

    if technology is not None:
        caveats.append(
            category(
                taxonomy_id=_TECHNOLOGY_TAXONOMY_ID,
                taxonomy_version=_TAXONOMY_VERSION,
                code=technology.casefold(),
                label=technology,
            )
        )

    return MappedField(
        value=tuple(caveats),
        quality=FieldQuality.INFERRED,
        source_fields=("technology", SOURCE_CAVEATS_FIELD),
    )


def _count_caveats() -> MappedField[tuple[CategoryRef, ...]]:
    return MappedField(
        value=(
            _caveat("micromobility-included", "Counts include bicycles and micromobility devices."),
            _caveat("detector-zone-only", "Counts only include riders in the detector zone."),
            _caveat("zero-can-reflect-blockage", "Zero bins can reflect detector-zone blockage."),
        ),
        quality=FieldQuality.INFERRED,
        source_fields=(SOURCE_CAVEATS_FIELD,),
    )


def _caveat(code: str, label: str) -> CategoryRef:
    return category(
        taxonomy_id=_CAVEAT_TAXONOMY_ID,
        taxonomy_version=_TAXONOMY_VERSION,
        code=code,
        label=label,
    )
