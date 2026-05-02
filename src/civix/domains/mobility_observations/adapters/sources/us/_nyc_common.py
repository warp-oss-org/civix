"""Shared helpers for NYC mobility-observation source slices."""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Final, cast

from pydantic import BaseModel, ValidationError

from civix.core.mapping.errors import MappingError
from civix.core.mapping.models.mapper import MappingReport
from civix.core.mapping.parsers import float_or_none, int_or_none, require_text, str_or_none
from civix.core.provenance.models.provenance import MapperVersion, ProvenanceRef
from civix.core.quality.models.fields import FieldQuality, MappedField
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.spatial.models.geometry import LineString, SpatialFootprint
from civix.core.spatial.models.location import Coordinate
from civix.core.temporal import TemporalPeriod, TemporalPeriodPrecision, TemporalTimezoneStatus
from civix.domains.mobility_observations.models.common import ObservationDirection

NYC_TIMEZONE: Final[str] = "America/New_York"
# Pseudo-fields name non-raw context used for inferred values; keep the
# `snapshot.` prefix for snapshot metadata and `source.` for explicit source caveats.
DATASET_CONTEXT_FIELD: Final[str] = "snapshot.dataset_id"

_DIRECTION_MAP: Final[dict[str, ObservationDirection]] = {
    "e/b": ObservationDirection.EASTBOUND,
    "eastbound": ObservationDirection.EASTBOUND,
    "eb": ObservationDirection.EASTBOUND,
    "n/b": ObservationDirection.NORTHBOUND,
    "northbound": ObservationDirection.NORTHBOUND,
    "nb": ObservationDirection.NORTHBOUND,
    "s/b": ObservationDirection.SOUTHBOUND,
    "southbound": ObservationDirection.SOUTHBOUND,
    "sb": ObservationDirection.SOUTHBOUND,
    "w/b": ObservationDirection.WESTBOUND,
    "westbound": ObservationDirection.WESTBOUND,
    "wb": ObservationDirection.WESTBOUND,
    "bidirectional": ObservationDirection.BIDIRECTIONAL,
    "inbound": ObservationDirection.INBOUND,
    "outbound": ObservationDirection.OUTBOUND,
}
_POINT_RE: Final[re.Pattern[str]] = re.compile(
    r"^POINT\s*\(\s*(?P<lon>-?\d+(?:\.\d+)?)\s+(?P<lat>-?\d+(?:\.\d+)?)\s*\)$",
    re.IGNORECASE,
)
_LINE_RE: Final[re.Pattern[str]] = re.compile(r"^LINESTRING\s*\((?P<body>.+)\)$", re.IGNORECASE)


def build_provenance(
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


def decimal_required_nonnegative(
    raw: Mapping[str, Any],
    field_name: str,
    *,
    mapper: MapperVersion,
    source_record_id: str | None,
) -> Decimal:
    text = require_text(
        raw.get(field_name),
        field_name=field_name,
        mapper=mapper,
        source_record_id=source_record_id,
    )

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


def source_datetime_period(
    raw: Mapping[str, Any],
    field_name: str,
    *,
    mapper: MapperVersion,
    source_record_id: str | None,
) -> TemporalPeriod:
    text = require_text(
        raw.get(field_name),
        field_name=field_name,
        mapper=mapper,
        source_record_id=source_record_id,
    )
    normalized = text.replace("Z", "+00:00")

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as e:
        raise MappingError(
            f"invalid datetime source field {field_name!r}",
            mapper=mapper,
            source_record_id=source_record_id,
            source_fields=(field_name,),
        ) from e

    return TemporalPeriod(
        precision=TemporalPeriodPrecision.DATETIME,
        datetime_value=parsed.replace(tzinfo=None),
        timezone_status=TemporalTimezoneStatus.NAMED_LOCAL,
        timezone=NYC_TIMEZONE,
    )


def date_parts_period(
    raw: Mapping[str, Any],
    *,
    mapper: MapperVersion,
    source_record_id: str | None,
) -> TemporalPeriod:
    source_fields = ("Yr", "M", "D", "HH", "MM")
    year = int_or_none(raw.get("Yr"))
    month = int_or_none(raw.get("M"))
    day = int_or_none(raw.get("D"))
    hour = int_or_none(raw.get("HH"))
    minute = int_or_none(raw.get("MM"))

    if year is None or month is None or day is None or hour is None or minute is None:
        raise MappingError(
            "invalid traffic-volume date/time fields",
            mapper=mapper,
            source_record_id=source_record_id,
            source_fields=source_fields,
        )

    try:
        parsed = datetime(
            year=year,
            month=month,
            day=day,
            hour=hour,
            minute=minute,
        )
    except ValueError as e:
        raise MappingError(
            "invalid traffic-volume date/time fields",
            mapper=mapper,
            source_record_id=source_record_id,
            source_fields=source_fields,
        ) from e

    return TemporalPeriod(
        precision=TemporalPeriodPrecision.DATETIME,
        datetime_value=parsed,
        timezone_status=TemporalTimezoneStatus.NAMED_LOCAL,
        timezone=NYC_TIMEZONE,
    )


def map_direction(
    raw_value: object,
    source_field: str,
) -> MappedField[ObservationDirection]:
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


def map_wkt_footprint(raw_value: object, source_field: str) -> MappedField[SpatialFootprint]:
    value = str_or_none(raw_value)

    if value is None:
        return MappedField[SpatialFootprint](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=(source_field,),
        )

    footprint = _parse_wkt(value)

    if footprint is None:
        return MappedField[SpatialFootprint](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=(source_field,),
        )

    return MappedField[SpatialFootprint](
        value=footprint,
        quality=FieldQuality.STANDARDIZED,
        source_fields=(source_field,),
    )


def point_footprint(
    *,
    raw: Mapping[str, Any],
    latitude_field: str,
    longitude_field: str,
) -> MappedField[SpatialFootprint]:
    latitude = float_or_none(raw.get(latitude_field))
    longitude = float_or_none(raw.get(longitude_field))

    if latitude is None or longitude is None:
        return MappedField[SpatialFootprint](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=(latitude_field, longitude_field),
        )

    try:
        footprint = SpatialFootprint(point=Coordinate(latitude=latitude, longitude=longitude))
    except ValidationError:
        return MappedField[SpatialFootprint](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=(latitude_field, longitude_field),
        )

    return MappedField[SpatialFootprint](
        value=footprint,
        quality=FieldQuality.STANDARDIZED,
        source_fields=(latitude_field, longitude_field),
    )


def mapping_report(raw: Mapping[str, Any], record: BaseModel) -> MappingReport:
    consumed: set[str] = set()
    _collect_source_fields(record, consumed)

    return MappingReport(
        unmapped_source_fields=tuple(sorted(set(raw) - consumed)),
    )


def text_id(
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


def optional_id_part(raw: Mapping[str, Any], field_name: str, fallback: str) -> str:
    return str_or_none(raw.get(field_name)) or fallback


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


def _parse_wkt(value: str) -> SpatialFootprint | None:
    point_match = _POINT_RE.match(value)

    if point_match is not None:
        try:
            return SpatialFootprint(
                point=Coordinate(
                    latitude=float(point_match.group("lat")),
                    longitude=float(point_match.group("lon")),
                )
            )
        except ValidationError:
            return None

    line_match = _LINE_RE.match(value)

    if line_match is None:
        return None

    coordinates: list[Coordinate] = []

    for pair in line_match.group("body").split(","):
        parts = pair.strip().split()

        if len(parts) != 2:
            return None

        try:
            coordinates.append(Coordinate(latitude=float(parts[1]), longitude=float(parts[0])))
        except (ValueError, ValidationError):
            return None

    try:
        return SpatialFootprint(line=LineString(coordinates=tuple(coordinates)))
    except ValidationError:
        return None
