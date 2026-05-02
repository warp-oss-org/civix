"""Shared helpers for Toronto mobility-observation source slices."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Final, cast

from pydantic import BaseModel, ValidationError

from civix.core.mapping.errors import MappingError
from civix.core.mapping.models.mapper import MappingReport
from civix.core.mapping.parsers import float_or_none, require_text, str_or_none
from civix.core.provenance.models.provenance import MapperVersion, ProvenanceRef
from civix.core.quality.models.fields import FieldQuality, MappedField
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.spatial.models.geometry import SpatialFootprint
from civix.core.spatial.models.location import Coordinate
from civix.core.taxonomy.models.category import CategoryRef
from civix.core.temporal import TemporalPeriod, TemporalPeriodPrecision, TemporalTimezoneStatus
from civix.domains.mobility_observations.models.common import ObservationDirection

TORONTO_TIMEZONE: Final[str] = "America/Toronto"
DATASET_CONTEXT_FIELD: Final[str] = "snapshot.dataset_id"
SOURCE_CAVEATS_FIELD: Final[str] = "source.caveats"

_DIRECTION_MAP: Final[dict[str, ObservationDirection]] = {
    "eastbound": ObservationDirection.EASTBOUND,
    "e/b": ObservationDirection.EASTBOUND,
    "eb": ObservationDirection.EASTBOUND,
    "northbound": ObservationDirection.NORTHBOUND,
    "n/b": ObservationDirection.NORTHBOUND,
    "nb": ObservationDirection.NORTHBOUND,
    "southbound": ObservationDirection.SOUTHBOUND,
    "s/b": ObservationDirection.SOUTHBOUND,
    "sb": ObservationDirection.SOUTHBOUND,
    "westbound": ObservationDirection.WESTBOUND,
    "w/b": ObservationDirection.WESTBOUND,
    "wb": ObservationDirection.WESTBOUND,
    "bidirectional": ObservationDirection.BIDIRECTIONAL,
}


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


def decimal_optional_nonnegative(
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


def source_datetime_interval(
    raw: Mapping[str, Any],
    *,
    start_field: str,
    end_field: str,
    mapper: MapperVersion,
    source_record_id: str | None,
) -> TemporalPeriod:
    start = parse_local_datetime(
        raw,
        start_field,
        mapper=mapper,
        source_record_id=source_record_id,
    )
    end = parse_local_datetime(
        raw,
        end_field,
        mapper=mapper,
        source_record_id=source_record_id,
    )

    return TemporalPeriod(
        precision=TemporalPeriodPrecision.INTERVAL,
        start_datetime=start,
        end_datetime=end,
        timezone_status=TemporalTimezoneStatus.NAMED_LOCAL,
        timezone=TORONTO_TIMEZONE,
    )


def source_datetime_period(
    raw: Mapping[str, Any],
    field_name: str,
    *,
    mapper: MapperVersion,
    source_record_id: str | None,
) -> TemporalPeriod:
    parsed = parse_local_datetime(
        raw,
        field_name,
        mapper=mapper,
        source_record_id=source_record_id,
    )

    return TemporalPeriod(
        precision=TemporalPeriodPrecision.DATETIME,
        datetime_value=parsed,
        timezone_status=TemporalTimezoneStatus.NAMED_LOCAL,
        timezone=TORONTO_TIMEZONE,
    )


def source_datetime_duration_interval(
    raw: Mapping[str, Any],
    field_name: str,
    *,
    duration: timedelta,
    mapper: MapperVersion,
    source_record_id: str | None,
) -> TemporalPeriod:
    start = parse_local_datetime(
        raw,
        field_name,
        mapper=mapper,
        source_record_id=source_record_id,
    )

    return TemporalPeriod(
        precision=TemporalPeriodPrecision.INTERVAL,
        start_datetime=start,
        end_datetime=start + duration,
        timezone_status=TemporalTimezoneStatus.NAMED_LOCAL,
        timezone=TORONTO_TIMEZONE,
    )


def source_date_period(
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

    try:
        parsed = date.fromisoformat(text)
    except ValueError as e:
        raise MappingError(
            f"invalid date source field {field_name!r}",
            mapper=mapper,
            source_record_id=source_record_id,
            source_fields=(field_name,),
        ) from e

    return TemporalPeriod(
        precision=TemporalPeriodPrecision.DATE,
        date_value=parsed,
        timezone_status=TemporalTimezoneStatus.NAMED_LOCAL,
        timezone=TORONTO_TIMEZONE,
    )


def active_date_period(raw: Mapping[str, Any]) -> MappedField[TemporalPeriod]:
    first_active = _date_or_none(raw.get("first_active"))
    last_active = _date_or_none(raw.get("last_active"))
    source_fields = ("first_active", "last_active")

    if first_active is None and last_active is None:
        return MappedField(
            value=None, quality=FieldQuality.NOT_PROVIDED, source_fields=source_fields
        )

    if first_active is None or last_active is None:
        active_date = first_active or last_active

        if active_date is None:
            return MappedField(
                value=None,
                quality=FieldQuality.NOT_PROVIDED,
                source_fields=source_fields,
            )

        return MappedField(
            value=TemporalPeriod(
                precision=TemporalPeriodPrecision.DATE,
                date_value=active_date,
                timezone_status=TemporalTimezoneStatus.NAMED_LOCAL,
                timezone=TORONTO_TIMEZONE,
            ),
            quality=FieldQuality.STANDARDIZED,
            source_fields=source_fields,
        )

    return MappedField(
        value=TemporalPeriod(
            precision=TemporalPeriodPrecision.INTERVAL,
            start_datetime=datetime.combine(first_active, datetime.min.time()),
            end_datetime=datetime.combine(last_active + timedelta(days=1), datetime.min.time()),
            timezone_status=TemporalTimezoneStatus.NAMED_LOCAL,
            timezone=TORONTO_TIMEZONE,
        ),
        quality=FieldQuality.STANDARDIZED,
        source_fields=source_fields,
    )


def parse_local_datetime(
    raw: Mapping[str, Any],
    field_name: str,
    *,
    mapper: MapperVersion,
    source_record_id: str | None,
) -> datetime:
    text = require_text(
        raw.get(field_name),
        field_name=field_name,
        mapper=mapper,
        source_record_id=source_record_id,
    )

    if "T" not in text and " " not in text:
        raise MappingError(
            f"date-only datetime source field {field_name!r} is not supported",
            mapper=mapper,
            source_record_id=source_record_id,
            source_fields=(field_name,),
        )

    if _has_timezone_designator(text):
        raise MappingError(
            f"timezone-aware datetime source field {field_name!r} is not supported",
            mapper=mapper,
            source_record_id=source_record_id,
            source_fields=(field_name,),
        )

    try:
        return datetime.fromisoformat(text)
    except ValueError as e:
        raise MappingError(
            f"invalid datetime source field {field_name!r}",
            mapper=mapper,
            source_record_id=source_record_id,
            source_fields=(field_name,),
        ) from e


def point_footprint(
    *,
    raw: Mapping[str, Any],
    latitude_field: str,
    longitude_field: str,
) -> MappedField[SpatialFootprint]:
    latitude = float_or_none(raw.get(latitude_field))
    longitude = float_or_none(raw.get(longitude_field))
    source_fields = (latitude_field, longitude_field)

    if latitude is None or longitude is None:
        return MappedField(
            value=None, quality=FieldQuality.NOT_PROVIDED, source_fields=source_fields
        )

    try:
        footprint = SpatialFootprint(point=Coordinate(latitude=latitude, longitude=longitude))
    except ValidationError:
        return MappedField(
            value=None, quality=FieldQuality.NOT_PROVIDED, source_fields=source_fields
        )

    return MappedField(
        value=footprint, quality=FieldQuality.STANDARDIZED, source_fields=source_fields
    )


def map_direction(raw_value: object, source_field: str) -> MappedField[ObservationDirection]:
    value = str_or_none(raw_value)

    if value is None:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=(source_field,),
        )

    mapped = _DIRECTION_MAP.get(value.casefold())

    if mapped is None:
        return MappedField(
            value=ObservationDirection.SOURCE_SPECIFIC,
            quality=FieldQuality.DERIVED,
            source_fields=(source_field,),
        )

    return MappedField(
        value=mapped,
        quality=FieldQuality.STANDARDIZED,
        source_fields=(source_field,),
    )


def category(
    *,
    taxonomy_id: str,
    taxonomy_version: str,
    code: str,
    label: str,
) -> CategoryRef:
    return CategoryRef(
        taxonomy_id=taxonomy_id,
        taxonomy_version=taxonomy_version,
        code=code,
        label=label,
    )


def mapping_report(
    raw: Mapping[str, Any], record: BaseModel | tuple[BaseModel, ...]
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


def _date_or_none(raw_value: object) -> date | None:
    value = str_or_none(raw_value)

    if value is None:
        return None

    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _has_timezone_designator(value: str) -> bool:
    if value.endswith(("Z", "z")):
        return True

    time_start = value.find("T")
    if time_start == -1:
        time_start = value.find(" ")

    if time_start == -1:
        return False

    time_part = value[time_start + 1 :]

    return "+" in time_part or "-" in time_part
