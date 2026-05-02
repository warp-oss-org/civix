"""Shared helpers for Chicago mobility-observation source slices.

This module deliberately duplicates the NYC helper rather than weakening the
NYC strictness. Promotion to a mobility-wide helper is out of scope until a
third city motivates it.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Final, cast

from pydantic import BaseModel, ValidationError

from civix.core.drift import TaxonomySpec
from civix.core.mapping.errors import MappingError
from civix.core.mapping.models.mapper import MappingReport
from civix.core.mapping.parsers import float_or_none, require_text, str_or_none
from civix.core.provenance.models.provenance import MapperVersion, ProvenanceRef
from civix.core.quality.models.fields import FieldQuality, MappedField
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.spatial.models.geometry import BoundingBox, LineString, SpatialFootprint
from civix.core.spatial.models.location import Coordinate
from civix.core.taxonomy.models.category import CategoryRef
from civix.core.temporal import TemporalPeriod, TemporalPeriodPrecision, TemporalTimezoneStatus
from civix.domains.mobility_observations.models.common import ObservationDirection

CHICAGO_TIMEZONE: Final[str] = "America/Chicago"
DATASET_CONTEXT_FIELD: Final[str] = "snapshot.dataset_id"
SOURCE_CAVEATS_FIELD: Final[str] = "source.caveats"

CAVEAT_TAXONOMY_ID: Final[str] = "chicago-traffic-tracker-caveats"
CAVEAT_TAXONOMY_VERSION: Final[str] = "2026-05-02"
REFRESH_NOT_INTERVAL_CAVEAT_CODE: Final[str] = "estimate-refreshed-at-not-observed-over"
REFRESH_NOT_INTERVAL_CAVEAT_LABEL: Final[str] = "estimate refreshed at, not observed-over"
REGIONAL_ROLLUP_CAVEAT_CODE: Final[str] = "regional-bus-gps-rollup"
REGIONAL_ROLLUP_CAVEAT_LABEL: Final[str] = "regional bus-GPS rollup"

# Caveats are mapper-emitted constants, not raw fields. The spec is included
# in each slice's TAXONOMIES tuple for documentation parity; the source_field
# is a synthetic pseudo-field so the standard TaxonomyObserver never fires on
# raw rows. Drift in caveat codes is caught at code review, not at runtime.
CHICAGO_TRAFFIC_TRACKER_CAVEAT_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id=CAVEAT_TAXONOMY_ID,
    version=CAVEAT_TAXONOMY_VERSION,
    source_field=SOURCE_CAVEATS_FIELD,
    normalization="strip_casefold",
    known_values=frozenset(
        {
            REFRESH_NOT_INTERVAL_CAVEAT_CODE,
            REGIONAL_ROLLUP_CAVEAT_CODE,
        }
    ),
)

_TRAFFIC_NOT_AVAILABLE_SENTINEL: Final[str] = "-1"

# Diagonals (NE/NW/SE/SW) are documented as known values by the segment
# direction taxonomy, so they don't trip drift; at the field level they fall
# through to SOURCE_SPECIFIC + DERIVED, identical to a fully unknown token.
# That asymmetry is intentional: drift reporting differentiates them; the
# field-level mapping stays simple.
_DIRECTION_MAP: Final[dict[str, ObservationDirection]] = {
    "n": ObservationDirection.NORTHBOUND,
    "nb": ObservationDirection.NORTHBOUND,
    "n/b": ObservationDirection.NORTHBOUND,
    "northbound": ObservationDirection.NORTHBOUND,
    "s": ObservationDirection.SOUTHBOUND,
    "sb": ObservationDirection.SOUTHBOUND,
    "s/b": ObservationDirection.SOUTHBOUND,
    "southbound": ObservationDirection.SOUTHBOUND,
    "e": ObservationDirection.EASTBOUND,
    "eb": ObservationDirection.EASTBOUND,
    "e/b": ObservationDirection.EASTBOUND,
    "eastbound": ObservationDirection.EASTBOUND,
    "w": ObservationDirection.WESTBOUND,
    "wb": ObservationDirection.WESTBOUND,
    "w/b": ObservationDirection.WESTBOUND,
    "westbound": ObservationDirection.WESTBOUND,
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


def decimal_traffic_estimate(
    raw: Mapping[str, Any],
    field_name: str,
    *,
    mapper: MapperVersion,
    source_record_id: str | None,
) -> MappedField[Decimal]:
    """Parse Chicago Traffic Tracker `_traffic` style speed fields.

    The documented sentinel `-1` means "no estimate available" and is mapped to
    a value of None with NOT_PROVIDED quality. Any other negative or non-numeric
    value raises MappingError, preserving the loud-failure contract.
    """
    text = require_text(
        raw.get(field_name),
        field_name=field_name,
        mapper=mapper,
        source_record_id=source_record_id,
    )

    if text == _TRAFFIC_NOT_AVAILABLE_SENTINEL:
        return MappedField[Decimal](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=(field_name,),
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

    return MappedField[Decimal](
        value=value,
        quality=FieldQuality.DIRECT,
        source_fields=(field_name,),
    )


def chicago_local_datetime_period(
    raw: Mapping[str, Any],
    field_name: str,
    *,
    mapper: MapperVersion,
    source_record_id: str | None,
) -> TemporalPeriod:
    """Parse a `_last_updt` style Chicago-local naive timestamp.

    The Traffic Tracker feeds publish wall-clock Chicago time without an
    offset. We accept either ISO 8601 ("T") or space-separated forms.
    """
    text = require_text(
        raw.get(field_name),
        field_name=field_name,
        mapper=mapper,
        source_record_id=source_record_id,
    )

    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as e:
        raise MappingError(
            f"invalid datetime source field {field_name!r}",
            mapper=mapper,
            source_record_id=source_record_id,
            source_fields=(field_name,),
        ) from e

    if parsed.tzinfo is not None:
        raise MappingError(
            f"timezone-aware datetime source field {field_name!r} is not supported",
            mapper=mapper,
            source_record_id=source_record_id,
            source_fields=(field_name,),
        )

    return TemporalPeriod(
        precision=TemporalPeriodPrecision.DATETIME,
        datetime_value=parsed,
        timezone_status=TemporalTimezoneStatus.NAMED_LOCAL,
        timezone=CHICAGO_TIMEZONE,
    )


def map_direction(raw_value: object, source_field: str) -> MappedField[ObservationDirection]:
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


def line_footprint(
    *,
    raw: Mapping[str, Any],
    start_lat_field: str,
    start_lon_field: str,
    end_lat_field: str,
    end_lon_field: str,
    mapper: MapperVersion,
    source_record_id: str | None,
) -> MappedField[SpatialFootprint]:
    source_fields = (start_lat_field, start_lon_field, end_lat_field, end_lon_field)
    start_lat = float_or_none(raw.get(start_lat_field))
    start_lon = float_or_none(raw.get(start_lon_field))
    end_lat = float_or_none(raw.get(end_lat_field))
    end_lon = float_or_none(raw.get(end_lon_field))

    if (
        start_lat is None
        or start_lon is None
        or end_lat is None
        or end_lon is None
    ):
        raise MappingError(
            "invalid segment coordinates",
            mapper=mapper,
            source_record_id=source_record_id,
            source_fields=source_fields,
        )

    try:
        line = LineString(
            coordinates=(
                Coordinate(latitude=start_lat, longitude=start_lon),
                Coordinate(latitude=end_lat, longitude=end_lon),
            )
        )
        footprint = SpatialFootprint(line=line)
    except ValidationError as e:
        raise MappingError(
            "invalid segment coordinates",
            mapper=mapper,
            source_record_id=source_record_id,
            source_fields=source_fields,
        ) from e

    return MappedField[SpatialFootprint](
        value=footprint,
        quality=FieldQuality.STANDARDIZED,
        source_fields=source_fields,
    )


def bounding_box_footprint(
    *,
    raw: Mapping[str, Any],
    west_field: str,
    east_field: str,
    south_field: str,
    north_field: str,
    mapper: MapperVersion,
    source_record_id: str | None,
) -> MappedField[SpatialFootprint]:
    source_fields = (west_field, east_field, south_field, north_field)
    west = float_or_none(raw.get(west_field))
    east = float_or_none(raw.get(east_field))
    south = float_or_none(raw.get(south_field))
    north = float_or_none(raw.get(north_field))

    if west is None or east is None or south is None or north is None:
        raise MappingError(
            "invalid region bounding box",
            mapper=mapper,
            source_record_id=source_record_id,
            source_fields=source_fields,
        )

    try:
        bounding_box = BoundingBox(west=west, south=south, east=east, north=north)
        footprint = SpatialFootprint(bounding_box=bounding_box)
    except ValidationError as e:
        raise MappingError(
            "invalid region bounding box",
            mapper=mapper,
            source_record_id=source_record_id,
            source_fields=source_fields,
        ) from e

    return MappedField[SpatialFootprint](
        value=footprint,
        quality=FieldQuality.STANDARDIZED,
        source_fields=source_fields,
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


def mapping_report(raw: Mapping[str, Any], record: BaseModel) -> MappingReport:
    consumed: set[str] = set()
    _collect_source_fields(record, consumed)

    return MappingReport(
        unmapped_source_fields=tuple(sorted(set(raw) - consumed)),
    )


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
