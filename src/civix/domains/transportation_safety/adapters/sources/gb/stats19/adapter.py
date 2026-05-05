"""Great Britain STATS19 source adapter."""

from __future__ import annotations

import csv
import hashlib
import io
import json
from collections.abc import AsyncIterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Final

import httpx
from pydantic import ValidationError

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, SnapshotId, SourceId
from civix.core.mapping.parsers import str_or_none
from civix.core.ports.errors import FetchError
from civix.core.ports.models.adapter import FetchResult
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.temporal import Clock, utc_now
from civix.infra.sources.csv import fetch_csv_bytes

SOURCE_ID: Final[SourceId] = SourceId("dft-open-data")
STATS19_RELEASE: Final[str] = "2024-final"
STATS19_COLLISIONS_DATASET_ID: Final[DatasetId] = DatasetId("stats19-collisions-2024")
STATS19_VEHICLES_DATASET_ID: Final[DatasetId] = DatasetId("stats19-vehicles-2024")
STATS19_CASUALTIES_DATASET_ID: Final[DatasetId] = DatasetId("stats19-casualties-2024")
STATS19_JURISDICTION: Final[Jurisdiction] = Jurisdiction(country="GB")

STATS19_BASE_URL: Final[str] = "https://data.dft.gov.uk/road-accidents-safety-data"
STATS19_COLLISIONS_URL: Final[str] = (
    f"{STATS19_BASE_URL}/dft-road-casualty-statistics-collision-2024.csv"
)
STATS19_VEHICLES_URL: Final[str] = (
    f"{STATS19_BASE_URL}/dft-road-casualty-statistics-vehicle-2024.csv"
)
STATS19_CASUALTIES_URL: Final[str] = (
    f"{STATS19_BASE_URL}/dft-road-casualty-statistics-casualty-2024.csv"
)

STATS19_SOURCE_SCOPE: Final[str] = (
    "Personal injury road collisions in Great Britain that were reported to police "
    "and recorded through STATS19."
)
STATS19_RELEASE_CAVEATS: Final[tuple[str, ...]] = (
    "Final 2024 is the latest full validated year published by DfT.",
    "Provisional first-half 2025 files are unvalidated and may contain duplicate casualties.",
    "November 2025 DfT revisions corrected junction_detail and noted an unresolved "
    "2024 vehicle_location_restricted_lane issue.",
    "Sensitive fields such as contributory factors are not included in the open data.",
)

_COLLISIONS_FIELDS: Final[tuple[str, ...]] = (
    "accident_index",
    "date",
    "time",
    "accident_severity",
    "longitude",
    "latitude",
    "first_road_class",
    "second_road_class",
    "road_type",
    "speed_limit",
    "junction_detail",
    "junction_control",
    "light_conditions",
    "weather_conditions",
    "road_surface_conditions",
    "number_of_vehicles",
    "number_of_casualties",
)
_VEHICLES_FIELDS: Final[tuple[str, ...]] = (
    "accident_index",
    "vehicle_reference",
    "vehicle_type",
    "vehicle_manoeuvre",
    "vehicle_direction_from",
    "vehicle_direction_to",
)
_CASUALTIES_FIELDS: Final[tuple[str, ...]] = (
    "accident_index",
    "vehicle_reference",
    "casualty_reference",
    "casualty_class",
    "casualty_severity",
    "casualty_type",
    "age_of_casualty",
    "age_band_of_casualty",
)


@dataclass(frozen=True, slots=True)
class _TableSpec:
    dataset_id: DatasetId
    expected_fields: tuple[str, ...]
    source_record_id_fields: tuple[str, ...]


_COLLISIONS_TABLE: Final[_TableSpec] = _TableSpec(
    dataset_id=STATS19_COLLISIONS_DATASET_ID,
    expected_fields=_COLLISIONS_FIELDS,
    source_record_id_fields=("accident_index",),
)
_VEHICLES_TABLE: Final[_TableSpec] = _TableSpec(
    dataset_id=STATS19_VEHICLES_DATASET_ID,
    expected_fields=_VEHICLES_FIELDS,
    source_record_id_fields=("accident_index", "vehicle_reference"),
)
_CASUALTIES_TABLE: Final[_TableSpec] = _TableSpec(
    dataset_id=STATS19_CASUALTIES_DATASET_ID,
    expected_fields=_CASUALTIES_FIELDS,
    source_record_id_fields=("accident_index", "casualty_reference"),
)


@dataclass(frozen=True, slots=True)
class Stats19FetchConfig:
    """Runtime fetch options for the STATS19 CSV resources."""

    client: httpx.AsyncClient
    clock: Clock = field(default=utc_now)
    collisions_url: str = STATS19_COLLISIONS_URL
    vehicles_url: str = STATS19_VEHICLES_URL
    casualties_url: str = STATS19_CASUALTIES_URL


@dataclass(frozen=True, slots=True)
class Stats19CollisionsAdapter:
    """Fetches the DfT STATS19 collisions CSV for the pinned year."""

    fetch_config: Stats19FetchConfig

    @property
    def source_id(self) -> SourceId:
        return SOURCE_ID

    @property
    def dataset_id(self) -> DatasetId:
        return _COLLISIONS_TABLE.dataset_id

    @property
    def jurisdiction(self) -> Jurisdiction:
        return STATS19_JURISDICTION

    async def fetch(self) -> FetchResult:
        return await _fetch_table(
            self.fetch_config, self.fetch_config.collisions_url, _COLLISIONS_TABLE
        )


@dataclass(frozen=True, slots=True)
class Stats19VehiclesAdapter:
    """Fetches the DfT STATS19 vehicles CSV for the pinned year."""

    fetch_config: Stats19FetchConfig

    @property
    def source_id(self) -> SourceId:
        return SOURCE_ID

    @property
    def dataset_id(self) -> DatasetId:
        return _VEHICLES_TABLE.dataset_id

    @property
    def jurisdiction(self) -> Jurisdiction:
        return STATS19_JURISDICTION

    async def fetch(self) -> FetchResult:
        return await _fetch_table(
            self.fetch_config, self.fetch_config.vehicles_url, _VEHICLES_TABLE
        )


@dataclass(frozen=True, slots=True)
class Stats19CasualtiesAdapter:
    """Fetches the DfT STATS19 casualties CSV for the pinned year."""

    fetch_config: Stats19FetchConfig

    @property
    def source_id(self) -> SourceId:
        return SOURCE_ID

    @property
    def dataset_id(self) -> DatasetId:
        return _CASUALTIES_TABLE.dataset_id

    @property
    def jurisdiction(self) -> Jurisdiction:
        return STATS19_JURISDICTION

    async def fetch(self) -> FetchResult:
        return await _fetch_table(
            self.fetch_config, self.fetch_config.casualties_url, _CASUALTIES_TABLE
        )


async def _fetch_table(config: Stats19FetchConfig, url: str, spec: _TableSpec) -> FetchResult:
    fetched_at = config.clock()
    content = await fetch_csv_bytes(
        config.client,
        url,
        source_id=SOURCE_ID,
        dataset_id=spec.dataset_id,
        error_message=f"failed to read STATS19 CSV from {url}",
    )
    content_hash = hashlib.sha256(content).hexdigest()
    rows = _parse_csv(content, dataset_id=spec.dataset_id, expected_fields=spec.expected_fields)
    snapshot = _build_snapshot(
        fetched_at=fetched_at,
        record_count=len(rows),
        source_url=url,
        content_hash=content_hash,
        dataset_id=spec.dataset_id,
    )

    return FetchResult(
        snapshot=snapshot,
        records=_stream_records(snapshot=snapshot, rows=rows, spec=spec),
    )


def _parse_csv(
    content: bytes, *, dataset_id: DatasetId, expected_fields: tuple[str, ...]
) -> tuple[dict[str, str], ...]:
    text = _decode_csv(content)

    if not text.strip():
        raise FetchError(
            "empty STATS19 CSV response",
            source_id=SOURCE_ID,
            dataset_id=dataset_id,
            operation="parse-csv",
        )

    if "," not in text:
        raise FetchError(
            "non-CSV STATS19 response",
            source_id=SOURCE_ID,
            dataset_id=dataset_id,
            operation="parse-csv",
        )

    reader = csv.DictReader(io.StringIO(text))
    _validate_headers(reader.fieldnames, dataset_id=dataset_id, expected_fields=expected_fields)
    rows: list[dict[str, str]] = []

    try:
        for row_number, row in enumerate(reader, start=2):
            if None in row:
                raise FetchError(
                    f"malformed STATS19 CSV row {row_number}",
                    source_id=SOURCE_ID,
                    dataset_id=dataset_id,
                    operation="parse-csv",
                )

            rows.append({key: value or "" for key, value in row.items()})
    except csv.Error as e:
        raise FetchError(
            "invalid STATS19 CSV response",
            source_id=SOURCE_ID,
            dataset_id=dataset_id,
            operation="parse-csv",
        ) from e

    return tuple(rows)


def _decode_csv(content: bytes) -> str:
    try:
        return content.decode("utf-8-sig")
    except UnicodeDecodeError:
        return content.decode("cp1252")


def _validate_headers(
    fieldnames: Sequence[str] | None,
    *,
    dataset_id: DatasetId,
    expected_fields: tuple[str, ...],
) -> None:
    if fieldnames is None:
        raise FetchError(
            "missing STATS19 CSV header",
            source_id=SOURCE_ID,
            dataset_id=dataset_id,
            operation="parse-csv",
        )

    missing = tuple(field_name for field_name in expected_fields if field_name not in fieldnames)

    if missing:
        raise FetchError(
            f"STATS19 CSV header missing required fields: {', '.join(missing)}",
            source_id=SOURCE_ID,
            dataset_id=dataset_id,
            operation="parse-csv",
        )


async def _stream_records(
    *,
    snapshot: SourceSnapshot,
    rows: tuple[Mapping[str, str], ...],
    spec: _TableSpec,
) -> AsyncIterable[RawRecord]:
    for index, row in enumerate(rows, start=1):
        yield _build_record(snapshot=snapshot, row=row, spec=spec, index=index)


def _build_record(
    *,
    snapshot: SourceSnapshot,
    row: Mapping[str, str],
    spec: _TableSpec,
    index: int,
) -> RawRecord:
    source_record_id = _source_record_id(row, fields=spec.source_record_id_fields, index=index)
    record_hash = hashlib.sha256(
        json.dumps(row, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()

    try:
        return RawRecord(
            snapshot_id=snapshot.snapshot_id,
            raw_data=dict(row),
            source_record_id=source_record_id,
            source_updated_at=None,
            record_hash=record_hash,
        )
    except ValidationError as e:
        raise FetchError(
            "raw STATS19 record failed validation",
            source_id=SOURCE_ID,
            dataset_id=spec.dataset_id,
            operation="stream-records",
        ) from e


def _source_record_id(row: Mapping[str, str], *, fields: tuple[str, ...], index: int) -> str:
    parts: list[str] = []
    for field_name in fields:
        value = str_or_none(row.get(field_name))
        if value is None:
            return f"row:{index}"
        parts.append(value)

    return ":".join(parts)


def _build_snapshot(
    *,
    fetched_at: datetime,
    record_count: int,
    source_url: str,
    content_hash: str,
    dataset_id: DatasetId,
) -> SourceSnapshot:
    snapshot_id = SnapshotId(f"{SOURCE_ID}:{dataset_id}:{fetched_at.isoformat()}")

    return SourceSnapshot(
        snapshot_id=snapshot_id,
        source_id=SOURCE_ID,
        dataset_id=dataset_id,
        jurisdiction=STATS19_JURISDICTION,
        fetched_at=fetched_at,
        record_count=record_count,
        source_url=source_url,
        fetch_params={"release": STATS19_RELEASE},
        content_hash=content_hash,
    )
