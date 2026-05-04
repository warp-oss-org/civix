"""France Georisques GASPAR PPRN source adapter."""

from __future__ import annotations

import csv
import hashlib
import io
import json
from collections.abc import AsyncIterable, Iterator, Mapping, Sequence
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
from civix.domains.hazard_risk.adapters.sources.fr.georisques_pprn.schema import (
    GEORISQUES_PPRN_FIELDS,
)
from civix.infra.sources.csv import fetch_csv_bytes

SOURCE_ID: Final[SourceId] = SourceId("georisques-gaspar")
GEORISQUES_PPRN_DATASET_ID: Final[DatasetId] = DatasetId("pprn_gaspar")
FR_JURISDICTION: Final[Jurisdiction] = Jurisdiction(country="FR")
GEORISQUES_PPRN_CSV_URL: Final[str] = "https://files.georisques.fr/GASPAR/pprn_gaspar.csv"
GEORISQUES_PPRN_SOURCE_SCOPE: Final[str] = (
    "France Georisques GASPAR PPRN CSV export. Rows are commune-grained "
    "administrative risk-prevention-plan procedure records, not plan polygons."
)


@dataclass(frozen=True, slots=True)
class GeorisquesPprnFetchConfig:
    """Runtime fetch options for the Georisques GASPAR PPRN CSV."""

    client: httpx.AsyncClient
    clock: Clock = field(default=utc_now)
    csv_url: str = GEORISQUES_PPRN_CSV_URL


@dataclass(frozen=True, slots=True)
class GeorisquesPprnAdapter:
    """Fetches Georisques GASPAR PPRN CSV rows."""

    fetch_config: GeorisquesPprnFetchConfig

    @property
    def source_id(self) -> SourceId:
        return SOURCE_ID

    @property
    def dataset_id(self) -> DatasetId:
        return GEORISQUES_PPRN_DATASET_ID

    @property
    def jurisdiction(self) -> Jurisdiction:
        return FR_JURISDICTION

    async def fetch(self) -> FetchResult:
        fetched_at = self.fetch_config.clock()
        content = await fetch_csv_bytes(
            self.fetch_config.client,
            self.fetch_config.csv_url,
            source_id=SOURCE_ID,
            dataset_id=GEORISQUES_PPRN_DATASET_ID,
            error_message=(
                f"failed to read Georisques PPRN CSV from {self.fetch_config.csv_url}"
            ),
        )
        content_hash = hashlib.sha256(content).hexdigest()
        rows = _parse_csv(content)
        snapshot = _build_snapshot(
            fetched_at=fetched_at,
            record_count=len(rows),
            source_url=self.fetch_config.csv_url,
            content_hash=content_hash,
        )

        return FetchResult(
            snapshot=snapshot,
            records=_stream_records(snapshot=snapshot, rows=rows),
        )


def _parse_csv(content: bytes) -> tuple[dict[str, str], ...]:
    text = _decode_csv(content)

    if not text.strip():
        raise FetchError(
            "empty Georisques PPRN CSV response",
            source_id=SOURCE_ID,
            dataset_id=GEORISQUES_PPRN_DATASET_ID,
            operation="parse-csv",
        )

    if ";" not in text:
        raise FetchError(
            "non-CSV Georisques PPRN response",
            source_id=SOURCE_ID,
            dataset_id=GEORISQUES_PPRN_DATASET_ID,
            operation="parse-csv",
        )

    csv_stream = io.StringIO(text)
    raw_reader = csv.reader(csv_stream, delimiter=";")
    raw_fieldnames = _read_header(raw_reader)
    fieldnames = _canonical_fieldnames(raw_fieldnames)
    _validate_headers(fieldnames)
    reader = csv.DictReader(csv_stream, delimiter=";", fieldnames=fieldnames)
    rows: list[dict[str, str]] = []

    try:
        for row_number, row in enumerate(reader, start=2):
            if None in row or any(value is None for value in row.values()):
                raise FetchError(
                    f"malformed Georisques PPRN CSV row {row_number}",
                    source_id=SOURCE_ID,
                    dataset_id=GEORISQUES_PPRN_DATASET_ID,
                    operation="parse-csv",
                )

            rows.append({key: value or "" for key, value in row.items()})
    except csv.Error as e:
        raise FetchError(
            "invalid Georisques PPRN CSV response",
            source_id=SOURCE_ID,
            dataset_id=GEORISQUES_PPRN_DATASET_ID,
            operation="parse-csv",
        ) from e

    return tuple(rows)


def _decode_csv(content: bytes) -> str:
    try:
        return content.decode("utf-8")
    except UnicodeDecodeError:
        return content.decode("cp1252")


def _read_header(reader: Iterator[list[str]]) -> list[str]:
    try:
        return next(reader)
    except StopIteration as e:
        raise FetchError(
            "missing Georisques PPRN CSV header",
            source_id=SOURCE_ID,
            dataset_id=GEORISQUES_PPRN_DATASET_ID,
            operation="parse-csv",
        ) from e


def _canonical_fieldnames(fieldnames: Sequence[str]) -> tuple[str, ...]:
    seen: dict[str, int] = {}
    canonical: list[str] = []

    for field_name in fieldnames:
        count = seen.get(field_name, 0) + 1
        seen[field_name] = count

        if field_name == "CODE RISQUE 2" and count == 2:
            canonical.append("CODE RISQUE 3")
            continue

        canonical.append(field_name)

    return tuple(canonical)


def _validate_headers(fieldnames: Sequence[str]) -> None:
    missing = tuple(field for field in GEORISQUES_PPRN_FIELDS if field not in fieldnames)

    if missing:
        raise FetchError(
            f"Georisques PPRN CSV header missing required fields: {', '.join(missing)}",
            source_id=SOURCE_ID,
            dataset_id=GEORISQUES_PPRN_DATASET_ID,
            operation="parse-csv",
        )


async def _stream_records(
    *,
    snapshot: SourceSnapshot,
    rows: tuple[Mapping[str, str], ...],
) -> AsyncIterable[RawRecord]:
    for index, row in enumerate(rows, start=1):
        yield _build_record(snapshot=snapshot, row=row, index=index)


def _build_record(
    *,
    snapshot: SourceSnapshot,
    row: Mapping[str, str],
    index: int,
) -> RawRecord:
    record_hash = hashlib.sha256(
        json.dumps(row, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()

    try:
        return RawRecord(
            snapshot_id=snapshot.snapshot_id,
            raw_data=dict(row),
            source_record_id=_source_record_id(row, record_hash=record_hash),
            source_updated_at=None,
            record_hash=record_hash,
        )
    except ValidationError as e:
        raise FetchError(
            "Georisques PPRN raw record failed validation",
            source_id=SOURCE_ID,
            dataset_id=GEORISQUES_PPRN_DATASET_ID,
            operation="stream-records",
        ) from e


def _source_record_id(row: Mapping[str, str], *, record_hash: str) -> str:
    plan_id = _source_id_part(row.get("CODE PROECEDURE"))
    commune_id = _source_id_part(row.get("CODE INSEE COMMUNE"))
    risk_id = _source_id_part(row.get("CODE RISQUE 3") or row.get("CODE RISQUE 2"))

    return f"{plan_id}:{commune_id}:{risk_id}:sha256-{record_hash[:16]}"


def _source_id_part(value: object) -> str:
    return str_or_none(value) or "blank"


def _build_snapshot(
    *,
    fetched_at: datetime,
    record_count: int,
    source_url: str,
    content_hash: str,
) -> SourceSnapshot:
    snapshot_id = SnapshotId(f"{SOURCE_ID}:{GEORISQUES_PPRN_DATASET_ID}:{fetched_at.isoformat()}")

    return SourceSnapshot(
        snapshot_id=snapshot_id,
        source_id=SOURCE_ID,
        dataset_id=GEORISQUES_PPRN_DATASET_ID,
        jurisdiction=FR_JURISDICTION,
        fetched_at=fetched_at,
        record_count=record_count,
        source_url=source_url,
        fetch_params={"format": "csv", "delimiter": ";"},
        content_hash=content_hash,
    )
