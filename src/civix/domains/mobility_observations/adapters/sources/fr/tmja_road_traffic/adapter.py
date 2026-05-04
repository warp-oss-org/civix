"""France TMJA road-traffic source adapter."""

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

SOURCE_ID: Final[SourceId] = SourceId("fr-mte-road-traffic")
FR_TMJA_RRNC_2024_DATASET_ID: Final[DatasetId] = DatasetId("tmja-rrnc-2024")
FR_TMJA_JURISDICTION: Final[Jurisdiction] = Jurisdiction(country="FR")
FR_TMJA_RESOURCE_ID: Final[str] = "dbec4f42-b5fc-429f-b913-eeb758777383"
FR_TMJA_RESOURCE_TITLE: Final[str] = "TMJA_RRNc_2024"
FR_TMJA_RESOURCE_LAST_MODIFIED: Final[str] = "2025-08-18T10:01:54.875000+00:00"
DEFAULT_RESOURCE_URL: Final[str] = (
    "https://static.data.gouv.fr/resources/"
    "trafic-moyen-journalier-annuel-sur-le-reseau-routier-national/"
    "20250818-100154/tmja-rrnc-2024.csv"
)

FR_TMJA_SOURCE_SCOPE: Final[str] = (
    "France Ministry for Ecological Transition TMJA_RRNc_2024 resource published on "
    "data.gouv.fr. Sprint 6 covers the 2024 concessioned national-road-network CSV only; "
    "future annual releases and non-concessioned or departmental road-network resources are "
    "intentional code-and-fixture follow-up work."
)
FR_TMJA_RELEASE_CAVEATS: Final[tuple[str, ...]] = (
    "Published under Licence Ouverte / Open Licence through data.gouv.fr.",
    "TMJA is annual-average daily traffic, not raw observed daily counts.",
    "The selected TMJA_RRNc_2024 resource covers the concessioned national road network.",
    "ratio_PL is preserved raw and is not mapped into a derived truck count.",
)

_EXPECTED_FIELDS: Final[tuple[str, ...]] = (
    "dateReferentiel",
    "route",
    "longueur",
    "prD",
    "depPrD",
    "concessionPrD",
    "absD",
    "cumulD",
    "xD",
    "yD",
    "zD",
    "prF",
    "depPrF",
    "concessionPrF",
    "absF",
    "cumulF",
    "xF",
    "yF",
    "zF",
    "cote",
    "anneeMesureTrafic",
    "typeComptageTrafic",
    "typeComptageTrafic_lib",
    "TMJA",
    "ratio_PL",
)


@dataclass(frozen=True, slots=True)
class FrTmjaRoadTrafficFetchConfig:
    """Runtime fetch options for the year-pinned TMJA CSV."""

    client: httpx.AsyncClient
    clock: Clock = field(default=utc_now)
    resource_url: str = DEFAULT_RESOURCE_URL


@dataclass(frozen=True, slots=True)
class FrTmjaRoadTrafficAdapter:
    """Fetches the France TMJA_RRNc_2024 CSV resource."""

    fetch_config: FrTmjaRoadTrafficFetchConfig

    @property
    def source_id(self) -> SourceId:
        return SOURCE_ID

    @property
    def dataset_id(self) -> DatasetId:
        return FR_TMJA_RRNC_2024_DATASET_ID

    @property
    def jurisdiction(self) -> Jurisdiction:
        return FR_TMJA_JURISDICTION

    async def fetch(self) -> FetchResult:
        fetched_at = self.fetch_config.clock()
        content = await fetch_csv_bytes(
            self.fetch_config.client,
            self.fetch_config.resource_url,
            source_id=SOURCE_ID,
            dataset_id=FR_TMJA_RRNC_2024_DATASET_ID,
            error_message=(f"failed to read TMJA CSV from {self.fetch_config.resource_url}"),
        )
        content_hash = hashlib.sha256(content).hexdigest()
        rows = _parse_csv(content)
        snapshot = _build_snapshot(
            fetched_at=fetched_at,
            record_count=len(rows),
            source_url=self.fetch_config.resource_url,
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
            "empty TMJA CSV response",
            source_id=SOURCE_ID,
            dataset_id=FR_TMJA_RRNC_2024_DATASET_ID,
            operation="parse-csv",
        )

    if ";" not in text:
        raise FetchError(
            "non-CSV TMJA response",
            source_id=SOURCE_ID,
            dataset_id=FR_TMJA_RRNC_2024_DATASET_ID,
            operation="parse-csv",
        )

    reader = csv.DictReader(io.StringIO(text), delimiter=";")
    _validate_headers(reader.fieldnames)
    rows: list[dict[str, str]] = []

    try:
        for row_number, row in enumerate(reader, start=2):
            if None in row:
                raise FetchError(
                    f"malformed TMJA CSV row {row_number}",
                    source_id=SOURCE_ID,
                    dataset_id=FR_TMJA_RRNC_2024_DATASET_ID,
                    operation="parse-csv",
                )

            rows.append({key: value or "" for key, value in row.items()})
    except csv.Error as e:
        raise FetchError(
            "invalid TMJA CSV response",
            source_id=SOURCE_ID,
            dataset_id=FR_TMJA_RRNC_2024_DATASET_ID,
            operation="parse-csv",
        ) from e

    return tuple(rows)


def _decode_csv(content: bytes) -> str:
    try:
        return content.decode("utf-8-sig")
    except UnicodeDecodeError:
        return content.decode("cp1252")


def _validate_headers(fieldnames: Sequence[str] | None) -> None:
    if fieldnames is None:
        raise FetchError(
            "missing TMJA CSV header",
            source_id=SOURCE_ID,
            dataset_id=FR_TMJA_RRNC_2024_DATASET_ID,
            operation="parse-csv",
        )

    missing = tuple(field for field in _EXPECTED_FIELDS if field not in fieldnames)

    if missing:
        raise FetchError(
            f"TMJA CSV header missing required fields: {', '.join(missing)}",
            source_id=SOURCE_ID,
            dataset_id=FR_TMJA_RRNC_2024_DATASET_ID,
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
    source_record_id = _source_record_id(row, index=index)
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
            "raw TMJA record failed validation",
            source_id=SOURCE_ID,
            dataset_id=FR_TMJA_RRNC_2024_DATASET_ID,
            operation="stream-records",
        ) from e


def _source_record_id(row: Mapping[str, str], *, index: int) -> str | None:
    route = _source_id_part(row.get("route"))
    cumul_d = _source_id_part(row.get("cumulD"))
    cumul_f = _source_id_part(row.get("cumulF"))
    cote = _source_id_part(row.get("cote"))
    year = _source_id_part(row.get("anneeMesureTrafic"))

    if None in (route, cumul_d, cumul_f, cote, year):
        return None

    return f"{route}:{cumul_d}:{cumul_f}:{cote}:{year}:row-{index}"


def _source_id_part(value: object) -> str | None:
    return str_or_none(value)


def _build_snapshot(
    *,
    fetched_at: datetime,
    record_count: int,
    source_url: str,
    content_hash: str,
) -> SourceSnapshot:
    snapshot_id = SnapshotId(f"{SOURCE_ID}:{FR_TMJA_RRNC_2024_DATASET_ID}:{fetched_at.isoformat()}")

    return SourceSnapshot(
        snapshot_id=snapshot_id,
        source_id=SOURCE_ID,
        dataset_id=FR_TMJA_RRNC_2024_DATASET_ID,
        jurisdiction=FR_TMJA_JURISDICTION,
        fetched_at=fetched_at,
        record_count=record_count,
        source_url=source_url,
        fetch_params={"resource_id": FR_TMJA_RESOURCE_ID},
        content_hash=content_hash,
    )
