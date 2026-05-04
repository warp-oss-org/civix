"""France BAAC / ONISR source adapter."""

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

SOURCE_ID: Final[SourceId] = SourceId("onisr-open-data")
BAAC_SOURCE_YEAR: Final[str] = "2024"
BAAC_RELEASE: Final[str] = "2024-data-gouv-2025-12-29"
BAAC_DATASET_LAST_UPDATE: Final[str] = "2025-12-29T09:29:20.308000+00:00"
BAAC_JURISDICTION: Final[Jurisdiction] = Jurisdiction(country="FR")

BAAC_CHARACTERISTICS_DATASET_ID: Final[DatasetId] = DatasetId("baac-caracteristiques-2024")
BAAC_LOCATIONS_DATASET_ID: Final[DatasetId] = DatasetId("baac-lieux-2024")
BAAC_VEHICLES_DATASET_ID: Final[DatasetId] = DatasetId("baac-vehicules-2024")
BAAC_USERS_DATASET_ID: Final[DatasetId] = DatasetId("baac-usagers-2024")

BAAC_CHARACTERISTICS_RESOURCE_ID: Final[str] = "83f0fb0e-e0ef-47fe-93dd-9aaee851674a"
BAAC_LOCATIONS_RESOURCE_ID: Final[str] = "228b3cda-fdfb-4677-bd54-ab2107028d2d"
BAAC_VEHICLES_RESOURCE_ID: Final[str] = "fd30513c-6b11-4a56-b6dc-5ac87728794b"
BAAC_USERS_RESOURCE_ID: Final[str] = "f57b1f58-386d-4048-8f78-2ebe435df868"

BAAC_CHARACTERISTICS_RESOURCE_TITLE: Final[str] = "Caract_2024.csv"
BAAC_LOCATIONS_RESOURCE_TITLE: Final[str] = "Lieux_2024.csv"
BAAC_VEHICLES_RESOURCE_TITLE: Final[str] = "Vehicules_2024.csv"
BAAC_USERS_RESOURCE_TITLE: Final[str] = "Usagers_2024.csv"

BAAC_RESOURCE_LAST_MODIFIED: Final[dict[DatasetId, str]] = {
    BAAC_CHARACTERISTICS_DATASET_ID: "2025-10-21T11:59:01.081000+00:00",
    BAAC_LOCATIONS_DATASET_ID: "2025-10-21T11:58:13.699000+00:00",
    BAAC_VEHICLES_DATASET_ID: "2025-12-29T09:29:20.308000+00:00",
    BAAC_USERS_DATASET_ID: "2025-10-21T11:56:56.552000+00:00",
}

BAAC_RESOURCE_URL_TEMPLATE: Final[str] = "https://www.data.gouv.fr/fr/datasets/r/{resource_id}"
BAAC_CHARACTERISTICS_URL: Final[str] = BAAC_RESOURCE_URL_TEMPLATE.format(
    resource_id=BAAC_CHARACTERISTICS_RESOURCE_ID
)
BAAC_LOCATIONS_URL: Final[str] = BAAC_RESOURCE_URL_TEMPLATE.format(
    resource_id=BAAC_LOCATIONS_RESOURCE_ID
)
BAAC_VEHICLES_URL: Final[str] = BAAC_RESOURCE_URL_TEMPLATE.format(
    resource_id=BAAC_VEHICLES_RESOURCE_ID
)
BAAC_USERS_URL: Final[str] = BAAC_RESOURCE_URL_TEMPLATE.format(resource_id=BAAC_USERS_RESOURCE_ID)

BAAC_SOURCE_SCOPE: Final[str] = (
    "Injury road traffic collisions on roads open to public traffic in France, "
    "recorded through the BAAC national file and published by ONISR/data.gouv.fr."
)
BAAC_LICENCE: Final[str] = "Licence Ouverte / Open Licence"
BAAC_RELEASE_CAVEATS: Final[tuple[str, ...]] = (
    "BAAC open data covers injury collisions and does not include property-damage-only collisions.",
    "Hospitalised-injury qualification changed from 2018 and the indicator is not "
    "labelled by the official statistics authority from 2019.",
    "Some privacy-sensitive investigation, user, vehicle, and behaviour details are "
    "omitted from the public extract.",
)

_CHARACTERISTICS_FIELDS: Final[tuple[str, ...]] = (
    "Num_Acc",
    "jour",
    "mois",
    "an",
    "hrmn",
    "lum",
    "agg",
    "int",
    "atm",
    "col",
    "adr",
    "lat",
    "long",
    "dep",
    "com",
)
_LOCATIONS_FIELDS: Final[tuple[str, ...]] = (
    "Num_Acc",
    "catr",
    "voie",
    "circ",
    "surf",
    "infra",
    "situ",
    "vma",
)
_VEHICLES_FIELDS: Final[tuple[str, ...]] = (
    "Num_Acc",
    "id_vehicule",
    "num_veh",
    "senc",
    "catv",
    "obs",
    "obsm",
    "choc",
    "manv",
    "motor",
    "occutc",
)
_USERS_FIELDS: Final[tuple[str, ...]] = (
    "Num_Acc",
    "id_usager",
    "id_vehicule",
    "num_veh",
    "place",
    "catu",
    "grav",
    "an_nais",
    "secu1",
    "secu2",
    "secu3",
    "locp",
    "actp",
    "etatp",
)


@dataclass(frozen=True, slots=True)
class _TableSpec:
    dataset_id: DatasetId
    resource_id: str
    expected_fields: tuple[str, ...]
    source_record_id_fields: tuple[str, ...]


_CHARACTERISTICS_TABLE: Final[_TableSpec] = _TableSpec(
    dataset_id=BAAC_CHARACTERISTICS_DATASET_ID,
    resource_id=BAAC_CHARACTERISTICS_RESOURCE_ID,
    expected_fields=_CHARACTERISTICS_FIELDS,
    source_record_id_fields=("Num_Acc",),
)
_LOCATIONS_TABLE: Final[_TableSpec] = _TableSpec(
    dataset_id=BAAC_LOCATIONS_DATASET_ID,
    resource_id=BAAC_LOCATIONS_RESOURCE_ID,
    expected_fields=_LOCATIONS_FIELDS,
    source_record_id_fields=("Num_Acc",),
)
_VEHICLES_TABLE: Final[_TableSpec] = _TableSpec(
    dataset_id=BAAC_VEHICLES_DATASET_ID,
    resource_id=BAAC_VEHICLES_RESOURCE_ID,
    expected_fields=_VEHICLES_FIELDS,
    source_record_id_fields=("Num_Acc", "id_vehicule"),
)
_USERS_TABLE: Final[_TableSpec] = _TableSpec(
    dataset_id=BAAC_USERS_DATASET_ID,
    resource_id=BAAC_USERS_RESOURCE_ID,
    expected_fields=_USERS_FIELDS,
    source_record_id_fields=("Num_Acc", "id_usager"),
)


@dataclass(frozen=True, slots=True)
class BaacFetchConfig:
    """Runtime fetch options for the BAAC CSV resources on data.gouv.fr."""

    client: httpx.AsyncClient
    clock: Clock = field(default=utc_now)
    characteristics_url: str = BAAC_CHARACTERISTICS_URL
    locations_url: str = BAAC_LOCATIONS_URL
    vehicles_url: str = BAAC_VEHICLES_URL
    users_url: str = BAAC_USERS_URL


@dataclass(frozen=True, slots=True)
class BaacCharacteristicsAdapter:
    """Fetches the BAAC characteristics CSV (Caract_2024.csv) for the pinned year."""

    fetch_config: BaacFetchConfig

    @property
    def source_id(self) -> SourceId:
        return SOURCE_ID

    @property
    def dataset_id(self) -> DatasetId:
        return _CHARACTERISTICS_TABLE.dataset_id

    @property
    def jurisdiction(self) -> Jurisdiction:
        return BAAC_JURISDICTION

    async def fetch(self) -> FetchResult:
        return await _fetch_table(
            self.fetch_config, self.fetch_config.characteristics_url, _CHARACTERISTICS_TABLE
        )


@dataclass(frozen=True, slots=True)
class BaacLocationsAdapter:
    """Fetches the BAAC locations CSV (Lieux_2024.csv) for the pinned year."""

    fetch_config: BaacFetchConfig

    @property
    def source_id(self) -> SourceId:
        return SOURCE_ID

    @property
    def dataset_id(self) -> DatasetId:
        return _LOCATIONS_TABLE.dataset_id

    @property
    def jurisdiction(self) -> Jurisdiction:
        return BAAC_JURISDICTION

    async def fetch(self) -> FetchResult:
        return await _fetch_table(
            self.fetch_config, self.fetch_config.locations_url, _LOCATIONS_TABLE
        )


@dataclass(frozen=True, slots=True)
class BaacVehiclesAdapter:
    """Fetches the BAAC vehicles CSV (Vehicules_2024.csv) for the pinned year."""

    fetch_config: BaacFetchConfig

    @property
    def source_id(self) -> SourceId:
        return SOURCE_ID

    @property
    def dataset_id(self) -> DatasetId:
        return _VEHICLES_TABLE.dataset_id

    @property
    def jurisdiction(self) -> Jurisdiction:
        return BAAC_JURISDICTION

    async def fetch(self) -> FetchResult:
        return await _fetch_table(
            self.fetch_config, self.fetch_config.vehicles_url, _VEHICLES_TABLE
        )


@dataclass(frozen=True, slots=True)
class BaacUsersAdapter:
    """Fetches the BAAC users CSV (Usagers_2024.csv) for the pinned year."""

    fetch_config: BaacFetchConfig

    @property
    def source_id(self) -> SourceId:
        return SOURCE_ID

    @property
    def dataset_id(self) -> DatasetId:
        return _USERS_TABLE.dataset_id

    @property
    def jurisdiction(self) -> Jurisdiction:
        return BAAC_JURISDICTION

    async def fetch(self) -> FetchResult:
        return await _fetch_table(self.fetch_config, self.fetch_config.users_url, _USERS_TABLE)


async def _fetch_table(config: BaacFetchConfig, url: str, spec: _TableSpec) -> FetchResult:
    fetched_at = config.clock()
    # data.gouv.fr issues signed redirects to object storage for BAAC CSV
    # archives, so this fetch must follow redirects even though the shared
    # helper defaults to off.
    content = await fetch_csv_bytes(
        config.client,
        url,
        source_id=SOURCE_ID,
        dataset_id=spec.dataset_id,
        follow_redirects=True,
        error_message=f"failed to read BAAC CSV from {url}",
    )
    content_hash = hashlib.sha256(content).hexdigest()
    rows = _parse_csv(content, dataset_id=spec.dataset_id, expected_fields=spec.expected_fields)
    snapshot = _build_snapshot(
        fetched_at=fetched_at,
        record_count=len(rows),
        source_url=url,
        content_hash=content_hash,
        dataset_id=spec.dataset_id,
        resource_id=spec.resource_id,
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
            "empty BAAC CSV response",
            source_id=SOURCE_ID,
            dataset_id=dataset_id,
            operation="parse-csv",
        )

    if ";" not in text:
        raise FetchError(
            "non-CSV BAAC response",
            source_id=SOURCE_ID,
            dataset_id=dataset_id,
            operation="parse-csv",
        )

    reader = csv.DictReader(io.StringIO(text), delimiter=";")
    _validate_headers(reader.fieldnames, dataset_id=dataset_id, expected_fields=expected_fields)
    rows: list[dict[str, str]] = []

    try:
        for row_number, row in enumerate(reader, start=2):
            if None in row:
                raise FetchError(
                    f"malformed BAAC CSV row {row_number}",
                    source_id=SOURCE_ID,
                    dataset_id=dataset_id,
                    operation="parse-csv",
                )

            rows.append({key: value or "" for key, value in row.items()})
    except csv.Error as e:
        raise FetchError(
            "invalid BAAC CSV response",
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
            "missing BAAC CSV header",
            source_id=SOURCE_ID,
            dataset_id=dataset_id,
            operation="parse-csv",
        )

    missing = tuple(field_name for field_name in expected_fields if field_name not in fieldnames)

    if missing:
        raise FetchError(
            f"BAAC CSV header missing required fields: {', '.join(missing)}",
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
            "raw BAAC record failed validation",
            source_id=SOURCE_ID,
            dataset_id=spec.dataset_id,
            operation="stream-records",
        ) from e


def _source_record_id(row: Mapping[str, str], *, fields: tuple[str, ...], index: int) -> str | None:
    parts = tuple(str_or_none(row.get(field_name)) for field_name in fields)

    if None in parts:
        return None

    return ":".join(parts)  # type: ignore[arg-type]


def _build_snapshot(
    *,
    fetched_at: datetime,
    record_count: int,
    source_url: str,
    content_hash: str,
    dataset_id: DatasetId,
    resource_id: str,
) -> SourceSnapshot:
    snapshot_id = SnapshotId(f"{SOURCE_ID}:{dataset_id}:{fetched_at.isoformat()}")

    return SourceSnapshot(
        snapshot_id=snapshot_id,
        source_id=SOURCE_ID,
        dataset_id=dataset_id,
        jurisdiction=BAAC_JURISDICTION,
        fetched_at=fetched_at,
        record_count=record_count,
        source_url=source_url,
        fetch_params={"resource_id": resource_id, "release": BAAC_RELEASE},
        content_hash=content_hash,
    )
