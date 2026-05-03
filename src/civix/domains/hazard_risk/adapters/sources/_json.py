"""Shared JSON-array fetch helpers for fixture-backed hazard-risk sources."""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast

import httpx
from pydantic import ValidationError

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, SnapshotId, SourceId
from civix.core.mapping.parsers import str_or_none
from civix.core.ports.errors import FetchError
from civix.core.ports.models.adapter import FetchResult
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot


@dataclass(frozen=True, slots=True)
class JsonArraySourceSpec:
    """Source identity and parsing contract for a JSON-array source."""

    source_id: SourceId
    dataset_id: DatasetId
    jurisdiction: Jurisdiction
    source_label: str
    id_fields: tuple[str, ...]
    records_key: str = "records"


async def fetch_json_array_source(
    *,
    client: httpx.AsyncClient,
    fetched_at: datetime,
    source_url: str,
    spec: JsonArraySourceSpec,
    fetch_params: Mapping[str, str] | None = None,
) -> FetchResult:
    """Fetch a source JSON document whose record payload is an array."""
    content = await _fetch_bytes(client=client, source_url=source_url, spec=spec)
    content_hash = hashlib.sha256(content).hexdigest()
    rows = _parse_rows(content=content, spec=spec)
    snapshot = _build_snapshot(
        fetched_at=fetched_at,
        source_url=source_url,
        content_hash=content_hash,
        record_count=len(rows),
        spec=spec,
        fetch_params=fetch_params,
    )

    return FetchResult(
        snapshot=snapshot,
        records=_stream_records(snapshot=snapshot, rows=rows, spec=spec),
    )


async def _fetch_bytes(
    *,
    client: httpx.AsyncClient,
    source_url: str,
    spec: JsonArraySourceSpec,
) -> bytes:
    try:
        response = await client.get(source_url)

        response.raise_for_status()
    except httpx.HTTPError as e:
        raise FetchError(
            f"failed to read {spec.source_label} JSON from {source_url}",
            source_id=spec.source_id,
            dataset_id=spec.dataset_id,
            operation="fetch-json",
        ) from e

    return response.content


def _parse_rows(*, content: bytes, spec: JsonArraySourceSpec) -> tuple[dict[str, Any], ...]:
    try:
        payload: object = json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as e:
        raise FetchError(
            f"invalid {spec.source_label} JSON response",
            source_id=spec.source_id,
            dataset_id=spec.dataset_id,
            operation="parse-json",
        ) from e

    if isinstance(payload, dict):
        payload_dict = cast(dict[str, object], payload)
        rows_value: object = payload_dict.get(spec.records_key)
    else:
        rows_value = payload
    if not isinstance(rows_value, list):
        raise FetchError(
            f"{spec.source_label} JSON response missing records array",
            source_id=spec.source_id,
            dataset_id=spec.dataset_id,
            operation="parse-json",
        )

    rows: list[dict[str, Any]] = []
    rows_list = cast(list[object], rows_value)
    for index, item in enumerate(rows_list, start=1):
        if not isinstance(item, dict):
            raise FetchError(
                f"{spec.source_label} JSON record {index} is not an object",
                source_id=spec.source_id,
                dataset_id=spec.dataset_id,
                operation="parse-json",
            )

        rows.append(cast(dict[str, Any], item))

    return tuple(rows)


async def _stream_records(
    *,
    snapshot: SourceSnapshot,
    rows: tuple[Mapping[str, Any], ...],
    spec: JsonArraySourceSpec,
) -> AsyncIterable[RawRecord]:
    for row in rows:
        yield _build_record(snapshot=snapshot, row=row, spec=spec)


def _build_record(
    *,
    snapshot: SourceSnapshot,
    row: Mapping[str, Any],
    spec: JsonArraySourceSpec,
) -> RawRecord:
    record_hash = hashlib.sha256(
        json.dumps(row, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()

    try:
        return RawRecord(
            snapshot_id=snapshot.snapshot_id,
            raw_data=dict(row),
            source_record_id=_source_record_id(row=row, record_hash=record_hash, spec=spec),
            source_updated_at=None,
            record_hash=record_hash,
        )
    except ValidationError as e:
        raise FetchError(
            f"{spec.source_label} raw record failed validation",
            source_id=spec.source_id,
            dataset_id=spec.dataset_id,
            operation="stream-records",
        ) from e


def _source_record_id(
    *,
    row: Mapping[str, Any],
    record_hash: str,
    spec: JsonArraySourceSpec,
) -> str:
    parts = tuple(str_or_none(row.get(field_name)) or "blank" for field_name in spec.id_fields)

    return f"{':'.join(parts)}:sha256-{record_hash[:16]}"


def _build_snapshot(
    *,
    fetched_at: datetime,
    source_url: str,
    content_hash: str,
    record_count: int,
    spec: JsonArraySourceSpec,
    fetch_params: Mapping[str, str] | None,
) -> SourceSnapshot:
    snapshot_id = SnapshotId(f"{spec.source_id}:{spec.dataset_id}:{fetched_at.isoformat()}")
    snapshot_fetch_params = {"format": "json", "records_key": spec.records_key}

    if fetch_params is not None:
        snapshot_fetch_params.update(fetch_params)

    return SourceSnapshot(
        snapshot_id=snapshot_id,
        source_id=spec.source_id,
        dataset_id=spec.dataset_id,
        jurisdiction=spec.jurisdiction,
        fetched_at=fetched_at,
        record_count=record_count,
        source_url=source_url,
        fetch_params=snapshot_fetch_params,
        content_hash=content_hash,
    )
