"""Single-shot CSV byte acquisition over httpx."""

from __future__ import annotations

import httpx

from civix.core.identity.models.identifiers import DatasetId, SourceId
from civix.core.ports.errors import FetchError


async def fetch_csv_bytes(
    client: httpx.AsyncClient,
    url: str,
    *,
    source_id: SourceId,
    dataset_id: DatasetId,
    follow_redirects: bool = False,
    error_message: str | None = None,
    operation: str = "fetch-csv",
) -> bytes:
    """Fetch a CSV resource and return its raw bytes.

    `follow_redirects` is opt-in because some open-data portals (notably
    data.gouv.fr) issue signed redirects to object storage that the caller
    must follow to reach the CSV body.
    """
    try:
        response = await client.get(url, follow_redirects=follow_redirects)

        response.raise_for_status()
    except httpx.HTTPError as e:
        raise FetchError(
            error_message if error_message is not None else f"failed to read CSV from {url}",
            source_id=source_id,
            dataset_id=dataset_id,
            operation=operation,
        ) from e

    return response.content
