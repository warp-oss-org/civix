from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Final

import httpx
import respx

from civix.domains.transportation_safety.adapters.sources.fr.baac import (
    BAAC_CHARACTERISTICS_DATASET_ID,
    BAAC_CHARACTERISTICS_URL,
)
from civix.domains.transportation_safety.adapters.sources.gb.stats19 import (
    STATS19_COLLISIONS_DATASET_ID,
    STATS19_COLLISIONS_URL,
)
from civix.sdk import Civix

PINNED_NOW: Final[datetime] = datetime(2026, 1, 1, tzinfo=UTC)
STATS19_FIXTURES: Final[Path] = Path(
    "tests/domains/transportation_safety/adapters/sources/gb/stats19/fixtures"
)
BAAC_FIXTURES: Final[Path] = Path(
    "tests/domains/transportation_safety/adapters/sources/fr/baac/fixtures"
)


async def test_stats19_collisions_fetches_through_sdk() -> None:
    fixture = STATS19_FIXTURES / "collisions.csv"

    async with httpx.AsyncClient() as http_client:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(STATS19_COLLISIONS_URL).mock(
                return_value=httpx.Response(200, content=fixture.read_bytes())
            )
            client = Civix(http_client=http_client, clock=lambda: PINNED_NOW)

            result = await client.fetch(
                client.datasets.gb.transportation_safety.collision.stats19_collisions
            )
            records = [record async for record in result.records]

    assert result.snapshot.dataset_id == STATS19_COLLISIONS_DATASET_ID
    assert len(records) == 1
    assert records[0].raw.source_record_id is not None


async def test_baac_characteristics_fetches_through_sdk() -> None:
    fixture = BAAC_FIXTURES / "caracteristiques.csv"

    async with httpx.AsyncClient() as http_client:
        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(BAAC_CHARACTERISTICS_URL).mock(
                return_value=httpx.Response(200, content=fixture.read_bytes())
            )
            client = Civix(http_client=http_client, clock=lambda: PINNED_NOW)

            result = await client.fetch(
                client.datasets.fr.transportation_safety.collision.baac_characteristics
            )
            records = [record async for record in result.records]

    assert result.snapshot.dataset_id == BAAC_CHARACTERISTICS_DATASET_ID
    assert len(records) == 2
    assert records[0].raw.source_record_id is not None
