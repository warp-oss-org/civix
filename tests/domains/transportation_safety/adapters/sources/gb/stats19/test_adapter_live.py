"""Live smoke test against the real DfT STATS19 CSV resources.

Excluded from the default test suite. Run on demand with:

    uv run pytest -m live

Smoke-tests the CSV-direct fetch path: that the published 2024-final
casualty CSV is still reachable, decodes cleanly, and exposes the header
row the parser validates against.
"""

from __future__ import annotations

import pytest

from civix.domains.transportation_safety.adapters.sources.gb.stats19 import (
    Stats19CasualtiesAdapter,
    Stats19FetchConfig,
)
from civix.infra.http import default_http_client

pytestmark = pytest.mark.live


async def test_live_smoke_current_dataset_returns_records() -> None:
    async with default_http_client(timeout=120.0) as client:
        adapter = Stats19CasualtiesAdapter(
            fetch_config=Stats19FetchConfig(client=client),
        )

        result = await adapter.fetch()
        first_record = await anext(aiter(result.records))

    assert result.snapshot.record_count > 0
    assert first_record.snapshot_id == result.snapshot.snapshot_id
    assert "accident_index" in first_record.raw_data
    assert "casualty_reference" in first_record.raw_data
    assert "casualty_severity" in first_record.raw_data
