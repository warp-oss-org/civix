"""Live smoke test against the real OpenFEMA HMA Projects endpoint.

Excluded from the default test suite. Run on demand with:

    uv run pytest -m live

Smoke-tests the shared OpenFEMA fetch loop: that the v4 HMA Projects
entity still answers with the expected metadata/count envelope and that
the first page deserializes into raw records with the keys the mapper
expects.
"""

from __future__ import annotations

import pytest

from civix.domains.hazard_mitigation.adapters.sources.us.fema_hma import (
    FEMA_HMA_PROJECTS_ORDER,
    FemaHmaProjectsAdapter,
)
from civix.infra.http import default_http_client
from civix.infra.sources.openfema import OpenFemaFetchConfig

pytestmark = pytest.mark.live


async def test_live_smoke_current_dataset_returns_records() -> None:
    async with default_http_client() as client:
        adapter = FemaHmaProjectsAdapter(
            fetch_config=OpenFemaFetchConfig(
                client=client,
                page_size=5,
                order_by=FEMA_HMA_PROJECTS_ORDER,
            )
        )

        result = await adapter.fetch()
        first_record = await anext(aiter(result.records))

    assert result.snapshot.record_count > 0
    assert first_record.snapshot_id == result.snapshot.snapshot_id
    assert "projectIdentifier" in first_record.raw_data
