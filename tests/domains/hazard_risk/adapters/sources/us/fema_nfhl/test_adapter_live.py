"""Live smoke test against the real FEMA NFHL ArcGIS MapServer.

Excluded from the default test suite. Run on demand with:

    uv run pytest -m live

Smoke-tests the ArcGIS feature-layer pagination shape: that the count
probe still returns an int and that the layer still serves features
whose attributes carry the FLD_AR_ID identity field the mapper relies on.
"""

from __future__ import annotations

import pytest

from civix.domains.hazard_risk.adapters.sources.us.fema_nfhl import (
    FemaNfhlFloodHazardZonesAdapter,
    FemaNfhlFloodHazardZonesFetchConfig,
)
from civix.infra.http import default_http_client

pytestmark = pytest.mark.live


async def test_live_smoke_current_dataset_returns_records() -> None:
    async with default_http_client(timeout=60.0) as client:
        adapter = FemaNfhlFloodHazardZonesAdapter(
            fetch_config=FemaNfhlFloodHazardZonesFetchConfig(
                client=client,
                page_size=5,
            )
        )

        result = await adapter.fetch()
        first_record = await anext(aiter(result.records))

    assert result.snapshot.record_count > 0
    assert first_record.snapshot_id == result.snapshot.snapshot_id
    assert first_record.source_record_id is not None
    assert "FLD_AR_ID" in first_record.raw_data
    assert "FLD_ZONE" in first_record.raw_data
