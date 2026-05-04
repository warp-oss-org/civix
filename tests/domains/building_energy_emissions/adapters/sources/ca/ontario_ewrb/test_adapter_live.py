"""Live smoke test against the real Ontario EWRB workbook on data.ontario.ca.

Excluded from the default test suite. Run on demand with:

    uv run pytest -m live

Smoke-tests the XLSX-direct fetch path: that the published EWRB yearly
workbook is still reachable, parseable by openpyxl, and exposes the
canonical ewrb_id field that drives raw record identity.
"""

from __future__ import annotations

import pytest

from civix.domains.building_energy_emissions.adapters.sources.ca.ontario_ewrb import (
    OntarioEwrbAdapter,
    OntarioEwrbFetchConfig,
)
from civix.infra.http import default_http_client

pytestmark = pytest.mark.live


async def test_live_smoke_current_dataset_returns_records() -> None:
    async with default_http_client(timeout=120.0) as client:
        adapter = OntarioEwrbAdapter(
            fetch_config=OntarioEwrbFetchConfig(client=client),
        )

        result = await adapter.fetch()
        first_record = await anext(aiter(result.records))

    assert result.snapshot.record_count > 0
    assert first_record.snapshot_id == result.snapshot.snapshot_id
    assert "ewrb_id" in first_record.raw_data
    assert "reporting_year" in first_record.raw_data
