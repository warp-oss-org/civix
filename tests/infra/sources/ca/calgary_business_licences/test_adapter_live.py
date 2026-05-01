"""Live smoke test against the real Calgary Socrata endpoint.

Excluded from the default test suite. Run on demand with:

    uv run pytest -m live
"""

from __future__ import annotations

import pytest

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction
from civix.infra.http import default_http_client
from civix.infra.sources.ca.calgary_business_licences import (
    CalgaryBusinessLicencesAdapter,
)

pytestmark = pytest.mark.live


async def test_live_smoke_current_dataset_returns_records() -> None:
    async with default_http_client() as client:
        adapter = CalgaryBusinessLicencesAdapter(
            dataset_id=DatasetId("vdjc-pybd"),
            jurisdiction=Jurisdiction(country="CA", region="AB", locality="Calgary"),
            client=client,
            page_size=5,
        )
        result = await adapter.fetch()
        first_record = await anext(aiter(result.records))

    assert result.snapshot.record_count > 0
    assert first_record.snapshot_id == result.snapshot.snapshot_id
    assert "getbusid" in first_record.raw_data
    assert "jobstatusdesc" in first_record.raw_data
    assert all(not key.startswith(":@computed_region_") for key in first_record.raw_data)
