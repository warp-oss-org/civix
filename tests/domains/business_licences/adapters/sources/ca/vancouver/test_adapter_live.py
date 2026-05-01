"""Live smoke test against the real Vancouver Open Data Portal.

Excluded from the default test suite. Run on demand with:

    uv run pytest -m live

The point is schema canary, not load testing — it checks that the
records endpoint still returns a parseable count and that exports
deliver at least one well-formed record.
"""

from __future__ import annotations

import pytest

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction
from civix.domains.business_licences.adapters.sources.ca.vancouver import (
    VancouverBusinessLicencesAdapter,
)
from civix.infra.http import default_http_client

pytestmark = pytest.mark.live


async def test_live_smoke_current_dataset_returns_records() -> None:
    async with default_http_client() as client:
        adapter = VancouverBusinessLicencesAdapter(
            dataset_id=DatasetId("business-licences"),
            jurisdiction=Jurisdiction(country="CA", region="BC", locality="Vancouver"),
            client=client,
        )
        result = await adapter.fetch()
        first_record = await anext(aiter(result.records))

    assert result.snapshot.record_count > 0
    assert first_record.snapshot_id == result.snapshot.snapshot_id
    assert "businessname" in first_record.raw_data
