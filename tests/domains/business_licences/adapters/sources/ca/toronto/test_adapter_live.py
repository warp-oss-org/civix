"""Live smoke test against the real Toronto Open Data Portal.

Excluded from the default test suite. Run on demand with:

    uv run pytest -m live

The point is schema canary, not load testing — it checks that the
CKAN package metadata still names a datastore-active resource and that
`datastore_search` delivers at least one well-formed record with the
fields the mapper expects.
"""

from __future__ import annotations

import pytest

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction
from civix.domains.business_licences.adapters.sources.ca.toronto import (
    TorontoBusinessLicencesAdapter,
)
from civix.infra.http import default_http_client

pytestmark = pytest.mark.live


async def test_live_smoke_current_dataset_returns_records() -> None:
    async with default_http_client() as client:
        adapter = TorontoBusinessLicencesAdapter(
            dataset_id=DatasetId("municipal-licensing-and-standards-business-licences-and-permits"),
            jurisdiction=Jurisdiction(country="CA", region="ON", locality="Toronto"),
            client=client,
            page_size=5,
        )
        result = await adapter.fetch()
        first_record = await anext(aiter(result.records))

    assert result.snapshot.record_count > 0
    assert first_record.snapshot_id == result.snapshot.snapshot_id
    assert "Licence No." in first_record.raw_data
    assert "Category" in first_record.raw_data
    assert "_id" in first_record.raw_data
