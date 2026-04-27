# Civix

Open-source civic data normalization library written in Python. Civix
turns messy public datasets into reproducible, inspectable artifacts:
raw snapshots, typed normalized records, mapping reports with explicit
quality states (direct / standardized / derived / inferred / unmapped /
conflicted / redacted / not-provided), and provenance metadata that
traces every value back to its source.

The current release is pre-1.0. Public contracts are usable but may
shift; the project's purpose, layering, and engineering rules are
documented in [`AGENTS.md`](AGENTS.md) and [`plans/core-idea.md`](plans/core-idea.md).

## What works today

- Typed primitives: identity, snapshots, quality, provenance,
  mapping, spatial, temporal, adapters, pipeline.
- Domain model: `BusinessLicence`.
- Source adapter + mapper for the Vancouver Open Data Portal's
  business-licences datasets, end-to-end via `civix.core.pipeline.run`.
- JSON snapshot exporter for normalized records, mapping reports,
  schemas, and manifests.
- Live opt-in test against the real Vancouver portal.

```python
import asyncio
from civix.core.identity import DatasetId, Jurisdiction
from civix.core.pipeline import run
from civix.infra.http import default_http_client
from civix.infra.sources.ca.vancouver_business_licences import (
    VancouverBusinessLicencesAdapter,
    VancouverBusinessLicencesMapper,
)


async def main() -> None:
    async with default_http_client() as client:
        adapter = VancouverBusinessLicencesAdapter(
            dataset_id=DatasetId("business-licences"),
            jurisdiction=Jurisdiction(country="CA", region="BC", locality="Vancouver"),
            client=client,
        )
        result = await run(adapter, VancouverBusinessLicencesMapper())
        async for pair in result.records:
            print(pair.mapped.record.business_name.value)


asyncio.run(main())
```

## Not yet implemented

Drift detection, validation, Parquet export, and additional sources
(Toronto, NYC, etc.) are next on the roadmap. See
[`plans/core-idea.md`](plans/core-idea.md) for the full build order.

## Tooling

- Python 3.12, [uv](https://github.com/astral-sh/uv) for env + deps
- Pydantic v2, httpx (async)
- Ruff (lint + format), Pyright (strict type-check), pytest (+ pytest-asyncio, respx)

## Commands

```bash
uv sync --dev
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest                # default suite (excludes live)
uv run pytest -m live        # opt-in: hits real civic APIs
```

## Contributing

Read [`AGENTS.md`](AGENTS.md) and [`docs/testing-guidelines.md`](docs/testing-guidelines.md)
before making non-trivial changes. Architecture boundaries, data and
provenance rules, and testing conventions are enforced.
