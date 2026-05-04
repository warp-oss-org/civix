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
The consumer API should stay narrow: application code should use stable
facades for supported datasets and artifact outputs, while lower-level
adapter, mapper, and pipeline primitives remain implementation and
extension points.

## What works today

Engine primitives:

- Typed `core/` capabilities: identity, snapshots, quality, provenance,
  mapping, spatial, temporal, taxonomy, drift, validation, ports, and
  pipeline orchestration.
- JSON and Parquet exporters for normalized records, mapping reports,
  schemas, and manifests; sibling `drift.json` and `validation.json`
  artifacts.

Domains and source slices (each slice ships an `adapter.py`,
`mapper.py`, `schema.py`, and fixture-backed tests):

- `business_licences` — Calgary, Edmonton, Toronto, Vancouver, NYC.
- `transportation_safety` — Chicago crashes/people/vehicles, NYC
  crashes/persons/vehicles, Toronto KSI, GB STATS19, France BAAC.
- `mobility_observations` — Chicago Traffic Tracker (regions, segments),
  NYC bicycle/pedestrian counts, NYC traffic speeds, NYC traffic volume
  counts, Toronto bicycle counters, Toronto turning-movement counts,
  France TMJA, GB road-traffic counts.
- `hazard_risk` — FEMA NRI, FEMA NFHL, NRCan FSI, PS FIFRA, France
  Géorisques PPRN, GB BGS GeoSure, GB EA RoFRS.
- `hazard_mitigation` — FEMA HMA, Canada DMAF, GB FCERM.
- `building_energy_emissions` — NYC LL84, NYC LL97, Ontario EWRB.

Live opt-in smoke tests are wired for Calgary, Toronto, and Vancouver
business-licences slices today. Other slices are exercised through
fixture-backed adapter, mapper, and drift tests; live coverage will
broaden as the SDK consumer surface stabilizes.

## Tooling

- Python 3.12, [uv](https://github.com/astral-sh/uv) for env + deps
- Pydantic v2, httpx (async), pyarrow via the `parquet` optional extra
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

Read [`AGENTS.md`](AGENTS.md),
[`docs/architecture.md`](docs/architecture.md),
[`docs/testing-guidelines.md`](docs/testing-guidelines.md), and
[`docs/source-package-conventions.md`](docs/source-package-conventions.md)
before making non-trivial source changes. Architecture boundaries, data
and provenance rules, and testing conventions are enforced — layering is
checked by `import-linter` (`uv run lint-imports`).
