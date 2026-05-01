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

- Typed primitives: identity, snapshots, quality, provenance,
  mapping, spatial, temporal, adapters, pipeline.
- Domain model: `BusinessLicence`.
- Source adapter + mapper for the Vancouver Open Data Portal's
  business-licences datasets.
- JSON and Parquet snapshot exporters for normalized records, mapping
  reports, schemas, and manifests.
- Schema and taxonomy drift detection with sibling `drift.json` artifact.
- Snapshot validation with pass/fail outcome and sibling `validation.json`
  artifact.
- Live opt-in test against the real Vancouver portal.

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
