# Architecture

This is the contract for how Civix code is organized. The layout below
is enforced by `import-linter` in CI; conventions in
[`source-package-conventions.md`](source-package-conventions.md) and
[`testing-guidelines.md`](testing-guidelines.md) refine specific slices.

## Layers

```
        civix.core          (pure capabilities)
            ^
            |
   +--------+---------+
   |                  |
civix.domains.<x>   civix.infra
 (bounded contexts:   (cross-cutting I/O:
  models + their       http transport,
  source slices)       format exporters)
```

- `civix.core` — pure capability code: drift, identity, mapping, quality,
  validation, provenance, snapshots, taxonomy, spatial, temporal,
  pipeline, export, ports. No I/O, no source-specific knowledge, no
  domain-specific knowledge. Imports nothing from `domains` or `infra`.
- `civix.domains.<x>` — a bounded context. Contains `models/` (canonical
  types; may import `civix.core` only) and optionally `adapters/` (source
  slices and any other domain-specific boundary implementations; may
  import `civix.core` and `civix.infra`). The same `models <- adapters`
  rule repeats inside every domain.
- `civix.infra` — cross-cutting I/O only: `http.py` and `exporters/`.
  May import `civix.core`. Must not import any domain.

A domain package exists only when at least one source slice consumes it,
or an active plan in `plans/` is building toward that consumer (current
carve-out: `domains/transportation_safety/`, models only — no `adapters/`
yet).

## Vertical slices

Two slice families:

- **Source slices** at
  `civix.domains.<domain>.adapters.sources.<country>.<city>/` — one
  package per source (e.g. `business_licences/adapters/sources/ca/calgary`).
  Owns its `adapter.py`, `mapper.py`, `schema.py`, and `__init__.py`.
  Shape and rules:
  [`source-package-conventions.md`](source-package-conventions.md).
- **Exporter slices** at `civix.infra.exporters.<format>/` — one package
  per output format (`drift`, `json`, `parquet`, `validation`). Each
  owns a `writer.py`.

Slices in the same family **must not import each other**. Cities stay
isolated from cities; exporters from exporters. Shared logic lives one
level up in the domain or in `core/`, not in a sibling slice.

## `core/` shape

Each capability is its own package. Do not pile new responsibilities
into an existing one — add a sibling.

| Package | Purpose |
| --- | --- |
| `ports` | Boundary contracts that adapters satisfy: `SourceAdapter` Protocol, `FetchResult`, `FetchError`. |
| `drift` | Schema and taxonomy drift observation, analysis, and reports. |
| `export` | Format-agnostic export contracts. |
| `identity` | Stable identity primitives. |
| `mapping` | Field mapping primitives (`MappedField`, quality states). |
| `pipeline` | Pipeline orchestration: `run`, observers, results. |
| `provenance` | Provenance metadata for normalized records. |
| `quality` | Mapping quality primitives. |
| `snapshots` | Raw source-capture primitives (`SourceSnapshot`, `RawRecord`). |
| `spatial` | Spatial value types. |
| `taxonomy` | Versioned taxonomy reference primitives. |
| `temporal` | Injected `Clock`; deterministic time for tests. |
| `validation` | `validate_snapshot` — single entry point for V1 pass/fail. |

## `domains/<x>/` shape

A domain package contains:

- **`models/`** — canonical types shared across that domain's sources.
  May import `civix.core`. Must not import its own `adapters/` or
  `civix.infra`.
- **`adapters/`** (optional, present once ≥1 source slice exists) — the
  domain's source slices and any other domain-specific boundary
  implementations. May import `civix.core`, `civix.infra`, and the
  domain's own `models/`.
- **`__init__.py`** — re-exports the canonical model(s). Source slices
  are imported from their own package paths, not eagerly re-exported.

## Module shape inside a capability or domain

Each `core/<capability>/` and `domains/<domain>/` package follows the
same internal split:

- **`models/`** — pure data types (pydantic models, dataclasses, enums,
  Protocols). One type per file. Imported by sibling files in the
  package and by other packages.
- **Flat `.py` files at the package root** — behavior: functions,
  observers, runners, validators, exception classes. May import from
  the local `models/` or from any other capability.

A type moves into `models/` when it is imported by more than one
sibling file, or when its definition would otherwise dominate a
behavior-oriented module. Small modules that mix one type with the
single function that operates on it (`temporal/__init__.py`,
`pipeline/runner.py`) stay flat — splitting them adds overhead without
clarifying anything.

Examples:

- `core/drift/` — `models/spec.py`, `models/report.py` hold the data
  contracts; `analysis.py`, `observation.py`, `observers.py` hold the
  behavior that produces and consumes them.
- `core/validation/` — `models/report.py` holds the report type;
  `validator.py` is the single entry point function.
- `core/ports/` — `models/adapter.py` holds the `SourceAdapter`
  Protocol; `errors.py` holds `FetchError` (exceptions stay flat —
  they're control-flow types, not data). The package is named after
  the hexagonal "port" role; the concrete adapters that implement it
  live under `domains/<x>/adapters/sources/`.

## Tests mirror src

`tests/` reproduces the `src/civix/` tree exactly. A change to
`src/civix/domains/business_licences/adapters/sources/ca/calgary/mapper.py`
has its test at
`tests/domains/business_licences/adapters/sources/ca/calgary/test_mapper.py`.
Test conventions: [`testing-guidelines.md`](testing-guidelines.md).

## Enforcement

Five `import-linter` contracts in `pyproject.toml` lock the rules above:

1. **core stays pure** — `civix.core` may not import `civix.domains` or
   `civix.infra`.
2. **top-level infra stays cross-cutting** — `civix.infra` may not
   import any domain. (Domain-owned `adapters/` may still import
   `civix.infra`.)
3. **domain models stay pure** — a domain's `models/` may not import
   its own `adapters/` or `civix.infra`.
4. **source slices stay independent** — no source package may import
   another source package.
5. **exporter slices stay independent** — no exporter package may
   import another exporter package.

Run locally:

```sh
uv run lint-imports
```

CI runs the same command alongside `pytest`, `ruff`, and `pyright`.
(If a CI workflow is not yet defined, add `lint-imports` when one is.)

## Adding new code

### A new source slice

1. Create
   `src/civix/domains/<domain>/adapters/sources/<country>/<city>/` with
   `adapter.py`, `mapper.py`, `schema.py`, `__init__.py`.
2. Follow [`source-package-conventions.md`](source-package-conventions.md)
   for shape, naming, and boundaries.
3. Mirror tests at
   `tests/domains/<domain>/adapters/sources/<country>/<city>/`.
4. Add the slice's dotted path to the **source slices stay independent**
   contract in `pyproject.toml`.
5. Reference: any of the existing five —
   `domains/business_licences/adapters/sources/{ca/calgary,ca/edmonton,ca/toronto,ca/vancouver,us/nyc}`.

### A new exporter slice

1. Create `src/civix/infra/exporters/<format>/` with `writer.py` and
   `__init__.py`.
2. Mirror tests at `tests/infra/exporters/<format>/`.
3. Add the slice's dotted path to the **exporter slices stay
   independent** contract in `pyproject.toml`.
4. Reference: `infra/exporters/json/` or `infra/exporters/parquet/`.

### A new domain

1. Only add when a source slice is about to consume it. No empty
   scaffolding.
2. Create `src/civix/domains/<domain>/models/` with the canonical model
   files; re-export through `domains/<domain>/__init__.py`.
3. Add `domains/<domain>/adapters/sources/...` when (not before) the first
   real source slice lands.
4. Extend the **domain models stay pure** contract in `pyproject.toml`
   so it covers the new domain's `models/`.
5. Reference: `domains/business_licences/` (live, five source slices).

### A new `core/` capability

1. Create `src/civix/core/<capability>/` as a new sibling. Do not bolt
   onto `mapping`, `quality`, `validation`, or any existing package
   unless the new code is genuinely a refinement of that capability.
2. Keep it pure: no I/O, no source-specific knowledge.
3. Reference: `core/drift/` for a capability with observation, analysis,
   and report submodules.
