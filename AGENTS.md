# AGENTS.md

## Purpose

Civix is an open-source Python CLI and library for normalizing civic data into reproducible, inspectable artifacts.

The project is not a hosted dashboard first. The core product is a local engine that can fetch public datasets, preserve raw records, map them into domain models, detect drift, and produce useful artifacts for downstream tools.

All changes should preserve or improve:

- correctness
- reproducibility
- provenance quality
- maintainability
- testability
- contributor clarity

## Decision Priority

When tradeoffs exist, prioritize in this order:

1. Correctness
2. Reproducibility
3. Provenance and auditability
4. Maintainability
5. Performance
6. Delivery speed

## Core Engineering Rules

- Preserve raw civic records. Never normalize destructively or discard source fields without making them available in raw output or mapping reports.
- Keep fetching separate from normalization. Source adapters fetch and snapshot records; mappers transform raw records into domain models.
- Keep domain models separate from source-specific field names. Source quirks belong in adapters or mappers, not shared domain objects.
- Prefer explicit typed data models for public contracts, persisted metadata, reports, and domain records.
- Prefer pure functions for deterministic mapping, validation, normalization, drift detection, and taxonomy logic.
- Make side effects explicit. Network calls, filesystem writes, clocks, randomness, and environment access should be isolated at boundaries.
- Fix root causes, not symptoms. Avoid broad fallback behavior that hides broken mapping, bad data, or schema drift.
- Use intention-revealing names. Prefer clear structure over explanatory comments.
- Group related logic inside functions with blank lines when it improves scanability, especially in core logic and tests. Separate setup, transformation, validation branches, side-effect groups, and return/assert blocks when doing so lowers cognitive overhead. Adjacent `if`/`try`/loop blocks that enforce different rules should usually have a blank line between them; compact one-step guards can stay tight when a blank line would add noise. Inside `try` blocks, separate distinct phases such as network calls, status checks, JSON parsing, model construction, and filesystem writes with blank lines; keep genuinely single-step `try` blocks compact.
- Do not introduce unrelated refactors or churn. Keep changes scoped to the behavior being added or fixed.
- Verify current external facts before treating them as current, stable, or recommended, especially open-data portals, API schemas, package tooling, and standards.
- Do not land production source slices from fixture-only data, staged extracts, browser-only pages, or guessed endpoint shapes. Source packages must follow [`docs/source-package-conventions.md`](docs/source-package-conventions.md), including stable source-contract evidence and a standard adapter + mapper pipeline path unless an explicit composite contract exists.

## Architecture Boundaries

Full layout, slice rules, and enforcement: [`docs/architecture.md`](docs/architecture.md). Layering is machine-checked by `import-linter` (`uv run lint-imports`).

The codebase has three top-level layers under `src/civix/`:

- `core`: pure contracts and primitives. Identity, snapshots, quality, provenance, mapping, spatial, temporal, drift, pipeline orchestration, adapter Protocol, and export manifest contracts. No I/O, no third-party portal knowledge, no domain-specific vocabulary.
- `domains/<x>`: a bounded context. Contains `models/` (canonical types; source-agnostic — a domain model never references a particular portal's field names) and optionally `adapters/` (the domain's source slices and other boundary implementations). The same `models <- adapters` rule repeats inside every domain. A `domains/<domain>/` package exists only when at least one source slice consumes it.
- `infra`: cross-cutting I/O. Contains `http.py` (shared HTTP transport), `sources/<format>/` (cross-domain source-acquisition helpers — e.g. Socrata, CKAN, OpenFEMA — wrapped by domain source slices), and `exporters/<format>/` (format-specific writers). Domain-specific source adapters and mappers live under `domains/<x>/adapters/sources/`, not at the top level.

Within `domains/<x>/adapters/sources/<country>/<city>/`: per-source `SourceAdapter` and `Mapper` implementations, colocated. Adapters fetch and snapshot raw records; mappers translate raw records into the domain's canonical model.

Within top-level `infra/`:

- `infra/http.py`: shared transport helpers used by adapter implementations across domains.
- `infra/sources/<format>/`: cross-domain acquisition helpers for source technologies. Domain source slices wrap these; the helper itself stays free of source semantics.
- `infra/exporters/<format>/`: writers that emit a `PipelineResult` to a target medium (filesystem layout, downstream system, etc.).

External consumers — command-line tools, notebooks, dashboards, applications — sit outside the package and decide where artifacts are written or stored.

Respect those boundaries:

- Source adapter implementations should not silently normalize records beyond source acquisition and snapshot metadata.
- Mappers should not fetch from the network.
- Exporters and serializers should not reinterpret civic semantics.
- Shared domain models should not depend on a single portal's naming conventions.
- `core` must not import from `domains` or `infra`. A domain's `models/` must not import its own `adapters/` or top-level `infra`. Top-level `infra` must not import any domain. Imports flow toward `core`.
- Persistence policy belongs to consumers. Civix may provide artifact encoders and layouts, but it should not own a storage layer unless a concrete library use case requires it.

If a capability does not fit an existing boundary, extend the boundary deliberately instead of leaking behavior across layers.

## Data And Provenance Rules

- Every normalized record must be traceable to its source dataset, source record, fetch time, mapper version, and source fields used.
- Raw snapshots should be reproducible and content-addressable where practical.
- Mapping reports should distinguish direct, standardized, derived, inferred, unmapped, conflicted, redacted, and not-provided values when those states matter.
- Schema drift and taxonomy drift are different concerns. Do not collapse field/type changes with category/status vocabulary changes.
- Missing, redacted, withheld, and unmapped data are not equivalent. Preserve the distinction in models and reports.
- Normalized taxonomies must be versioned once they become part of persisted output.

## Python Project Standards

- Use Python 3.12 as declared in `pyproject.toml`.
- Use `uv` for dependency syncing and command execution.
- Use `pyproject.toml` as the source of truth for package metadata and tool configuration.
- Use Ruff for linting and formatting; do not hand-format against a different style.
- Use Pyright in strict mode for type checking. Type errors are part of the change, not a follow-up.
- Keep package code under `src/civix`.
- Keep tests under `tests`.
- Keep imports explicit. Avoid `import *`.
- Keep `__init__.py` lightweight; avoid import-time work.
- Avoid global mutable state. Prefer passing dependencies and configuration explicitly.
- Use the standard library when it is clear and sufficient; add dependencies only when they materially improve correctness, interoperability, or maintainability.

## Consumer Interface Standards

- The Python package should expose stable, typed pipeline primitives before broad convenience abstractions.
- A future CLI may be implemented outside the Python package, including in Go. It should consume the same public contracts instead of duplicating civic semantics.
- User-facing failures should explain the dataset, operation, and artifact involved.
- Machine-readable artifacts should be stable and documented.
- The Python SDK should remain a thin wrapper around pipeline primitives until repeated usage proves a higher-level abstraction is needed.

## Work Process

For non-trivial changes:

1. Understand the requested behavior and relevant data contracts.
2. Inspect existing code and docs before editing.
3. Identify the smallest correct change.
4. Implement within the existing architecture.
5. Add or update tests for changed behavior.
6. Run focused verification.
7. Self-review for hidden side effects, provenance gaps, and overbroad abstractions.

## Definition Of Done

A change is complete when:

- behavior is correct and reproducible
- raw data and provenance expectations are preserved
- architecture boundaries are respected
- tests cover changed behavior at the right layer
- tests are deterministic and self-contained
- generated or exported artifacts have clear contracts
- no hidden network dependency, broad fallback, or unrelated refactor was introduced

## Testing Guidance

Detailed testing guidance lives in `docs/testing-guidelines.md`. Follow that document in addition to this file.

At a minimum:

- test behavior and data contracts, not implementation details
- use fixtures instead of live civic APIs by default
- mock only true boundaries such as HTTP clients, clocks, filesystems, and third-party services
- include a fixture-backed `core.pipeline.run(adapter, mapper)` test for each public source product
- keep tests deterministic, readable, and cheap enough to run during normal development
- separate Arrange, Act, and Assert with one blank line per test, even for short tests
- test through the public interface; do not promote `_`-prefixed helpers or suppress the resulting type/lint warnings (`# type: ignore`, `# pyright: ignore`, `# noqa`, `warnings.filterwarnings`)

## Comments

Comments are a last resort, not a band-aid for sloppy implementation. Before adding one, fix the code first: rename the function or variable, extract a helper, name a magic value as a constant, or shrink an oversized block. A comment is justified when the WHY is non-obvious — a hidden constraint, a deliberate decision, a workaround, or a source-quirk note. Do not write multi-line block comments that restate what the next few lines do, and do not add section-divider comments (`# ---- foo ----`) inside files that already use classes or functions for grouping.
