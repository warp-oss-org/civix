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
- Do not introduce unrelated refactors or churn. Keep changes scoped to the behavior being added or fixed.
- Verify current external facts before treating them as current, stable, or recommended, especially open-data portals, API schemas, package tooling, and standards.

## Architecture Boundaries

The intended boundaries are:

- `core`: shared identity, observation, quality, provenance, mapping, spatial, temporal, drift, validation, pipeline, and export primitives. Pure typed contracts and pure-function logic.
- `core/adapters`: the source-fetching infrastructure layer — `SourceAdapter` Protocol, `FetchResult`, HTTP client factory, `FetchError`. Anything that talks to external civic data portals lives behind this contract.
- `core/pipeline`: orchestration that composes an adapter and a mapper into a single runnable transform. The only place where fetching and mapping meet.
- `core/export`: source-independent artifact and serialization contracts.
- `domains`: domain models and domain-specific normalization rules such as business licences or procurement.
- `sources`: per-jurisdiction implementations of `SourceAdapter` and per-source mappers (e.g. `sources/ca/vancouver_business_licences/`).
- external consumers: command-line tools, notebooks, dashboards, and applications that decide where artifacts are written or stored.

Respect those boundaries:

- Source adapter implementations should not silently normalize records beyond source acquisition and snapshot metadata.
- Mappers should not fetch from the network.
- Exporters and serializers should not reinterpret civic semantics.
- Shared domain models should not depend on a single city portal's naming conventions.
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
- keep tests deterministic, readable, and cheap enough to run during normal development
- separate Arrange, Act, and Assert with one blank line per test, even for short tests
- test through the public interface; do not promote `_`-prefixed helpers or suppress the resulting type/lint warnings (`# type: ignore`, `# pyright: ignore`, `# noqa`, `warnings.filterwarnings`)

## Comments

Comments are a last resort, not a band-aid for sloppy implementation. Before adding one, fix the code first: rename the function or variable, extract a helper, name a magic value as a constant, or shrink an oversized block. A comment is justified when the WHY is non-obvious — a hidden constraint, a deliberate decision, a workaround, or a source-quirk note. Do not write multi-line block comments that restate what the next few lines do, and do not add section-divider comments (`# ---- foo ----`) inside files that already use classes or functions for grouping.
