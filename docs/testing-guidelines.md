# Testing Guidelines

This document supplements `AGENTS.md`. Use it to choose the right test layer, write high-signal assertions, and avoid tests that look busy without increasing confidence. For where tests live, see [`architecture.md`](architecture.md) — `tests/` mirrors `src/civix/` exactly.

## Goals

Tests in this repository should:

- verify observable behavior and persisted data contracts
- catch regressions at the cheapest meaningful layer
- stay deterministic, readable, and easy to refactor
- protect raw preservation, mapping quality, provenance, drift reports, and exports

High test count is not the goal. Confidence is.

## Test Layers

Use the lowest layer that can catch the failure meaningfully:

- **Unit tests**: pure mapping functions, taxonomy normalization, status normalization, drift classification, schema/report builders, provenance helpers, and deterministic exporters.
- **Integration tests**: source pipeline flows, adapter-to-mapper orchestration, CLI commands, filesystem output layout, JSON/Parquet export round trips, and validation behavior across multiple modules.
- **Opt-in external tests**: live civic API checks, portal availability checks, or smoke tests against real remote datasets. These must not run in the default test suite.

When behavior spans fetching, mapping, reporting, and exporting, prefer an integration test with fixture-backed input rather than several isolated unit tests that duplicate implementation details.

## Data Fixtures

- Use small representative fixtures for civic datasets.
- Include source quirks that matter: missing fields, redacted values, renamed fields, unknown categories, malformed dates, duplicate identifiers, and changed field types.
- Keep raw fixture shape close to the source API response.
- Do not edit fixtures casually. A fixture change should reflect an intentional contract change or a newly discovered source behavior.
- Prefer explicit fixture names that describe the civic scenario, not the implementation branch.

## Network And Filesystem

- Default tests must not call live civic APIs.
- Mock or fake HTTP at the adapter boundary, not inside mapping code.
- Use temporary directories for files written during tests.
- Assert written artifact names, formats, and key contents when testing pipeline output.
- Do not depend on local absolute paths, developer machines, current dates, or network availability.

## CLI Tests

CLI tests should assert the user-visible command contract:

- exit code
- stdout and stderr
- created artifacts
- behavior for invalid input, validation failures, and drift detection

For the current `argparse` scaffold, subprocess tests are acceptable. If the CLI later moves to Click, prefer Click's test runner for command-level tests while keeping end-to-end subprocess smoke coverage for installed-module behavior.

## Test Structure

Within each test, separate the Arrange, Act, and Assert steps with one
blank line so the three phases are visually distinct:

```python
def test_redacted_business_name(self) -> None:
    raw = _raw(businessname="REDACTED")

    licence = mapper(RawRecord(raw_data=raw, ...), snapshot).record

    assert licence.business_name.value is None
    assert licence.business_name.quality is FieldQuality.REDACTED
```

When arrange and act collapse onto one line (e.g. `result = pure_fn(x)`),
still leave a blank line before the assertions. Apply this even to
short tests; the readability win is consistent.

Do not add section-divider comments (`# ---- foo ----`) between test
classes. The class names already group related cases, and a blank line
between classes is enough.

## Privates And Suppressions

Test through the public interface of the module under test. Do not
promote a `_`-prefixed helper to public solely so a test can import it,
and do not silence the resulting warning (`# type: ignore`,
`# pyright: ignore`, `# noqa`, `warnings.filterwarnings`, ...). If a
behavior is hard to observe through the public surface, treat that as a
design signal rather than a license to expose internals.

## Assertions

Assert on public outcomes:

- normalized record values
- field quality states
- unmapped field lists
- provenance fields
- drift severity and changed fields
- exported schema and data files
- CLI exit behavior

Avoid assertions on:

- private helper call order
- incidental dict ordering unless order is part of the contract
- internal class structure when a public report or artifact proves the behavior
- broad snapshots of entire reports when targeted assertions would be clearer

## Time, IDs, And Determinism

- Inject clocks where observed or fetched times matter.
- Use stable fixture IDs and hashes in tests.
- Avoid sleeping or polling fixed timeouts.
- If randomness is introduced, inject a random source or seed it explicitly.
- Treat timezone handling as part of the data contract when parsing civic dates and timestamps.

## Mocking Rules

Mock only true boundaries:

- HTTP clients
- clocks
- filesystem roots
- environment variables
- external services
- optional third-party library failures

Avoid mocking internal modules by default. If the product behavior depends on the interaction between internal modules, test that interaction directly with fixtures.

## Export And Schema Tests

Export tests should verify:

- raw records remain available
- normalized records round-trip through the supported format
- JSON Schema reflects the documented contract
- Decimal, date, datetime, and nullable fields serialize intentionally
- Parquet exports preserve column names and expected primitive types

When an export format is optional or dependency-backed, test both the happy path and the failure message for missing dependencies.

## Drift And Mapping Tests

Drift and mapping tests should cover:

- new source fields
- missing expected fields
- source type changes
- unknown taxonomy values
- retired taxonomy values
- redacted and not-provided values
- conflicted source fields
- inferred or derived normalized values

Validation should fail loudly for breaking drift while still preserving raw fetched records when fetch succeeded.

## Running Tests

Use the project tools:

```bash
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run pyright
```

Run focused tests while developing, then run the relevant full checks before finishing a behavior change.
