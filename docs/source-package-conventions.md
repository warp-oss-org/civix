# Source Package Conventions

See [`architecture.md`](architecture.md) for the overall layering and the
slice independence rules these packages live inside.

Source packages are source-system boundary code. They fetch public
records, preserve raw source shape, map records into domain models, and
pin drift expectations for that source. They live inside their domain:

```text
src/civix/domains/<domain>/adapters/sources/<country>/<city>/
```

Examples:

```text
src/civix/domains/business_licences/adapters/sources/ca/calgary/
src/civix/domains/business_licences/adapters/sources/us/nyc/
```

## Package Shape

Each source package should usually contain:

- `adapter.py`: fetches raw source records and builds `SourceSnapshot`
  and `RawRecord` values.
- `mapper.py`: maps raw records into a source-agnostic domain model.
- `schema.py`: pins raw source schema expectations and bounded taxonomy
  drift specs.
- `__init__.py`: re-exports the public adapter, mapper, schema, and
  taxonomy constants. It must not define adapter logic, mapper logic,
  schemas, models, validators, or constants directly.

Tests mirror the package path under
`tests/domains/<domain>/adapters/sources/...`.

## Source Eligibility

A production source slice must be backed by a confirmed, stable,
machine-readable source contract. Before adding `adapter.py`,
`mapper.py`, or `schema.py`, record the source facts in the plan or PR:

- public dataset page or official documentation URL
- concrete machine endpoint or downloadable resource URL used by the
  adapter
- source update cadence or pinned release/version, when available
- licence or reuse terms, when available
- row grain and stable source-record identifier
- reason the endpoint is stable enough to support reproducible fetches

Do not land a supported source slice when the only available input is a
hand-staged extract, browser-only map, HTML landing page, undocumented
private backing call, or guessed JSON shape. Those can be used for
domain-model pressure tests or archived research notes, but not as
production adapters.

If a source has real public data but not in the shape initially
imagined, either implement the adapter against the real source contract
or defer the slice. Do not create fixture-shaped adapters that require
callers to supply an arbitrary extract URL unless that is explicitly
the source's documented distribution contract.

## Standard Pipeline Contract

The default public source product is one `SourceAdapter` plus one
`Mapper[T]` that works with:

```python
await civix.core.pipeline.run(adapter, mapper)
```

Public mappers in source packages should satisfy:

```python
def __call__(self, record: RawRecord, snapshot: SourceSnapshot) -> MapResult[T]: ...
```

Grouped, linked, or multi-table orchestration is not the default source
product. Only add it when the consumer contract is explicit and tested
as a separate capability. Do not export grouped/linker helpers as the
main mapper for a source slice, because they cannot be consumed by the
standard pipeline runner.

For multi-table sources, prefer exposing each table as a standard
adapter + mapper pair. If one normalized record truly requires joining
multiple raw tables, choose one of these deliberately:

- keep the joined product out of the public source surface until a
  first-class orchestration contract exists
- map the primary table alone and mark fields that require missing
  tables as `UNMAPPED` or `NOT_PROVIDED`
- add a separately named composite runner with its own tests and docs

Do not hide cross-table joins inside a mapper that claims to implement
the standard `Mapper` protocol.

## Naming

The domain is in the path, so city directories drop the redundant
domain suffix. The current five business-licence slices are:

- `domains/business_licences/adapters/sources/ca/calgary`
- `domains/business_licences/adapters/sources/ca/edmonton`
- `domains/business_licences/adapters/sources/ca/toronto`
- `domains/business_licences/adapters/sources/ca/vancouver`
- `domains/business_licences/adapters/sources/us/nyc`

Prefer the repository's domain spelling in package and class names. For
example, use `business_licences` even when a source labels its dataset
"licenses."

## Boundaries

Adapters may:

- perform network calls and other source-acquisition side effects
- build snapshot metadata
- surface stable source identifiers as `RawRecord.source_record_id`
- remove transport-only fields, such as Socrata computed region fields

Adapters must not normalize civic semantics. Preserve source fields in
`raw_data` unless a field is strictly a transport artifact.

Mappers:

- must be pure functions of `RawRecord` and `SourceSnapshot`
- must not fetch from the network or read external files
- must build normalized domain records with provenance
- must report source fields they saw but did not consume
- should distinguish missing, redacted, unmapped, inferred, derived, and
  standardized values through `MappedField.quality`

Schemas:

- define raw source field expectations
- define taxonomy drift specs only for bounded vocabularies
- should not guess open vocabularies just to have a taxonomy
- should version persisted schema and taxonomy expectations

## Tests

Each source should usually include:

- adapter unit tests for source protocol behavior and failure messages
- mapper unit tests for every normalized field and quality state
- fixture-backed adapter integration tests
- fixture-backed mapper or pipeline integration tests
- at least one fixture-backed `core.pipeline.run(adapter, mapper)` test
  for each public adapter + mapper pair
- drift integration tests for schema and bounded taxonomies
- optional live smoke tests marked out of the default suite

Fixtures should be small and source-shaped. Include source quirks that
matter, such as missing fields, unknown statuses, malformed dates,
redacted values, invalid coordinates, and intentionally unmapped fields.

Fixtures prove parser and mapper behavior; they do not prove a source
exists. A new adapter also needs source-contract evidence from the
eligibility checklist above.
