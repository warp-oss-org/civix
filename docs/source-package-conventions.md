# Source Package Conventions

Source packages are source-system boundary code. They fetch public
records, preserve raw source shape, map records into domain models, and
pin drift expectations for that source. They live under:

```text
src/civix/infra/sources/<country>/<dataset_name>/
```

Examples:

```text
src/civix/infra/sources/ca/calgary_business_licences/
src/civix/infra/sources/us/nyc_business_licences/
```

## Package Shape

Each source package should usually contain:

- `adapter.py`: fetches raw source records and builds `SourceSnapshot`
  and `RawRecord` values.
- `mapper.py`: maps raw records into a source-agnostic domain model.
- `schema.py`: pins raw source schema expectations and bounded taxonomy
  drift specs.
- `__init__.py`: exports the public adapter, mapper, schema, and
  taxonomy constants.

Tests mirror the package path under `tests/infra/sources/...`.

## Naming

Use source-specific package names that make the jurisdiction and domain
clear, such as:

- `calgary_business_licences`
- `edmonton_business_licences`
- `nyc_business_licences`
- `toronto_business_licences`
- `vancouver_business_licences`

Prefer the repository's domain spelling in package names. For example,
use `business_licences` even when a source labels its dataset
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
- drift integration tests for schema and bounded taxonomies
- optional live smoke tests marked out of the default suite

Fixtures should be small and source-shaped. Include source quirks that
matter, such as missing fields, unknown statuses, malformed dates,
redacted values, invalid coordinates, and intentionally unmapped fields.
