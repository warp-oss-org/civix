"""Schema and taxonomy drift primitives.

Drift detection is split into two phases:

- **Observation** (`observation.py`): pure passes over raw records that
  tally what was actually present.
- **Analysis** (`analysis.py`): pure comparisons of observations against
  explicit specs (`models/spec.py`), producing reports (`models/report.py`).

The library itself is stateless. Specs live in source under each
adapter's package and are versioned via PR.
"""

from civix.core.drift.analysis import (
    analyze_schema,
    analyze_taxonomy,
    compare_schema,
    compare_taxonomy,
)
from civix.core.drift.models.report import (
    DriftSeverity,
    SchemaDriftFinding,
    SchemaDriftKind,
    SchemaDriftReport,
    TaxonomyDriftFinding,
    TaxonomyDriftKind,
    TaxonomyDriftReport,
)
from civix.core.drift.models.spec import (
    JsonFieldKind,
    SchemaFieldSpec,
    SourceSchemaSpec,
    TaxonomyNormalization,
    TaxonomySpec,
)
from civix.core.drift.observation import (
    ObservedField,
    ObservedSchema,
    ObservedTaxonomy,
    ObservedTaxonomyValue,
    observe_schema,
    observe_taxonomy,
)
from civix.core.drift.observers import (
    DriftObserver,
    SchemaObserver,
    TaxonomyObserver,
)

__all__ = [
    "DriftObserver",
    "DriftSeverity",
    "JsonFieldKind",
    "ObservedField",
    "ObservedSchema",
    "ObservedTaxonomy",
    "ObservedTaxonomyValue",
    "SchemaDriftFinding",
    "SchemaDriftKind",
    "SchemaDriftReport",
    "SchemaFieldSpec",
    "SchemaObserver",
    "SourceSchemaSpec",
    "TaxonomyDriftFinding",
    "TaxonomyDriftKind",
    "TaxonomyDriftReport",
    "TaxonomyNormalization",
    "TaxonomyObserver",
    "TaxonomySpec",
    "analyze_schema",
    "analyze_taxonomy",
    "compare_schema",
    "compare_taxonomy",
    "observe_schema",
    "observe_taxonomy",
]
