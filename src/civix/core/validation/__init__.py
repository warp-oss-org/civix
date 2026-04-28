"""Snapshot validation primitives.

`validate_snapshot` is the single entry point. Call it once with a
manifest plus whatever drift reports you ran; it returns a
`ValidationReport` whose `outcome` is the V1 pass/fail decision.
"""

from civix.core.validation.models import (
    ValidationFinding,
    ValidationFindingSource,
    ValidationOutcome,
    ValidationReport,
)
from civix.core.validation.validator import validate_snapshot

__all__ = [
    "ValidationFinding",
    "ValidationFindingSource",
    "ValidationOutcome",
    "ValidationReport",
    "validate_snapshot",
]
