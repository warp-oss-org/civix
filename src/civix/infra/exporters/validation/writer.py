"""Write a `ValidationReport` to a sibling `validation.json` artifact.

Validation is orthogonal to snapshot content and to drift: a snapshot
can be exported without ever being validated, and a validation report
can be written without touching the records file. This writer is
therefore standalone and does not mutate the snapshot's `ExportManifest`.

Format on disk: the JSON form of `ValidationReport`. Returns an
`ExportedFile` describing the artifact so the caller can fold it into
a manifest if it wants to.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

from civix.core.export.models.manifest import ExportedFile
from civix.core.validation.models.report import ValidationReport

_VALIDATION_FILE = "validation.json"


def write_validation(*, snapshot_dir: Path, report: ValidationReport) -> ExportedFile:
    """Write `validation.json` into `snapshot_dir` and return its file entry."""
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    body = report.model_dump_json(indent=2).encode("utf-8")
    path = snapshot_dir / _VALIDATION_FILE
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(body)
    tmp.rename(path)

    return ExportedFile(
        filename=_VALIDATION_FILE,
        sha256=hashlib.sha256(body).hexdigest(),
        byte_count=len(body),
    )
