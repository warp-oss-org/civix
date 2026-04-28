"""Write drift reports to a sibling `drift.json` artifact.

Drift is orthogonal to snapshot content: a snapshot can be exported
without ever running drift, and a drift report can be written without
touching the records file. This writer is therefore standalone and does
not mutate the snapshot's `ExportManifest`.

Format on disk:

```json
{
  "schema": { ...SchemaDriftReport... },
  "taxonomy": { ...TaxonomyDriftReport... }
}
```

Either key may be absent if the corresponding observer was not
attached. Returns an `ExportedFile` describing the artifact so the
caller can fold it into a manifest if it wants to.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from civix.core.drift import SchemaDriftReport, TaxonomyDriftReport
from civix.core.export import ExportedFile

_DRIFT_FILE = "drift.json"


def write_drift(
    *,
    snapshot_dir: Path,
    schema: SchemaDriftReport | None = None,
    taxonomy: TaxonomyDriftReport | None = None,
) -> ExportedFile:
    """Write `drift.json` into `snapshot_dir` and return its file entry.

    Raises `ValueError` if both reports are `None` — an empty drift
    artifact would only confuse downstream readers about whether drift
    was actually checked.
    """
    if schema is None and taxonomy is None:
        raise ValueError("write_drift requires at least one of schema or taxonomy")

    snapshot_dir.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {}
    if schema is not None:
        payload["schema"] = schema.model_dump(mode="json")
    if taxonomy is not None:
        payload["taxonomy"] = taxonomy.model_dump(mode="json")

    body = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
    path = snapshot_dir / _DRIFT_FILE
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(body)
    tmp.rename(path)

    return ExportedFile(
        filename=_DRIFT_FILE,
        sha256=hashlib.sha256(body).hexdigest(),
        byte_count=len(body),
    )
