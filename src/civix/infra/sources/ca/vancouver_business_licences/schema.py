"""Source-field schema constants shared between the Vancouver adapter and mapper.

`ADAPTER_CONSUMED_FIELDS` lists source fields the adapter surfaces via
`RawRecord` metadata rather than leaving in `raw_data`. The mapper
imports it when building `unmapped_source_fields` so fields the adapter
already accounted for are not double-listed.

Lives in its own module so neither `adapter.py` nor `mapper.py` has to
import the other for facts about the source's field shape.
"""

from __future__ import annotations

from typing import Final

ADAPTER_CONSUMED_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "licencersn",  # → record.source_record_id
        "extractdate",  # → record.source_updated_at
    }
)
