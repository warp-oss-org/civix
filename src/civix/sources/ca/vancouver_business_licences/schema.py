"""Source-field schema constants shared across the Vancouver adapter and mapper.

Lives outside both `adapter.py` and `mapper.py` so neither has to depend
on the other for facts about the source's field shape. The mapper reads
`ADAPTER_CONSUMED_FIELDS` to compute `unmapped_source_fields` accurately
without importing the adapter class.
"""

from __future__ import annotations

from typing import Final

ADAPTER_CONSUMED_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "licencersn",  # → record.source_record_id
        "extractdate",  # → record.source_updated_at
    }
)
"""Source fields the adapter surfaces via `RawRecord` metadata rather
than leaving in `raw_data` for the mapper to interpret.

The mapper imports this set when building `unmapped_source_fields` to
avoid double-listing fields the adapter already accounted for.
"""
