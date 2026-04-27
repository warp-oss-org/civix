"""Source-fetch errors.

`FetchError` is the single exception raised by adapter implementations
for transport, parse, or protocol failures during a fetch. The original
exception is preserved via `__cause__` (raise...from) for callers who
want granular handling; callers who only need stable types can catch
`FetchError` and read its `source_id`, `dataset_id`, and `operation`
context fields.

Subtypes are not provided until a concrete consumer needs them.
"""

from __future__ import annotations

from civix.core.identity import DatasetId, SourceId


class FetchError(Exception):
    """An error encountered while an adapter was fetching from a source.

    Attributes:
        source_id: The adapter's source identifier.
        dataset_id: The dataset being fetched when the error occurred.
        operation: A short label for what the adapter was doing
            (e.g. "count", "stream-records"). Useful in logs.
    """

    def __init__(
        self,
        message: str,
        *,
        source_id: SourceId,
        dataset_id: DatasetId,
        operation: str,
    ) -> None:
        super().__init__(message)
        self.source_id = source_id
        self.dataset_id = dataset_id
        self.operation = operation

    def __str__(self) -> str:
        base = super().__str__()

        return (
            f"{base} "
            f"[source={self.source_id!r} dataset={self.dataset_id!r} "
            f"operation={self.operation!r}]"
        )
