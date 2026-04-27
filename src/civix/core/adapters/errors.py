"""Source-fetch errors.

A single base exception (`FetchError`) wraps any underlying transport,
parse, or protocol failure that occurs while an adapter is fetching.
The original exception is preserved via `__cause__` (raise...from),
so callers needing granular handling can inspect it; callers needing
stable types catch `FetchError` and rely on its civix-specific context
(source_id, dataset_id, operation).

Subtypes are intentionally not provided. Add them only when a real
consumer demonstrates a need to distinguish failure modes — premature
hierarchy is a maintenance burden without payoff.
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
