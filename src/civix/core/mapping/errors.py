"""Mapping exceptions."""

from __future__ import annotations

from civix.core.provenance.models.provenance import MapperVersion


class MappingError(Exception):
    """Failure to map one raw source record into a normalized record."""

    def __init__(
        self,
        reason: str,
        *,
        mapper: MapperVersion,
        source_record_id: str | None,
        source_fields: tuple[str, ...],
    ) -> None:
        super().__init__(reason)
        self.reason = reason
        self.mapper = mapper
        self.source_record_id = source_record_id
        self.source_fields = source_fields
