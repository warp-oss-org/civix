"""Small parsing helpers for source mappers."""

from __future__ import annotations

import re

from civix.core.mapping.errors import MappingError
from civix.core.provenance.models.provenance import MapperVersion


def str_or_none(value: object) -> str | None:
    """Return a stripped string, or None for null/blank values."""
    if value is None:
        return None

    s = str(value).strip()

    return s if s else None


def int_or_none(value: object) -> int | None:
    """Parse a whole-number source value, or None when blank/invalid."""
    text = str_or_none(value)

    if text is None:
        return None

    try:
        return int(text)
    except ValueError:
        return None


def float_or_none(value: object) -> float | None:
    """Parse a numeric source value, or None when blank/invalid."""
    text = str_or_none(value)

    if text is None:
        return None

    try:
        return float(text)
    except ValueError:
        return None


def slugify(text: str) -> str:
    """Normalize a source label into a lowercase hyphenated code."""
    s = re.sub(r"[^a-z0-9]+", "-", text.lower().strip())

    return s.strip("-")


def require_text(
    value: object,
    *,
    field_name: str,
    mapper: MapperVersion,
    source_record_id: str | None,
) -> str:
    """Return nonblank text or raise a mapper-scoped error."""
    parsed = str_or_none(value)

    if parsed is not None:
        return parsed

    raise MappingError(
        f"missing required source field {field_name!r}",
        mapper=mapper,
        source_record_id=source_record_id,
        source_fields=(field_name,),
    )
