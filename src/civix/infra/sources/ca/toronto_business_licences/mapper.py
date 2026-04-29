"""Toronto business-licences mapper.

Transforms a Toronto Open Data Portal row into a normalized
`BusinessLicence`. Pure: no I/O, deterministic given a stable mapper
version.

Per-field `MappedField.quality` is set so consumers can distinguish
direct, standardized, derived, inferred, redacted, missing, and unmapped
values. Toronto's source schema does not carry an explicit status, an
expiry date, a coordinate, or a neighbourhood, so those domain fields
land as UNMAPPED rather than NOT_PROVIDED — the source is mute on the
question, not just blank on a known column.

Status is derived from `Cancel Date`: a non-null value means the licence
was cancelled; a null leaves status NOT_PROVIDED. Toronto does not
publish "active vs expired" in this dataset, so guessing further would
be fiction.

Country is inferred to "CA" because the dataset is municipal Toronto
and address Lines 1–3 follow Canadian conventions; the address quality
stays DERIVED to acknowledge that province/locality were parsed out of
Line 2 rather than copied verbatim.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any, Final

from pydantic import ValidationError

from civix.core.identity import MapperId
from civix.core.mapping import MappingReport, MapResult
from civix.core.provenance import MapperVersion, ProvenanceRef
from civix.core.quality import FieldQuality, MappedField
from civix.core.snapshots import RawRecord, SourceSnapshot
from civix.core.spatial import Address, Coordinate
from civix.domains.business_licences import (
    BusinessLicence,
    CategoryRef,
    LicenceStatus,
)
from civix.infra.sources.ca.toronto_business_licences.schema import (
    ADAPTER_CONSUMED_FIELDS,
)

MAPPER_ID: Final[MapperId] = MapperId("toronto-business-licences")
MAPPER_VERSION: Final[str] = "0.1.0"

_TAXONOMY_ID: Final[str] = "toronto-business-categories"
_TAXONOMY_VERSION: Final[str] = "2026-04-28"
_INFERRED_COUNTRY: Final[str] = "CA"

_ADDRESS_SOURCE_FIELDS: Final[tuple[str, ...]] = (
    "Licence Address Line 1",
    "Licence Address Line 2",
    "Licence Address Line 3",
)


@dataclass(frozen=True, slots=True)
class TorontoBusinessLicencesMapper:
    """Maps Toronto Open Data business-licence rows to `BusinessLicence`.

    Stateless. Construct once and reuse across records.
    """

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=MAPPER_ID, version=MAPPER_VERSION)

    def __call__(self, record: RawRecord, snapshot: SourceSnapshot) -> MapResult[BusinessLicence]:
        raw = record.raw_data
        provenance = self._build_provenance(record=record, snapshot=snapshot)

        licence = BusinessLicence(
            provenance=provenance,
            business_name=_map_business_name(raw),
            licence_number=_map_licence_number(raw),
            status=_map_status(raw),
            category=_map_category(raw),
            issued_at=_map_issued_at(raw),
            expires_at=_unmapped_date(),
            address=_map_address(raw),
            coordinate=_unmapped_coordinate(),
            neighbourhood=_unmapped_neighbourhood(),
        )
        report = MappingReport(
            unmapped_source_fields=_unmapped_source_fields(raw, licence),
        )

        return MapResult[BusinessLicence](record=licence, report=report)

    def _build_provenance(self, *, record: RawRecord, snapshot: SourceSnapshot) -> ProvenanceRef:
        return ProvenanceRef(
            snapshot_id=snapshot.snapshot_id,
            source_id=snapshot.source_id,
            dataset_id=snapshot.dataset_id,
            jurisdiction=snapshot.jurisdiction,
            fetched_at=snapshot.fetched_at,
            mapper=self.version,
            source_record_id=record.source_record_id,
        )


def _map_business_name(raw: Mapping[str, Any]) -> MappedField[str]:
    value = _str_or_none(raw.get("Operating Name"))

    if value is None:
        return MappedField[str](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("Operating Name",),
        )

    return MappedField[str](
        value=value,
        quality=FieldQuality.DIRECT,
        source_fields=("Operating Name",),
    )


def _map_licence_number(raw: Mapping[str, Any]) -> MappedField[str]:
    value = _str_or_none(raw.get("Licence No."))

    if value is None:
        return MappedField[str](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("Licence No.",),
        )

    return MappedField[str](
        value=value,
        quality=FieldQuality.DIRECT,
        source_fields=("Licence No.",),
    )


def _map_status(raw: Mapping[str, Any]) -> MappedField[LicenceStatus]:
    value = _str_or_none(raw.get("Cancel Date"))

    if value is None:
        return MappedField[LicenceStatus](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("Cancel Date",),
        )

    return MappedField[LicenceStatus](
        value=LicenceStatus.CANCELLED,
        quality=FieldQuality.DERIVED,
        source_fields=("Cancel Date",),
    )


def _map_category(raw: Mapping[str, Any]) -> MappedField[CategoryRef]:
    label = _str_or_none(raw.get("Category"))

    if label is None:
        return MappedField[CategoryRef](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("Category",),
        )

    return MappedField[CategoryRef](
        value=CategoryRef(
            code=_slugify(label),
            label=label,
            taxonomy_id=_TAXONOMY_ID,
            taxonomy_version=_TAXONOMY_VERSION,
        ),
        quality=FieldQuality.DERIVED,
        source_fields=("Category",),
    )


def _map_issued_at(raw: Mapping[str, Any]) -> MappedField[date]:
    value = raw.get("Issued")

    if not isinstance(value, str) or not value:
        return MappedField[date](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("Issued",),
        )

    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        return MappedField[date](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("Issued",),
        )

    return MappedField[date](
        value=parsed,
        quality=FieldQuality.STANDARDIZED,
        source_fields=("Issued",),
    )


def _map_address(raw: Mapping[str, Any]) -> MappedField[Address]:
    line1 = _str_or_none(raw.get("Licence Address Line 1"))
    line2 = _str_or_none(raw.get("Licence Address Line 2"))
    line3 = _str_or_none(raw.get("Licence Address Line 3"))

    locality, region = _parse_locality_region(line2)

    try:
        address = Address(
            country=_INFERRED_COUNTRY,
            region=region,
            locality=locality,
            street=line1,
            postal_code=line3,
        )
    except ValidationError:
        return MappedField[Address](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=_ADDRESS_SOURCE_FIELDS,
        )

    return MappedField[Address](
        value=address,
        quality=FieldQuality.DERIVED,
        source_fields=_ADDRESS_SOURCE_FIELDS,
    )


def _unmapped_date() -> MappedField[date]:
    return MappedField[date](value=None, quality=FieldQuality.UNMAPPED, source_fields=())


def _unmapped_coordinate() -> MappedField[Coordinate]:
    return MappedField[Coordinate](value=None, quality=FieldQuality.UNMAPPED, source_fields=())


def _unmapped_neighbourhood() -> MappedField[str]:
    return MappedField[str](value=None, quality=FieldQuality.UNMAPPED, source_fields=())


def _unmapped_source_fields(raw: Mapping[str, Any], licence: BusinessLicence) -> tuple[str, ...]:
    """Source fields present in raw_data that the mapper did not consume.

    Built dynamically from the `MappedField.source_fields` of every
    domain field on the licence, plus the adapter's own consumed set.
    Self-correcting: if a future domain field starts consuming a new
    source field, no separate constant needs updating.
    """
    consumed: set[str] = set()

    for field_name in licence.__class__.model_fields:
        attr = getattr(licence, field_name)

        if isinstance(attr, MappedField):
            consumed.update(attr.source_fields)

    consumed |= ADAPTER_CONSUMED_FIELDS

    return tuple(sorted(name for name in raw if name not in consumed))


def _str_or_none(value: object) -> str | None:
    """Coerce a possibly-None value to a non-empty trimmed string, or None."""
    if value is None:
        return None

    s = str(value).strip()

    return s if s else None


def _parse_locality_region(line2: str | None) -> tuple[str | None, str | None]:
    """Split Toronto's `Licence Address Line 2` ("CITY, PROV") into parts.

    Returns `(locality, region)`. When the input is missing or does not
    contain a comma, both come back as None and the address still has
    line1/line3 available to reach DERIVED.
    """
    if line2 is None:
        return None, None

    parts = [p.strip() for p in line2.split(",")]

    if len(parts) < 2:
        return None, None

    locality = parts[0] or None
    region = parts[-1] or None

    return locality, region


def _slugify(text: str) -> str:
    """Lowercase, replace any non-alphanumeric run with a single hyphen."""
    s = re.sub(r"[^a-z0-9]+", "-", text.lower().strip())

    return s.strip("-")
