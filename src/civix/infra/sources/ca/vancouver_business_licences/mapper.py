"""Vancouver business-licences mapper.

Transforms a Vancouver Open Data Portal row into a normalized
`BusinessLicence`. Pure: no I/O, deterministic given a stable mapper
version.

Per-field `MappedField.quality` is set so consumers can distinguish
direct, standardized, derived, redacted, and missing values. Malformed
inputs (out-of-range coordinates, unparseable dates) degrade to
`NOT_PROVIDED` rather than failing the whole record; the original
values are still preserved in `RawRecord.raw_data`.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Final, cast
from zoneinfo import ZoneInfo

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
from civix.infra.sources.ca.vancouver_business_licences.schema import (
    ADAPTER_CONSUMED_FIELDS,
)

MAPPER_ID: Final[MapperId] = MapperId("vancouver-business-licences")
MAPPER_VERSION: Final[str] = "0.1.0"

_LOCAL_TZ: Final[ZoneInfo] = ZoneInfo("America/Vancouver")
_TAXONOMY_ID: Final[str] = "vancouver-business-types"
_TAXONOMY_VERSION: Final[str] = "2024-05-06"

_REDACTION_SENTINELS: Final[frozenset[str]] = frozenset({"REDACTED"})

_STATUS_MAP: Final[dict[str, LicenceStatus]] = {
    "issued": LicenceStatus.ACTIVE,
    "active": LicenceStatus.ACTIVE,
    "pending": LicenceStatus.PENDING,
    "inactive": LicenceStatus.INACTIVE,
    "expired": LicenceStatus.EXPIRED,
    "cancelled": LicenceStatus.CANCELLED,
    "gone out of business": LicenceStatus.INACTIVE,
}

_ADDRESS_SOURCE_FIELDS: Final[tuple[str, ...]] = (
    "country",
    "house",
    "postalcode",
    "province",
    "street",
    "unit",
    "unittype",
    "city",
)


@dataclass(frozen=True, slots=True)
class VancouverBusinessLicencesMapper:
    """Maps Vancouver Open Data business-licence rows to `BusinessLicence`.

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
            expires_at=_map_expires_at(raw),
            address=_map_address(raw),
            coordinate=_map_coordinate(raw),
            neighbourhood=_map_neighbourhood(raw),
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
    value = raw.get("businessname")

    if value is None or value == "":
        return MappedField[str](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("businessname",),
        )

    if isinstance(value, str) and value in _REDACTION_SENTINELS:
        return MappedField[str](
            value=None,
            quality=FieldQuality.REDACTED,
            source_fields=("businessname",),
        )

    return MappedField[str](
        value=str(value),
        quality=FieldQuality.DIRECT,
        source_fields=("businessname",),
    )


def _map_licence_number(raw: Mapping[str, Any]) -> MappedField[str]:
    value = raw.get("licencenumber")

    if not value:
        return MappedField[str](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("licencenumber",),
        )

    return MappedField[str](
        value=str(value),
        quality=FieldQuality.DIRECT,
        source_fields=("licencenumber",),
    )


def _map_status(raw: Mapping[str, Any]) -> MappedField[LicenceStatus]:
    value = raw.get("status")

    if value is None or value == "":
        return MappedField[LicenceStatus](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("status",),
        )

    if not isinstance(value, str):
        return MappedField[LicenceStatus](
            value=LicenceStatus.UNKNOWN,
            quality=FieldQuality.INFERRED,
            source_fields=("status",),
        )

    normalized = _STATUS_MAP.get(value.strip().lower())

    if normalized is not None:
        return MappedField[LicenceStatus](
            value=normalized,
            quality=FieldQuality.STANDARDIZED,
            source_fields=("status",),
        )

    return MappedField[LicenceStatus](
        value=LicenceStatus.UNKNOWN,
        quality=FieldQuality.INFERRED,
        source_fields=("status",),
    )


# Both fields are cited unconditionally so `unmapped_source_fields`
# reflects what the mapper actually consulted, even when subtype is null.
_CATEGORY_SOURCE_FIELDS: Final[tuple[str, ...]] = ("businesstype", "businesssubtype")


def _map_category(raw: Mapping[str, Any]) -> MappedField[CategoryRef]:
    type_str = _str_or_none(raw.get("businesstype"))
    subtype_str = _str_or_none(raw.get("businesssubtype"))

    if not type_str:
        return MappedField[CategoryRef](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=_CATEGORY_SOURCE_FIELDS,
        )

    label = f"{type_str} - {subtype_str}" if subtype_str else type_str

    return MappedField[CategoryRef](
        value=CategoryRef(
            code=_slugify(label),
            label=label,
            taxonomy_id=_TAXONOMY_ID,
            taxonomy_version=_TAXONOMY_VERSION,
        ),
        quality=FieldQuality.DERIVED,
        source_fields=_CATEGORY_SOURCE_FIELDS,
    )


def _map_issued_at(raw: Mapping[str, Any]) -> MappedField[date]:
    value = raw.get("issueddate")
    parsed = _parse_local_date_from_iso_datetime(value)

    if parsed is None:
        return MappedField[date](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("issueddate",),
        )

    return MappedField[date](
        value=parsed,
        quality=FieldQuality.STANDARDIZED,
        source_fields=("issueddate",),
    )


def _map_expires_at(raw: Mapping[str, Any]) -> MappedField[date]:
    value = raw.get("expireddate")

    if not isinstance(value, str) or not value:
        return MappedField[date](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("expireddate",),
        )

    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        return MappedField[date](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("expireddate",),
        )

    return MappedField[date](
        value=parsed,
        quality=FieldQuality.STANDARDIZED,
        source_fields=("expireddate",),
    )


def _map_address(raw: Mapping[str, Any]) -> MappedField[Address]:
    country = _str_or_none(raw.get("country"))
    province = _str_or_none(raw.get("province"))
    city = _str_or_none(raw.get("city"))
    house = _str_or_none(raw.get("house"))
    street_name = _str_or_none(raw.get("street"))
    unit = _str_or_none(raw.get("unit"))
    unittype = _str_or_none(raw.get("unittype"))
    postal = _str_or_none(raw.get("postalcode"))

    if not country:
        return MappedField[Address](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=_ADDRESS_SOURCE_FIELDS,
        )

    street = _assemble_street(house=house, street=street_name, unit=unit, unittype=unittype)

    try:
        address = Address(
            country=country,
            region=province,
            locality=city,
            street=street,
            postal_code=postal,
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


def _map_coordinate(raw: Mapping[str, Any]) -> MappedField[Coordinate]:
    value = raw.get("geo_point_2d")

    if not isinstance(value, dict):
        return MappedField[Coordinate](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("geo_point_2d",),
        )

    geo = cast(dict[str, Any], value)
    lat = geo.get("lat")
    lon = geo.get("lon")

    if not isinstance(lat, int | float) or not isinstance(lon, int | float):
        return MappedField[Coordinate](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("geo_point_2d",),
        )

    try:
        coord = Coordinate(latitude=float(lat), longitude=float(lon))
    except ValidationError:
        # Out-of-range coords degrade to NOT_PROVIDED; raw value remains
        # in raw_data for downstream inspection.
        return MappedField[Coordinate](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("geo_point_2d",),
        )

    return MappedField[Coordinate](
        value=coord,
        quality=FieldQuality.STANDARDIZED,
        source_fields=("geo_point_2d",),
    )


def _map_neighbourhood(raw: Mapping[str, Any]) -> MappedField[str]:
    value = _str_or_none(raw.get("localarea"))

    if value is None:
        return MappedField[str](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("localarea",),
        )

    return MappedField[str](
        value=value,
        quality=FieldQuality.DIRECT,
        source_fields=("localarea",),
    )


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


def _assemble_street(
    *,
    house: str | None,
    street: str | None,
    unit: str | None,
    unittype: str | None,
) -> str | None:
    """Combine house, street, and unit into one address line, or None."""
    if not house and not street:
        return None

    base = " ".join(p for p in (house, street) if p)

    if unit:
        unit_label = unittype if unittype else "Unit"
        return f"{base} {unit_label} {unit}"

    return base


def _parse_local_date_from_iso_datetime(value: object) -> date | None:
    """Parse an ISO 8601 datetime and return its calendar date in Vancouver.

    Naive timestamps are assumed UTC; OpenDataSoft normally serves
    explicit offsets, so this is a defensive fallback rather than a
    routine path.
    """
    if not isinstance(value, str) or not value:
        return None

    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)

    return parsed.astimezone(_LOCAL_TZ).date()


def _slugify(text: str) -> str:
    """Lowercase, replace any non-alphanumeric run with a single hyphen."""
    s = re.sub(r"[^a-z0-9]+", "-", text.lower().strip())

    return s.strip("-")
