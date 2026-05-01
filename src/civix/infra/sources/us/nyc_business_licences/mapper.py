"""NYC DCWP premises business-licences mapper."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Final

from pydantic import ValidationError

from civix.core.identity.models.identifiers import MapperId
from civix.core.mapping.models.mapper import MappingReport, MapResult
from civix.core.provenance.models.provenance import MapperVersion, ProvenanceRef
from civix.core.quality.models.fields import FieldQuality, MappedField
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.spatial.models.location import Address, Coordinate
from civix.core.taxonomy.models.category import CategoryRef
from civix.domains.business_licences.models.licence import (
    BusinessLicence,
    LicenceStatus,
)
from civix.infra.sources.us.nyc_business_licences.schema import ADAPTER_CONSUMED_FIELDS

MAPPER_ID: Final[MapperId] = MapperId("nyc-business-licences")
MAPPER_VERSION: Final[str] = "0.1.0"

_TAXONOMY_ID: Final[str] = "nyc-dcwp-business-categories"
_TAXONOMY_VERSION: Final[str] = "2026-04-30"
_INFERRED_COUNTRY: Final[str] = "US"

_STATUS_MAP: Final[dict[str, LicenceStatus]] = {
    "active": LicenceStatus.ACTIVE,
    "expired": LicenceStatus.EXPIRED,
    "surrendered": LicenceStatus.SURRENDERED,
    "revoked": LicenceStatus.REVOKED,
    "suspended": LicenceStatus.SUSPENDED,
    "ready for renewal": LicenceStatus.RENEWAL_DUE,
}
_UNKNOWN_STATUS_VALUES: Final[frozenset[str]] = frozenset(
    {
        "failed to renew",
        "voided",
        "out of business",
        "close",
        "tol",
    }
)
_ADDRESS_SOURCE_FIELDS: Final[tuple[str, ...]] = (
    "address_building",
    "address_street_name",
    "address_street_name_2",
    "street3",
    "unit_type",
    "apt_suite",
    "address_city",
    "address_state",
    "address_zip",
)
_COORDINATE_SOURCE_FIELDS: Final[tuple[str, ...]] = ("latitude", "longitude")


@dataclass(frozen=True, slots=True)
class NycBusinessLicencesMapper:
    """Maps NYC DCWP premises licence rows to `BusinessLicence`."""

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
            issued_at=_map_date(raw, "license_creation_date"),
            expires_at=_map_date(raw, "lic_expir_dd"),
            address=_map_address(raw),
            coordinate=_map_coordinate(raw),
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
    dba = _str_or_none(raw.get("dba_trade_name"))
    legal_name = _str_or_none(raw.get("business_name"))

    if dba is not None:
        return MappedField[str](
            value=dba,
            quality=FieldQuality.DIRECT,
            source_fields=("dba_trade_name",),
        )

    if legal_name is None:
        return MappedField[str](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("dba_trade_name", "business_name"),
        )

    return MappedField[str](
        value=legal_name,
        quality=FieldQuality.DIRECT,
        source_fields=("dba_trade_name", "business_name"),
    )


def _map_licence_number(raw: Mapping[str, Any]) -> MappedField[str]:
    value = _str_or_none(raw.get("license_nbr"))

    if value is None:
        return MappedField[str](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("license_nbr",),
        )

    return MappedField[str](
        value=value,
        quality=FieldQuality.DIRECT,
        source_fields=("license_nbr",),
    )


def _map_status(raw: Mapping[str, Any]) -> MappedField[LicenceStatus]:
    value = _str_or_none(raw.get("license_status"))

    if value is None:
        return MappedField[LicenceStatus](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("license_status",),
        )

    normalized = value.casefold()
    mapped = _STATUS_MAP.get(normalized)

    if mapped is not None:
        return MappedField[LicenceStatus](
            value=mapped,
            quality=FieldQuality.STANDARDIZED,
            source_fields=("license_status",),
        )

    return MappedField[LicenceStatus](
        value=LicenceStatus.UNKNOWN,
        quality=FieldQuality.INFERRED,
        source_fields=("license_status",),
    )


def _map_category(raw: Mapping[str, Any]) -> MappedField[CategoryRef]:
    label = _str_or_none(raw.get("business_category"))

    if label is None:
        return MappedField[CategoryRef](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("business_category",),
        )

    return MappedField[CategoryRef](
        value=CategoryRef(
            code=_slugify(label),
            label=label,
            taxonomy_id=_TAXONOMY_ID,
            taxonomy_version=_TAXONOMY_VERSION,
        ),
        quality=FieldQuality.DERIVED,
        source_fields=("business_category",),
    )


def _map_date(raw: Mapping[str, Any], field_name: str) -> MappedField[date]:
    parsed = _parse_date(raw.get(field_name))

    if parsed is None:
        return MappedField[date](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=(field_name,),
        )

    return MappedField[date](
        value=parsed,
        quality=FieldQuality.STANDARDIZED,
        source_fields=(field_name,),
    )


def _map_address(raw: Mapping[str, Any]) -> MappedField[Address]:
    street = _street(raw)
    locality = _str_or_none(raw.get("address_city"))
    region = _str_or_none(raw.get("address_state"))
    postal_code = _str_or_none(raw.get("address_zip"))

    try:
        address = Address(
            country=_INFERRED_COUNTRY,
            region=region,
            locality=locality,
            street=street,
            postal_code=postal_code,
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
    lat = _float_or_none(raw.get("latitude"))
    lon = _float_or_none(raw.get("longitude"))

    if lat is None or lon is None:
        return MappedField[Coordinate](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=_COORDINATE_SOURCE_FIELDS,
        )

    try:
        coordinate = Coordinate(latitude=lat, longitude=lon)
    except ValidationError:
        return MappedField[Coordinate](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=_COORDINATE_SOURCE_FIELDS,
        )

    return MappedField[Coordinate](
        value=coordinate,
        quality=FieldQuality.STANDARDIZED,
        source_fields=_COORDINATE_SOURCE_FIELDS,
    )


def _unmapped_neighbourhood() -> MappedField[str]:
    return MappedField[str](value=None, quality=FieldQuality.UNMAPPED, source_fields=())


def _unmapped_source_fields(raw: Mapping[str, Any], licence: BusinessLicence) -> tuple[str, ...]:
    consumed: set[str] = set()

    for field_name in licence.__class__.model_fields:
        attr = getattr(licence, field_name)

        if isinstance(attr, MappedField):
            consumed.update(attr.source_fields)

    consumed |= ADAPTER_CONSUMED_FIELDS

    return tuple(sorted(name for name in raw if name not in consumed))


def _street(raw: Mapping[str, Any]) -> str | None:
    building = _str_or_none(raw.get("address_building"))
    street1 = _str_or_none(raw.get("address_street_name"))
    street2 = _str_or_none(raw.get("address_street_name_2"))
    street3 = _str_or_none(raw.get("street3"))
    unit_type = _str_or_none(raw.get("unit_type"))
    apt_suite = _str_or_none(raw.get("apt_suite"))

    street_parts = [part for part in (building, street1, street2, street3) if part is not None]
    street = " ".join(street_parts) if street_parts else None

    if apt_suite is None:
        return street

    unit_label = unit_type if unit_type is not None else "Unit"
    unit = f"{unit_label} {apt_suite}"

    if street is None:
        return unit

    return f"{street} {unit}"


def _str_or_none(value: object) -> str | None:
    if value is None:
        return None

    s = str(value).strip()

    return s if s else None


def _float_or_none(value: object) -> float | None:
    text = _str_or_none(value)

    if text is None:
        return None

    try:
        return float(text)
    except ValueError:
        return None


def _parse_date(value: object) -> date | None:
    text = _str_or_none(value)

    if text is None:
        return None

    normalized = text.replace("Z", "+00:00")

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)

    return parsed.astimezone(UTC).date()


def _slugify(text: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", text.lower().strip())

    return s.strip("-")
