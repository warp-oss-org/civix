"""Edmonton business-licences mapper."""

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
from civix.infra.sources.ca.edmonton_business_licences.schema import (
    ADAPTER_CONSUMED_FIELDS,
)

MAPPER_ID: Final[MapperId] = MapperId("edmonton-business-licences")
MAPPER_VERSION: Final[str] = "0.1.0"

_TAXONOMY_ID: Final[str] = "edmonton-business-licence-categories"
_TAXONOMY_VERSION: Final[str] = "2026-04-30"
_INFERRED_COUNTRY: Final[str] = "CA"
_INFERRED_REGION: Final[str] = "AB"
_INFERRED_LOCALITY: Final[str] = "Edmonton"

_ADDRESS_SOURCE_FIELDS: Final[tuple[str, ...]] = ("business_address",)
_COORDINATE_SOURCE_FIELDS: Final[tuple[str, ...]] = ("latitude", "longitude")


@dataclass(frozen=True, slots=True)
class EdmontonBusinessLicencesMapper:
    """Maps Edmonton Socrata business-licence rows to `BusinessLicence`."""

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
            issued_at=_map_date(raw, "most_recent_issue_date"),
            expires_at=_map_date(raw, "expiry_date"),
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
    value = _str_or_none(raw.get("business_name"))

    if value is None:
        return MappedField[str](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("business_name",),
        )

    return MappedField[str](
        value=value,
        quality=FieldQuality.DIRECT,
        source_fields=("business_name",),
    )


def _map_licence_number(raw: Mapping[str, Any]) -> MappedField[str]:
    value = _str_or_none(raw.get("externalid"))

    if value is None:
        return MappedField[str](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("externalid",),
        )

    return MappedField[str](
        value=value,
        quality=FieldQuality.DIRECT,
        source_fields=("externalid",),
    )


def _map_status(raw: Mapping[str, Any]) -> MappedField[LicenceStatus]:
    _ = raw.get("expiry_date")

    return MappedField[LicenceStatus](
        value=None,
        quality=FieldQuality.NOT_PROVIDED,
        source_fields=("expiry_date",),
    )


def _map_category(raw: Mapping[str, Any]) -> MappedField[CategoryRef]:
    value = _str_or_none(raw.get("business_licence_category"))
    label = _primary_category(value)

    if label is None:
        return MappedField[CategoryRef](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("business_licence_category",),
        )

    return MappedField[CategoryRef](
        value=CategoryRef(
            code=_slugify(label),
            label=label,
            taxonomy_id=_TAXONOMY_ID,
            taxonomy_version=_TAXONOMY_VERSION,
        ),
        quality=FieldQuality.DERIVED,
        source_fields=("business_licence_category",),
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
    street = _str_or_none(raw.get("business_address"))

    try:
        address = Address(
            country=_INFERRED_COUNTRY,
            region=_INFERRED_REGION,
            locality=_INFERRED_LOCALITY,
            street=street,
            postal_code=None,
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


def _map_neighbourhood(raw: Mapping[str, Any]) -> MappedField[str]:
    value = _str_or_none(raw.get("neighbourhood"))

    if value is None:
        return MappedField[str](
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("neighbourhood",),
        )

    return MappedField[str](
        value=value,
        quality=FieldQuality.DIRECT,
        source_fields=("neighbourhood",),
    )


def _unmapped_source_fields(raw: Mapping[str, Any], licence: BusinessLicence) -> tuple[str, ...]:
    consumed: set[str] = set()

    for field_name in licence.__class__.model_fields:
        attr = getattr(licence, field_name)

        if isinstance(attr, MappedField):
            consumed.update(attr.source_fields)

    consumed |= ADAPTER_CONSUMED_FIELDS

    return tuple(sorted(name for name in raw if name not in consumed))


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


def _primary_category(value: str | None) -> str | None:
    if value is None:
        return None

    parts = re.split(r";\s*|\r?\n", value)

    for part in parts:
        normalized = _str_or_none(part)

        if normalized is not None:
            return normalized

    return None


def _parse_date(value: object) -> date | None:
    text = _str_or_none(value)

    if text is None:
        return None

    if re.fullmatch(r"\d{14}", text):
        text = text[:8]

    if re.fullmatch(r"\d{8}", text):
        try:
            return datetime.strptime(text, "%Y%m%d").replace(tzinfo=UTC).date()
        except ValueError:
            return None

    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _slugify(text: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", text.lower().strip())

    return s.strip("-")
