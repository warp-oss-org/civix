"""England FCERM scheme-allocation mappers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Final, cast

from pydantic import BaseModel

from civix.core.identity.models.identifiers import MapperId
from civix.core.mapping.errors import MappingError
from civix.core.mapping.models.mapper import MappingReport, MapResult
from civix.core.mapping.parsers import require_text, slugify, str_or_none
from civix.core.provenance.models.provenance import MapperVersion, ProvenanceRef
from civix.core.quality.models.fields import FieldQuality, MappedField
from civix.core.snapshots.models.snapshot import RawRecord, SourceSnapshot
from civix.core.taxonomy.models.category import CategoryRef
from civix.core.temporal import (
    TemporalPeriod,
    TemporalPeriodPrecision,
    TemporalTimezoneStatus,
)
from civix.domains.hazard_mitigation.adapters.sources.gb.fcerm.adapter import (
    ENGLAND_FCERM_PROGRAMME_YEAR,
)
from civix.domains.hazard_mitigation.adapters.sources.gb.fcerm.caveats import (
    ENGLAND_FCERM_READ_ME_FIELD,
    ENGLAND_FCERM_TAXONOMY_VERSION,
    england_fcerm_caveat_categories,
)
from civix.domains.hazard_mitigation.adapters.sources.gb.fcerm.schema import (
    CEREMONIAL_COUNTY_FIELD,
    INDICATIVE_GOVERNMENT_INVESTMENT_FIELD,
    LEAD_AUTHORITY_FIELD,
    ONS_REGION_FIELD,
    PARLIAMENTARY_CONSTITUENCY_FIELD,
    PROJECT_NAME_FIELD,
    PROJECT_TYPE_FIELD,
    RFCC_FIELD,
    RISK_SOURCE_FIELD,
)
from civix.domains.hazard_mitigation.models.common import (
    MitigationFundingAmountKind,
    MitigationFundingEventType,
    MitigationFundingShareKind,
    MitigationGeographySemantics,
    MitigationHazardType,
    MitigationInterventionType,
    MitigationOrganizationRole,
)
from civix.domains.hazard_mitigation.models.funding import (
    MitigationFundingAmount,
    MitigationMoneyAmount,
)
from civix.domains.hazard_mitigation.models.geography import MitigationProjectGeography
from civix.domains.hazard_mitigation.models.organization import MitigationOrganization
from civix.domains.hazard_mitigation.models.project import HazardMitigationProject

PROJECT_MAPPER_ID: Final[MapperId] = MapperId("england-fcerm-schemes")
MAPPER_VERSION: Final[str] = "0.1.0"
GBP: Final[str] = "GBP"

_PROGRAMME_TAXONOMY_ID: Final[str] = "england-fcerm-programme"
_RISK_SOURCE_TAXONOMY_ID: Final[str] = "england-fcerm-risk-source"
_PROJECT_TYPE_TAXONOMY_ID: Final[str] = "england-fcerm-project-type"
_ORGANIZATION_ROLE_TAXONOMY_ID: Final[str] = "england-fcerm-organization-role"
_GEOGRAPHY_SEMANTICS_TAXONOMY_ID: Final[str] = "england-fcerm-geography-semantics"
_FUNDING_AMOUNT_TAXONOMY_ID: Final[str] = "england-fcerm-funding-amount-field"

_THOUSAND: Final[Decimal] = Decimal("1000")
_FISCAL_START: Final[datetime] = datetime(2026, 4, 1)
_FISCAL_END: Final[datetime] = datetime(2027, 3, 31)
_RISK_SOURCE_MAP: Final[dict[str, MitigationHazardType]] = {
    "coastal erosion": MitigationHazardType.COASTAL_EROSION,
    "river flooding": MitigationHazardType.FLOOD,
    "sea flooding": MitigationHazardType.FLOOD,
    "surface water flooding": MitigationHazardType.FLOOD,
}
_PROJECT_TYPE_MAP: Final[dict[str, MitigationInterventionType]] = {
    "defence": MitigationInterventionType.FLOOD_DEFENCE,
}


@dataclass(frozen=True, slots=True)
class EnglandFcermProjectMapper:
    """Maps England FCERM scheme-allocation rows to hazard mitigation projects."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=PROJECT_MAPPER_ID, version=MAPPER_VERSION)

    def __call__(
        self,
        record: RawRecord,
        snapshot: SourceSnapshot,
    ) -> MapResult[HazardMitigationProject]:
        raw = record.raw_data
        project = HazardMitigationProject(
            provenance=_build_provenance(record=record, snapshot=snapshot, mapper=self.version),
            project_id=_required_source_record_id(record, self.version),
            title=_map_text_field(raw, PROJECT_NAME_FIELD),
            description=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            programme=_map_programme(),
            organizations=_map_organizations(raw),
            hazard_types=_map_hazard_types(raw),
            source_hazards=_map_source_category_tuple(
                raw,
                RISK_SOURCE_FIELD,
                taxonomy_id=_RISK_SOURCE_TAXONOMY_ID,
            ),
            intervention_types=_map_intervention_types(raw),
            source_interventions=_map_source_category_tuple(
                raw,
                PROJECT_TYPE_FIELD,
                taxonomy_id=_PROJECT_TYPE_TAXONOMY_ID,
            ),
            status=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            source_status=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            approval_period=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            project_period=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            fiscal_period=_map_fiscal_period(),
            publication_period=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            geography=_map_geography(raw),
            funding_summaries=_map_funding_summaries(raw, self.version, record),
            benefit_cost_ratio=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            net_benefits=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            source_caveats=_map_source_caveats(),
        )

        return MapResult(record=project, report=_mapping_report(raw, project))


def _build_provenance(
    *,
    record: RawRecord,
    snapshot: SourceSnapshot,
    mapper: MapperVersion,
) -> ProvenanceRef:
    return ProvenanceRef(
        snapshot_id=snapshot.snapshot_id,
        source_id=snapshot.source_id,
        dataset_id=snapshot.dataset_id,
        jurisdiction=snapshot.jurisdiction,
        fetched_at=snapshot.fetched_at,
        mapper=mapper,
        source_record_id=record.source_record_id,
    )


def _required_source_record_id(record: RawRecord, mapper: MapperVersion) -> str:
    return require_text(
        record.source_record_id,
        field_name="source_record_id",
        mapper=mapper,
        source_record_id=record.source_record_id,
    )


def _map_text_field(raw: Mapping[str, Any], field_name: str) -> MappedField[str]:
    value = str_or_none(raw.get(field_name))

    if value is None:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=(field_name,),
        )

    return MappedField(value=value, quality=FieldQuality.DIRECT, source_fields=(field_name,))


def _map_programme() -> MappedField[CategoryRef]:
    return MappedField(
        value=CategoryRef(
            code="fcerm-2026-27",
            label=f"Flood and Coastal Erosion Risk Management {ENGLAND_FCERM_PROGRAMME_YEAR}",
            taxonomy_id=_PROGRAMME_TAXONOMY_ID,
            taxonomy_version=ENGLAND_FCERM_TAXONOMY_VERSION,
        ),
        quality=FieldQuality.STANDARDIZED,
        source_fields=(ENGLAND_FCERM_READ_ME_FIELD,),
    )


def _map_organizations(raw: Mapping[str, Any]) -> MappedField[tuple[MitigationOrganization, ...]]:
    value = str_or_none(raw.get(LEAD_AUTHORITY_FIELD))

    if value is None:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=(LEAD_AUTHORITY_FIELD,),
        )

    return MappedField(
        value=(
            MitigationOrganization(
                role=MitigationOrganizationRole.LEAD_AUTHORITY,
                name=value,
                source_role=_category(
                    "lead risk management authority",
                    taxonomy_id=_ORGANIZATION_ROLE_TAXONOMY_ID,
                ),
            ),
        ),
        quality=FieldQuality.DERIVED,
        source_fields=(LEAD_AUTHORITY_FIELD,),
    )


def _map_hazard_types(raw: Mapping[str, Any]) -> MappedField[tuple[MitigationHazardType, ...]]:
    value = str_or_none(raw.get(RISK_SOURCE_FIELD))

    if value is None:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=(RISK_SOURCE_FIELD,),
        )

    mapped = _RISK_SOURCE_MAP.get(value.casefold(), MitigationHazardType.SOURCE_SPECIFIC)

    return MappedField(
        value=(mapped,),
        quality=FieldQuality.STANDARDIZED,
        source_fields=(RISK_SOURCE_FIELD,),
    )


def _map_intervention_types(
    raw: Mapping[str, Any],
) -> MappedField[tuple[MitigationInterventionType, ...]]:
    value = str_or_none(raw.get(PROJECT_TYPE_FIELD))

    if value is None:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=(PROJECT_TYPE_FIELD,),
        )

    mapped = _PROJECT_TYPE_MAP.get(value.casefold(), MitigationInterventionType.SOURCE_SPECIFIC)

    return MappedField(
        value=(mapped,),
        quality=FieldQuality.STANDARDIZED,
        source_fields=(PROJECT_TYPE_FIELD,),
    )


def _map_source_category_tuple(
    raw: Mapping[str, Any],
    field_name: str,
    *,
    taxonomy_id: str,
) -> MappedField[tuple[CategoryRef, ...]]:
    value = str_or_none(raw.get(field_name))

    if value is None:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=(field_name,),
        )

    return MappedField(
        value=(_category(value, taxonomy_id=taxonomy_id),),
        quality=FieldQuality.DERIVED,
        source_fields=(field_name,),
    )


def _map_fiscal_period() -> MappedField[TemporalPeriod]:
    return MappedField(
        value=TemporalPeriod(
            precision=TemporalPeriodPrecision.INTERVAL,
            start_datetime=_FISCAL_START,
            end_datetime=_FISCAL_END,
            timezone_status=TemporalTimezoneStatus.UNKNOWN,
        ),
        quality=FieldQuality.STANDARDIZED,
        source_fields=(ENGLAND_FCERM_READ_ME_FIELD,),
    )


def _map_geography(raw: Mapping[str, Any]) -> MappedField[tuple[MitigationProjectGeography, ...]]:
    areas = tuple(
        value
        for value in (
            str_or_none(raw.get(ONS_REGION_FIELD)),
            str_or_none(raw.get(RFCC_FIELD)),
            str_or_none(raw.get(PARLIAMENTARY_CONSTITUENCY_FIELD)),
            str_or_none(raw.get(CEREMONIAL_COUNTY_FIELD)),
        )
        if value is not None
    )
    source_fields = (
        ONS_REGION_FIELD,
        RFCC_FIELD,
        PARLIAMENTARY_CONSTITUENCY_FIELD,
        CEREMONIAL_COUNTY_FIELD,
    )

    if not areas:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=source_fields,
        )

    geography = MitigationProjectGeography(
        semantics=MitigationGeographySemantics.PROJECT_AREA,
        place_name=areas[-1],
        administrative_areas=areas,
        source_category=_category("project area", taxonomy_id=_GEOGRAPHY_SEMANTICS_TAXONOMY_ID),
    )

    return MappedField(
        value=(geography,),
        quality=FieldQuality.DERIVED,
        source_fields=source_fields,
    )


def _map_funding_summaries(
    raw: Mapping[str, Any],
    mapper: MapperVersion,
    record: RawRecord,
) -> MappedField[tuple[MitigationFundingAmount, ...]]:
    amount_thousands = _decimal_or_none(
        raw.get(INDICATIVE_GOVERNMENT_INVESTMENT_FIELD),
        INDICATIVE_GOVERNMENT_INVESTMENT_FIELD,
        mapper,
        record,
    )

    if amount_thousands is None:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=(INDICATIVE_GOVERNMENT_INVESTMENT_FIELD,),
        )

    return MappedField(
        value=(
            MitigationFundingAmount(
                money=MitigationMoneyAmount(amount=amount_thousands * _THOUSAND, currency=GBP),
                amount_kind=MitigationFundingAmountKind.PROJECT_AMOUNT,
                share_kind=MitigationFundingShareKind.GOVERNMENT,
                lifecycle=MitigationFundingEventType.PLANNED_AMOUNT,
                source_category=_category(
                    INDICATIVE_GOVERNMENT_INVESTMENT_FIELD,
                    taxonomy_id=_FUNDING_AMOUNT_TAXONOMY_ID,
                ),
            ),
        ),
        quality=FieldQuality.STANDARDIZED,
        source_fields=(INDICATIVE_GOVERNMENT_INVESTMENT_FIELD,),
    )


def _map_source_caveats() -> MappedField[tuple[CategoryRef, ...]]:
    return MappedField(
        value=england_fcerm_caveat_categories(),
        quality=FieldQuality.STANDARDIZED,
        source_fields=(ENGLAND_FCERM_READ_ME_FIELD,),
    )


def _decimal_or_none(
    value: object,
    field_name: str,
    mapper: MapperVersion,
    record: RawRecord,
) -> Decimal | None:
    text = str_or_none(value)

    if text is None:
        return None

    try:
        return Decimal(text)
    except InvalidOperation as e:
        raise MappingError(
            f"invalid decimal source field {field_name!r}",
            mapper=mapper,
            source_record_id=record.source_record_id,
            source_fields=(field_name,),
        ) from e


def _category(label: str, *, taxonomy_id: str) -> CategoryRef:
    return CategoryRef(
        code=slugify(label),
        label=label,
        taxonomy_id=taxonomy_id,
        taxonomy_version=ENGLAND_FCERM_TAXONOMY_VERSION,
    )


def _mapping_report(raw: Mapping[str, Any], record: BaseModel) -> MappingReport:
    consumed: set[str] = set()
    _collect_source_fields(record, consumed)

    return MappingReport(unmapped_source_fields=tuple(sorted(set(raw) - consumed)))


def _collect_source_fields(value: object, consumed: set[str]) -> None:
    if isinstance(value, MappedField):
        consumed.update(value.source_fields)
        return

    if isinstance(value, BaseModel):
        for field_name in value.__class__.model_fields:
            _collect_source_fields(getattr(value, field_name), consumed)
        return

    if isinstance(value, tuple):
        for item in cast(tuple[object, ...], value):
            _collect_source_fields(item, consumed)
