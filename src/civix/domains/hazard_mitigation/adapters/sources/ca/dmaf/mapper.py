"""Canada DMAF project mappers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, time
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
from civix.core.spatial.models.location import Address
from civix.core.taxonomy.models.category import CategoryRef
from civix.core.temporal import (
    TemporalPeriod,
    TemporalPeriodPrecision,
    TemporalTimezoneStatus,
)
from civix.domains.hazard_mitigation.adapters.sources.ca.dmaf.caveats import (
    CANADA_DMAF_TAXONOMY_VERSION,
    CanadaDmafCaveat,
    canada_dmaf_caveat_category,
)
from civix.domains.hazard_mitigation.adapters.sources.ca.dmaf.schema import (
    ADAPTER_CONSUMED_FIELDS,
)
from civix.domains.hazard_mitigation.models.common import (
    MitigationFundingAmountKind,
    MitigationFundingEventType,
    MitigationFundingShareKind,
    MitigationGeographySemantics,
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

PROJECT_MAPPER_ID: Final[MapperId] = MapperId("canada-dmaf-projects")
MAPPER_VERSION: Final[str] = "0.1.0"
CAD: Final[str] = "CAD"

_PROGRAMME_TAXONOMY_ID: Final[str] = "canada-dmaf-program-code"
_CATEGORY_TAXONOMY_ID: Final[str] = "canada-dmaf-category"
_ORGANIZATION_ROLE_TAXONOMY_ID: Final[str] = "canada-dmaf-organization-role"
_GEOGRAPHY_SEMANTICS_TAXONOMY_ID: Final[str] = "canada-dmaf-geography-semantics"
_FUNDING_AMOUNT_TAXONOMY_ID: Final[str] = "canada-dmaf-funding-amount-field"


@dataclass(frozen=True, slots=True)
class CanadaDmafProjectMapper:
    """Maps Infrastructure Canada DMAF rows to hazard mitigation projects."""

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
            project_id=_required_text(raw, "projectNumber", self.version, record),
            title=_map_text_field(raw, "projectTitle_en"),
            description=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            programme=_map_programme(raw),
            organizations=_map_organizations(raw),
            hazard_types=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            source_hazards=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            intervention_types=_map_intervention_types(raw),
            source_interventions=_map_source_interventions(raw),
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
            approval_period=_map_optional_date_period(raw, "approvedDate", self.version, record),
            project_period=_map_project_period(raw, self.version, record),
            fiscal_period=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
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
            source_caveats=_map_source_caveats(raw),
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


def _required_text(
    raw: Mapping[str, Any],
    field_name: str,
    mapper: MapperVersion,
    record: RawRecord,
) -> str:
    return require_text(
        raw.get(field_name),
        field_name=field_name,
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


def _map_programme(raw: Mapping[str, Any]) -> MappedField[CategoryRef]:
    code = str_or_none(raw.get("programCode_en"))
    label = str_or_none(raw.get("program_en"))

    if code is None and label is None:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("programCode_en", "program_en"),
        )

    if code is None or label is None:
        return MappedField(
            value=None,
            quality=FieldQuality.CONFLICTED,
            source_fields=("programCode_en", "program_en"),
        )

    return MappedField(
        value=CategoryRef(
            code=slugify(code),
            label=label,
            taxonomy_id=_PROGRAMME_TAXONOMY_ID,
            taxonomy_version=CANADA_DMAF_TAXONOMY_VERSION,
        ),
        quality=FieldQuality.DERIVED,
        source_fields=("programCode_en", "program_en"),
    )


def _map_organizations(raw: Mapping[str, Any]) -> MappedField[tuple[MitigationOrganization, ...]]:
    value = str_or_none(raw.get("ultimateRecipient_en"))

    if value is None:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("ultimateRecipient_en",),
        )

    return MappedField(
        value=(
            MitigationOrganization(
                role=MitigationOrganizationRole.RECIPIENT,
                name=value,
                source_role=_category("recipient", taxonomy_id=_ORGANIZATION_ROLE_TAXONOMY_ID),
            ),
        ),
        quality=FieldQuality.DERIVED,
        source_fields=("ultimateRecipient_en",),
    )


def _map_intervention_types(
    raw: Mapping[str, Any],
) -> MappedField[tuple[MitigationInterventionType, ...]]:
    value = str_or_none(raw.get("category_en"))

    if value is None:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("category_en",),
        )

    return MappedField(
        value=(MitigationInterventionType.SOURCE_SPECIFIC,),
        quality=FieldQuality.STANDARDIZED,
        source_fields=("category_en",),
    )


def _map_source_interventions(raw: Mapping[str, Any]) -> MappedField[tuple[CategoryRef, ...]]:
    value = str_or_none(raw.get("category_en"))

    if value is None:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("category_en",),
        )

    return MappedField(
        value=(_category(value, taxonomy_id=_CATEGORY_TAXONOMY_ID),),
        quality=FieldQuality.DERIVED,
        source_fields=("category_en",),
    )


def _map_optional_date_period(
    raw: Mapping[str, Any],
    field_name: str,
    mapper: MapperVersion,
    record: RawRecord,
) -> MappedField[TemporalPeriod]:
    parsed = _date_or_none(raw.get(field_name), field_name, mapper, record)

    if parsed is None:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=(field_name,),
        )

    return MappedField(
        value=TemporalPeriod(
            precision=TemporalPeriodPrecision.DATE,
            date_value=parsed,
            timezone_status=TemporalTimezoneStatus.UNKNOWN,
        ),
        quality=FieldQuality.STANDARDIZED,
        source_fields=(field_name,),
    )


def _map_project_period(
    raw: Mapping[str, Any],
    mapper: MapperVersion,
    record: RawRecord,
) -> MappedField[TemporalPeriod]:
    start = _date_or_none(raw.get("constructionStartDate"), "constructionStartDate", mapper, record)
    end = _date_or_none(raw.get("constructionEndDate"), "constructionEndDate", mapper, record)

    if start is None or end is None:
        return MappedField(value=None, quality=FieldQuality.UNMAPPED, source_fields=())

    return MappedField(
        value=TemporalPeriod(
            precision=TemporalPeriodPrecision.INTERVAL,
            start_datetime=datetime.combine(start, time.min),
            end_datetime=datetime.combine(end, time.min),
            timezone_status=TemporalTimezoneStatus.UNKNOWN,
        ),
        quality=FieldQuality.STANDARDIZED,
        source_fields=("constructionStartDate", "constructionEndDate"),
    )


def _map_geography(raw: Mapping[str, Any]) -> MappedField[tuple[MitigationProjectGeography, ...]]:
    location = str_or_none(raw.get("location_en"))
    region = str_or_none(raw.get("region"))
    source_fields = ("location_en", "region")

    if location is None and region is None:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=source_fields,
        )

    region_code = region.upper() if region is not None else None
    geography = MitigationProjectGeography(
        semantics=(
            MitigationGeographySemantics.MUNICIPALITY
            if location is not None
            else MitigationGeographySemantics.STATE_OR_PROVINCE
        ),
        address=Address(country="CA", region=region_code),
        place_name=location or region_code,
        administrative_areas=tuple(area for area in (location, region_code) if area is not None),
        source_category=_category(
            "municipality" if location is not None else "state_or_province",
            taxonomy_id=_GEOGRAPHY_SEMANTICS_TAXONOMY_ID,
        ),
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
    components: list[MitigationFundingAmount] = []

    for field_name, amount_kind, share_kind, lifecycle in (
        (
            "federalContribution",
            MitigationFundingAmountKind.PROJECT_AMOUNT,
            MitigationFundingShareKind.FEDERAL,
            MitigationFundingEventType.AWARD,
        ),
        (
            "totalEligibleCost",
            MitigationFundingAmountKind.TOTAL_ELIGIBLE_COST,
            MitigationFundingShareKind.TOTAL,
            None,
        ),
    ):
        value = _decimal_or_none(raw.get(field_name), field_name, mapper, record)

        if value is None:
            continue

        components.append(
            MitigationFundingAmount(
                money=MitigationMoneyAmount(amount=value, currency=CAD),
                amount_kind=amount_kind,
                share_kind=share_kind,
                lifecycle=lifecycle,
                source_category=_category(field_name, taxonomy_id=_FUNDING_AMOUNT_TAXONOMY_ID),
            )
        )

    if not components:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("federalContribution", "totalEligibleCost"),
        )

    return MappedField(
        value=tuple(components),
        quality=FieldQuality.STANDARDIZED,
        source_fields=("federalContribution", "totalEligibleCost"),
    )


def _map_source_caveats(raw: Mapping[str, Any]) -> MappedField[tuple[CategoryRef, ...]]:
    caveats: list[CategoryRef] = []
    source_fields: list[str] = []

    if (
        str_or_none(raw.get("forecastedConstructionStartDate")) is not None
        or str_or_none(raw.get("forecastedConstructionEndDate")) is not None
    ):
        caveats.append(canada_dmaf_caveat_category(CanadaDmafCaveat.FORECAST_CONSTRUCTION_DATES))
        source_fields.extend(("forecastedConstructionStartDate", "forecastedConstructionEndDate"))

    if str_or_none(raw.get("totalEligibleCost")) is not None:
        caveats.append(canada_dmaf_caveat_category(CanadaDmafCaveat.TOTAL_ELIGIBLE_COST_LIFECYCLE))
        source_fields.append("totalEligibleCost")

    if not caveats:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=(
                "forecastedConstructionStartDate",
                "forecastedConstructionEndDate",
                "totalEligibleCost",
            ),
        )

    return MappedField(
        value=tuple(caveats),
        quality=FieldQuality.STANDARDIZED,
        source_fields=tuple(source_fields),
    )


def _date_or_none(
    value: object,
    field_name: str,
    mapper: MapperVersion,
    record: RawRecord,
) -> date | None:
    text = str_or_none(value)

    if text is None:
        return None

    try:
        return date.fromisoformat(text)
    except ValueError as e:
        raise MappingError(
            f"invalid date source field {field_name!r}",
            mapper=mapper,
            source_record_id=record.source_record_id,
            source_fields=(field_name,),
        ) from e


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
        taxonomy_version=CANADA_DMAF_TAXONOMY_VERSION,
    )


def _mapping_report(raw: Mapping[str, Any], record: BaseModel) -> MappingReport:
    consumed: set[str] = set(ADAPTER_CONSUMED_FIELDS)
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
