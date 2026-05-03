"""FEMA Hazard Mitigation Assistance mappers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
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
from civix.domains.hazard_mitigation.adapters.sources.us.fema_hma.caveats import (
    OPENFEMA_HMA_TAXONOMY_VERSION,
    OPENFEMA_METADATA_DESCRIPTION_FIELD,
    openfema_hma_caveat_categories,
)
from civix.domains.hazard_mitigation.models.common import (
    MitigationFundingAmountKind,
    MitigationFundingEventType,
    MitigationFundingShareKind,
    MitigationGeographySemantics,
    MitigationInterventionType,
    MitigationOrganizationRole,
    MitigationProjectStatus,
)
from civix.domains.hazard_mitigation.models.funding import (
    MitigationFundingAmount,
    MitigationMoneyAmount,
)
from civix.domains.hazard_mitigation.models.geography import MitigationProjectGeography
from civix.domains.hazard_mitigation.models.organization import MitigationOrganization
from civix.domains.hazard_mitigation.models.project import HazardMitigationProject
from civix.domains.hazard_mitigation.models.transaction import MitigationFundingTransaction

PROJECT_MAPPER_ID: Final[MapperId] = MapperId("openfema-hma-projects")
TRANSACTION_MAPPER_ID: Final[MapperId] = MapperId("openfema-hma-financial-transactions")
MAPPER_VERSION: Final[str] = "0.1.0"
USD: Final[str] = "USD"

_PROGRAMME_TAXONOMY_ID: Final[str] = "openfema-hma-program-area"
_STATUS_TAXONOMY_ID: Final[str] = "openfema-hma-project-status"
_PROJECT_TYPE_TAXONOMY_ID: Final[str] = "openfema-hma-project-type"
_ORGANIZATION_ROLE_TAXONOMY_ID: Final[str] = "openfema-hma-organization-role"
_FUNDING_AMOUNT_TAXONOMY_ID: Final[str] = "openfema-hma-funding-amount-field"
_FUND_CODE_TAXONOMY_ID: Final[str] = "openfema-hma-fund-code"
_SOURCE_DATE_CONTEXT_TAXONOMY_ID: Final[str] = "openfema-hma-source-date-context"
_GEOGRAPHY_SEMANTICS_TAXONOMY_ID: Final[str] = "openfema-hma-geography-semantics"
_DATA_SOURCE_TAXONOMY_ID: Final[str] = "openfema-hma-data-source"

_STATUS_MAP: Final[dict[str, MitigationProjectStatus]] = {
    "approved": MitigationProjectStatus.APPROVED,
    "awarded": MitigationProjectStatus.APPROVED,
    "closed": MitigationProjectStatus.CLOSED,
    "completed": MitigationProjectStatus.COMPLETE,
    "obligated": MitigationProjectStatus.SOURCE_SPECIFIC,
    "pending": MitigationProjectStatus.PENDING,
    "revision requested": MitigationProjectStatus.PENDING,
    "void": MitigationProjectStatus.CANCELLED,
    "withdrawn": MitigationProjectStatus.CANCELLED,
}
_INTERVENTION_MAP: Final[tuple[tuple[str, MitigationInterventionType], ...]] = (
    ("acquisition", MitigationInterventionType.PROPERTY_ACQUISITION),
    ("elevation", MitigationInterventionType.ELEVATION),
)
_PROJECT_AMOUNT_FIELDS: Final[
    tuple[
        tuple[
            str,
            MitigationFundingAmountKind,
            MitigationFundingShareKind,
            MitigationFundingEventType | None,
        ],
        ...,
    ]
] = (
    (
        "projectAmount",
        MitigationFundingAmountKind.PROJECT_AMOUNT,
        MitigationFundingShareKind.TOTAL,
        None,
    ),
    (
        "initialObligationAmount",
        MitigationFundingAmountKind.PROJECT_AMOUNT,
        MitigationFundingShareKind.UNKNOWN,
        MitigationFundingEventType.OBLIGATION,
    ),
    (
        "federalShareObligated",
        MitigationFundingAmountKind.PROJECT_AMOUNT,
        MitigationFundingShareKind.FEDERAL,
        MitigationFundingEventType.OBLIGATION,
    ),
    (
        "recipientAdminCostAmt",
        MitigationFundingAmountKind.ADMINISTRATIVE_COST,
        MitigationFundingShareKind.RECIPIENT,
        None,
    ),
    (
        "subrecipientAdminCostAmt",
        MitigationFundingAmountKind.ADMINISTRATIVE_COST,
        MitigationFundingShareKind.SUBRECIPIENT,
        None,
    ),
    (
        "srmcObligatedAmt",
        MitigationFundingAmountKind.MANAGEMENT_COST,
        MitigationFundingShareKind.SUBRECIPIENT,
        MitigationFundingEventType.OBLIGATION,
    ),
)
_TRANSACTION_AMOUNT_FIELDS: Final[
    tuple[tuple[str, MitigationFundingAmountKind, MitigationFundingShareKind], ...]
] = (
    (
        "federalShareProjectCostAmt",
        MitigationFundingAmountKind.PROJECT_AMOUNT,
        MitigationFundingShareKind.FEDERAL,
    ),
    (
        "recipientAdminCostAmt",
        MitigationFundingAmountKind.ADMINISTRATIVE_COST,
        MitigationFundingShareKind.RECIPIENT,
    ),
    (
        "subrecipientAdminCostAmt",
        MitigationFundingAmountKind.ADMINISTRATIVE_COST,
        MitigationFundingShareKind.SUBRECIPIENT,
    ),
    (
        "subrecipientMgmtCostAmt",
        MitigationFundingAmountKind.MANAGEMENT_COST,
        MitigationFundingShareKind.SUBRECIPIENT,
    ),
)


@dataclass(frozen=True, slots=True)
class FemaHmaProjectMapper:
    """Maps FEMA HMA project rows to hazard mitigation projects."""

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
            project_id=_required_text(raw, "projectIdentifier", self.version, record),
            title=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            description=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            programme=_map_category_field(
                raw,
                "programArea",
                taxonomy_id=_PROGRAMME_TAXONOMY_ID,
            ),
            organizations=_map_project_organizations(raw),
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
            source_interventions=_map_source_categories(
                raw,
                "projectType",
                taxonomy_id=_PROJECT_TYPE_TAXONOMY_ID,
                split_semicolon=True,
            ),
            status=_map_project_status(raw),
            source_status=_map_category_field(
                raw,
                "status",
                taxonomy_id=_STATUS_TAXONOMY_ID,
            ),
            approval_period=_map_optional_date_period(raw, "dateApproved", self.version, record),
            project_period=_map_project_period(raw, self.version, record),
            fiscal_period=_map_year_period(raw, "programFy", mapper=self.version, record=record),
            publication_period=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            geography=_map_project_geography(raw),
            funding_summaries=_map_project_funding_summaries(raw, self.version, record),
            benefit_cost_ratio=_map_optional_decimal(raw, "benefitCostRatio", self.version, record),
            net_benefits=_map_optional_money(raw, "netValueBenefits", self.version, record),
            source_caveats=_map_project_caveats(raw),
        )

        return MapResult(record=project, report=_mapping_report(raw, project))


@dataclass(frozen=True, slots=True)
class FemaHmaTransactionMapper:
    """Maps FEMA HMA financial transaction rows to funding transactions."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=TRANSACTION_MAPPER_ID, version=MAPPER_VERSION)

    def __call__(
        self,
        record: RawRecord,
        snapshot: SourceSnapshot,
    ) -> MapResult[MitigationFundingTransaction]:
        raw = record.raw_data
        transaction = MitigationFundingTransaction(
            provenance=_build_provenance(record=record, snapshot=snapshot, mapper=self.version),
            transaction_id=_transaction_id(raw, mapper=self.version, record=record),
            project_id=MappedField(
                value=_required_text(raw, "projectIdentifier", self.version, record),
                quality=FieldQuality.DIRECT,
                source_fields=("projectIdentifier",),
            ),
            transaction_period=_map_optional_date_period(
                raw,
                "transactionDate",
                self.version,
                record,
            ),
            fiscal_period=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            funding_programme=_map_category_field(
                raw,
                "fundCode",
                taxonomy_id=_FUND_CODE_TAXONOMY_ID,
            ),
            event_type=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            source_event_category=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            amount_components=_map_transaction_amount_components(raw, self.version, record),
            source_caveats=MappedField(
                value=openfema_hma_caveat_categories(),
                quality=FieldQuality.STANDARDIZED,
                source_fields=(OPENFEMA_METADATA_DESCRIPTION_FIELD,),
            ),
        )

        return MapResult(record=transaction, report=_mapping_report(raw, transaction))


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


def _map_category_field(
    raw: Mapping[str, Any],
    field_name: str,
    *,
    taxonomy_id: str,
) -> MappedField[CategoryRef]:
    value = str_or_none(raw.get(field_name))

    if value is None:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=(field_name,),
        )

    return MappedField(
        value=_category(value, taxonomy_id=taxonomy_id),
        quality=FieldQuality.DERIVED,
        source_fields=(field_name,),
    )


def _map_source_categories(
    raw: Mapping[str, Any],
    field_name: str,
    *,
    taxonomy_id: str,
    split_semicolon: bool = False,
) -> MappedField[tuple[CategoryRef, ...]]:
    value = str_or_none(raw.get(field_name))

    if value is None:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=(field_name,),
        )

    labels = _split_semicolon(value) if split_semicolon else (value,)

    return MappedField(
        value=tuple(_category(label, taxonomy_id=taxonomy_id) for label in labels),
        quality=FieldQuality.DERIVED,
        source_fields=(field_name,),
    )


def _map_project_organizations(
    raw: Mapping[str, Any],
) -> MappedField[tuple[MitigationOrganization, ...]]:
    organizations: list[MitigationOrganization] = []

    for field_name, role in (
        ("recipient", MitigationOrganizationRole.RECIPIENT),
        ("subrecipient", MitigationOrganizationRole.SUBRECIPIENT),
    ):
        value = str_or_none(raw.get(field_name))

        if value is None:
            continue

        organizations.append(
            MitigationOrganization(
                role=role,
                name=value,
                source_role=_category(role.value, taxonomy_id=_ORGANIZATION_ROLE_TAXONOMY_ID),
            )
        )

    if not organizations:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("recipient", "subrecipient"),
        )

    return MappedField(
        value=tuple(organizations),
        quality=FieldQuality.DERIVED,
        source_fields=("recipient", "subrecipient"),
    )


def _map_intervention_types(
    raw: Mapping[str, Any],
) -> MappedField[tuple[MitigationInterventionType, ...]]:
    value = str_or_none(raw.get("projectType"))

    if value is None:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("projectType",),
        )

    mapped: list[MitigationInterventionType] = []

    for label in _split_semicolon(value):
        normalized = label.casefold()
        intervention = next(
            (candidate for needle, candidate in _INTERVENTION_MAP if needle in normalized),
            MitigationInterventionType.SOURCE_SPECIFIC,
        )

        if intervention not in mapped:
            mapped.append(intervention)

    return MappedField(
        value=tuple(mapped),
        quality=FieldQuality.STANDARDIZED,
        source_fields=("projectType",),
    )


def _map_project_status(raw: Mapping[str, Any]) -> MappedField[MitigationProjectStatus]:
    value = str_or_none(raw.get("status"))

    if value is None:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=("status",),
        )

    mapped = _STATUS_MAP.get(value.casefold(), MitigationProjectStatus.SOURCE_SPECIFIC)

    return MappedField(
        value=mapped,
        quality=FieldQuality.STANDARDIZED,
        source_fields=("status",),
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
    start = _datetime_or_none(raw.get("dateApproved"), "dateApproved", mapper, record)
    end = _datetime_or_none(raw.get("dateClosed"), "dateClosed", mapper, record)

    if start is None or end is None:
        return MappedField(
            value=None,
            quality=FieldQuality.UNMAPPED,
            source_fields=(),
        )

    return MappedField(
        value=TemporalPeriod(
            precision=TemporalPeriodPrecision.INTERVAL,
            start_datetime=start,
            end_datetime=end,
            timezone_status=TemporalTimezoneStatus.UTC,
        ),
        quality=FieldQuality.STANDARDIZED,
        source_fields=("dateApproved", "dateClosed"),
    )


def _map_year_period(
    raw: Mapping[str, Any],
    field_name: str,
    *,
    mapper: MapperVersion,
    record: RawRecord,
) -> MappedField[TemporalPeriod]:
    text = _required_text(raw, field_name, mapper, record)

    try:
        year = int(text)
    except ValueError as e:
        raise MappingError(
            f"invalid year source field {field_name!r}",
            mapper=mapper,
            source_record_id=record.source_record_id,
            source_fields=(field_name,),
        ) from e

    return MappedField(
        value=TemporalPeriod(
            precision=TemporalPeriodPrecision.YEAR,
            year_value=year,
            timezone_status=TemporalTimezoneStatus.UNKNOWN,
        ),
        quality=FieldQuality.STANDARDIZED,
        source_fields=(field_name,),
    )


def _map_project_geography(
    raw: Mapping[str, Any],
) -> MappedField[tuple[MitigationProjectGeography, ...]]:
    state = str_or_none(raw.get("state"))
    county = str_or_none(raw.get("county"))
    project_counties = _split_semicolon(str_or_none(raw.get("projectCounties")) or "")
    source_fields = ("state", "county", "projectCounties")

    if state is None and county is None and not project_counties:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=source_fields,
        )

    county_names: list[str] = []
    seen: set[str] = set()

    for name in (*project_counties, county) if county is not None else project_counties:
        normalized_name = name.casefold()

        if normalized_name in seen:
            continue

        seen.add(normalized_name)
        county_names.append(name)

    geographies: list[MitigationProjectGeography] = []

    for name in county_names:
        geographies.append(
            MitigationProjectGeography(
                semantics=MitigationGeographySemantics.COUNTY,
                address=Address(country="US", region=state),
                place_name=name,
                administrative_areas=tuple(area for area in (name, state) if area is not None),
                source_category=_category("county", taxonomy_id=_GEOGRAPHY_SEMANTICS_TAXONOMY_ID),
            )
        )

    if not geographies and state is not None:
        geographies.append(
            MitigationProjectGeography(
                semantics=MitigationGeographySemantics.STATE_OR_PROVINCE,
                address=Address(country="US", region=state),
                place_name=state,
                administrative_areas=(state,),
                source_category=_category("state", taxonomy_id=_GEOGRAPHY_SEMANTICS_TAXONOMY_ID),
            )
        )

    return MappedField(
        value=tuple(geographies),
        quality=FieldQuality.DERIVED,
        source_fields=source_fields,
    )


def _map_project_funding_summaries(
    raw: Mapping[str, Any],
    mapper: MapperVersion,
    record: RawRecord,
) -> MappedField[tuple[MitigationFundingAmount, ...]]:
    components: list[MitigationFundingAmount] = []

    for field_name, amount_kind, share_kind, lifecycle in _PROJECT_AMOUNT_FIELDS:
        value = _decimal_or_none(raw.get(field_name), field_name, mapper, record)

        if value is None:
            continue

        components.append(
            MitigationFundingAmount(
                money=MitigationMoneyAmount(amount=value, currency=USD),
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
            source_fields=tuple(field for field, *_ in _PROJECT_AMOUNT_FIELDS),
        )

    return MappedField(
        value=tuple(components),
        quality=FieldQuality.STANDARDIZED,
        source_fields=tuple(field for field, *_ in _PROJECT_AMOUNT_FIELDS),
    )


def _map_transaction_amount_components(
    raw: Mapping[str, Any],
    mapper: MapperVersion,
    record: RawRecord,
) -> MappedField[tuple[MitigationFundingAmount, ...]]:
    components: list[MitigationFundingAmount] = []

    for field_name, amount_kind, share_kind in _TRANSACTION_AMOUNT_FIELDS:
        value = _decimal_or_none(raw.get(field_name), field_name, mapper, record)

        if value is None:
            continue

        components.append(
            MitigationFundingAmount(
                money=MitigationMoneyAmount(amount=value, currency=USD),
                amount_kind=amount_kind,
                share_kind=share_kind,
                lifecycle=None,
                source_category=_category(field_name, taxonomy_id=_FUNDING_AMOUNT_TAXONOMY_ID),
            )
        )

    if not components:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=tuple(field for field, *_ in _TRANSACTION_AMOUNT_FIELDS),
        )

    return MappedField(
        value=tuple(components),
        quality=FieldQuality.STANDARDIZED,
        source_fields=tuple(field for field, *_ in _TRANSACTION_AMOUNT_FIELDS),
    )


def _map_optional_decimal(
    raw: Mapping[str, Any],
    field_name: str,
    mapper: MapperVersion,
    record: RawRecord,
) -> MappedField[Decimal]:
    value = _decimal_or_none(raw.get(field_name), field_name, mapper, record)

    if value is None:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=(field_name,),
        )

    return MappedField(value=value, quality=FieldQuality.DIRECT, source_fields=(field_name,))


def _map_optional_money(
    raw: Mapping[str, Any],
    field_name: str,
    mapper: MapperVersion,
    record: RawRecord,
) -> MappedField[MitigationMoneyAmount]:
    value = _decimal_or_none(raw.get(field_name), field_name, mapper, record)

    if value is None:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=(field_name,),
        )

    return MappedField(
        value=MitigationMoneyAmount(amount=value, currency=USD),
        quality=FieldQuality.STANDARDIZED,
        source_fields=(field_name,),
    )


def _map_project_caveats(raw: Mapping[str, Any]) -> MappedField[tuple[CategoryRef, ...]]:
    # The HMA projects metadata page repeats the same caveat paragraph as the
    # transactions dataset, including the non-official-financial-reporting
    # sentence, because project rows expose summary funding fields.
    caveats = list(openfema_hma_caveat_categories())
    source_fields = [OPENFEMA_METADATA_DESCRIPTION_FIELD]

    for field_name, label in (
        ("dateInitiallyApproved", "initial-approval-date-published"),
        ("initialObligationDate", "initial-obligation-date-published"),
    ):
        if str_or_none(raw.get(field_name)) is None:
            continue

        caveats.append(_category(label, taxonomy_id=_SOURCE_DATE_CONTEXT_TAXONOMY_ID))
        source_fields.append(field_name)

    data_source = str_or_none(raw.get("dataSource"))

    if data_source is not None:
        caveats.append(_category(data_source, taxonomy_id=_DATA_SOURCE_TAXONOMY_ID))
        source_fields.append("dataSource")

    return MappedField(
        value=tuple(caveats),
        quality=FieldQuality.STANDARDIZED,
        source_fields=tuple(source_fields),
    )


def _transaction_id(
    raw: Mapping[str, Any],
    *,
    mapper: MapperVersion,
    record: RawRecord,
) -> str:
    project_id = _required_text(raw, "projectIdentifier", mapper, record)
    transaction_id = _required_text(raw, "transactionIdentifier", mapper, record)

    return f"{project_id}:{transaction_id}"


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
        parsed = Decimal(text)
    except InvalidOperation as e:
        raise MappingError(
            f"invalid numeric source field {field_name!r}",
            mapper=mapper,
            source_record_id=record.source_record_id,
            source_fields=(field_name,),
        ) from e

    if not parsed.is_finite():
        raise MappingError(
            f"non-finite numeric source field {field_name!r}",
            mapper=mapper,
            source_record_id=record.source_record_id,
            source_fields=(field_name,),
        )

    return parsed


def _date_or_none(
    value: object,
    field_name: str,
    mapper: MapperVersion,
    record: RawRecord,
) -> date | None:
    parsed = _datetime_or_none(value, field_name, mapper, record)

    return None if parsed is None else parsed.date()


def _datetime_or_none(
    value: object,
    field_name: str,
    mapper: MapperVersion,
    record: RawRecord,
) -> datetime | None:
    text = str_or_none(value)

    if text is None:
        return None

    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as e:
        raise MappingError(
            f"invalid datetime source field {field_name!r}",
            mapper=mapper,
            source_record_id=record.source_record_id,
            source_fields=(field_name,),
        ) from e


def _split_semicolon(value: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in value.split(";") if part.strip())


def _category(label: str, *, taxonomy_id: str) -> CategoryRef:
    return CategoryRef(
        code=slugify(label),
        label=label,
        taxonomy_id=taxonomy_id,
        taxonomy_version=OPENFEMA_HMA_TAXONOMY_VERSION,
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
