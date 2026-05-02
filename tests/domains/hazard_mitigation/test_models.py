from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from civix.core.identity.models.identifiers import (
    DatasetId,
    Jurisdiction,
    MapperId,
    SnapshotId,
    SourceId,
)
from civix.core.provenance.models.provenance import MapperVersion, ProvenanceRef
from civix.core.quality.models.fields import FieldQuality, MappedField
from civix.core.spatial.models.geometry import SpatialFootprint
from civix.core.spatial.models.location import Address, Coordinate
from civix.core.taxonomy.models.category import CategoryRef
from civix.core.temporal import TemporalPeriod, TemporalPeriodPrecision, TemporalTimezoneStatus
from civix.domains.hazard_mitigation.models.common import (
    MitigationFundingAmountKind,
    MitigationFundingEventType,
    MitigationFundingShareKind,
    MitigationGeographySemantics,
    MitigationHazardType,
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


def _mapped[T](
    value: T | None,
    *source_fields: str,
    quality: FieldQuality = FieldQuality.DIRECT,
) -> MappedField[T]:
    return MappedField[T](value=value, quality=quality, source_fields=source_fields)


def _provenance(source_record_id: str = "hma-project-1") -> ProvenanceRef:
    return ProvenanceRef(
        snapshot_id=SnapshotId("snap-hazard-1"),
        source_id=SourceId("hazard-mitigation-test-source"),
        dataset_id=DatasetId("hazard-mitigation-projects"),
        jurisdiction=Jurisdiction(country="US"),
        fetched_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
        mapper=MapperVersion(
            mapper_id=MapperId("hazard-mitigation-test-mapper"),
            version="0.1.0",
        ),
        source_record_id=source_record_id,
    )


def _category(code: str = "hma") -> CategoryRef:
    return CategoryRef(
        code=code,
        label=code.replace("-", " ").title(),
        taxonomy_id="civix.hazard-mitigation.test",
        taxonomy_version="2026-05-01",
    )


def _period(year: int = 2026) -> TemporalPeriod:
    return TemporalPeriod(
        precision=TemporalPeriodPrecision.YEAR,
        year_value=year,
        timezone_status=TemporalTimezoneStatus.UNKNOWN,
    )


def _date_period(value: date = date(2026, 5, 1)) -> TemporalPeriod:
    return TemporalPeriod(
        precision=TemporalPeriodPrecision.DATE,
        date_value=value,
        timezone_status=TemporalTimezoneStatus.UNKNOWN,
    )


def _money(amount: Decimal = Decimal("100.00"), currency: str = "USD") -> MitigationMoneyAmount:
    return MitigationMoneyAmount(amount=amount, currency=currency)


def _funding_amount(
    amount: Decimal = Decimal("100.00"),
    *,
    currency: str = "USD",
    amount_kind: MitigationFundingAmountKind = MitigationFundingAmountKind.PROJECT_AMOUNT,
    share_kind: MitigationFundingShareKind = MitigationFundingShareKind.TOTAL,
    lifecycle: MitigationFundingEventType | None = None,
) -> MitigationFundingAmount:
    return MitigationFundingAmount(
        money=_money(amount, currency),
        amount_kind=amount_kind,
        share_kind=share_kind,
        lifecycle=lifecycle,
        source_category=_category(amount_kind.value),
    )


def _organization(
    role: MitigationOrganizationRole = MitigationOrganizationRole.RECIPIENT,
) -> MitigationOrganization:
    return MitigationOrganization(
        role=role,
        name=role.value.replace("_", " ").title(),
        source_role=_category(role.value),
    )


def _geography() -> MitigationProjectGeography:
    return MitigationProjectGeography(
        semantics=MitigationGeographySemantics.MUNICIPALITY,
        address=Address(country="US", region="NY", locality="New York"),
        footprint=SpatialFootprint(point=Coordinate(latitude=40.7128, longitude=-74.0060)),
        place_name="New York",
        administrative_areas=("New York County",),
        source_category=_category("municipality"),
    )


def _project(**overrides: Any) -> HazardMitigationProject:
    defaults: dict[str, Any] = {
        "provenance": _provenance(),
        "project_id": "HMA-P-1",
        "title": _mapped("Flood mitigation project", "projectTitle"),
        "description": _mapped("Elevate flood-prone structures.", "projectDescription"),
        "programme": _mapped(_category("hma"), "programArea", quality=FieldQuality.STANDARDIZED),
        "organizations": _mapped(
            (_organization(MitigationOrganizationRole.RECIPIENT),),
            "recipient",
            quality=FieldQuality.STANDARDIZED,
        ),
        "hazard_types": _mapped(
            (MitigationHazardType.FLOOD,),
            "projectType",
            quality=FieldQuality.STANDARDIZED,
        ),
        "source_hazards": _mapped((_category("flood"),), "projectType"),
        "intervention_types": _mapped(
            (MitigationInterventionType.ELEVATION,),
            "projectType",
            quality=FieldQuality.STANDARDIZED,
        ),
        "source_interventions": _mapped((_category("elevation"),), "projectType"),
        "status": _mapped(
            MitigationProjectStatus.APPROVED,
            "status",
            quality=FieldQuality.STANDARDIZED,
        ),
        "source_status": _mapped(_category("approved"), "status"),
        "approval_period": _mapped(
            _date_period(),
            "dateApproved",
            quality=FieldQuality.STANDARDIZED,
        ),
        "project_period": _mapped(_period(), "programFy", quality=FieldQuality.STANDARDIZED),
        "fiscal_period": _mapped(_period(), "programFy", quality=FieldQuality.STANDARDIZED),
        "publication_period": _mapped(_date_period(), "lastRefresh", quality=FieldQuality.DERIVED),
        "geography": _mapped((_geography(),), "county", "state", quality=FieldQuality.DERIVED),
        "funding_summaries": _mapped(
            (
                _funding_amount(
                    Decimal("250000.00"),
                    amount_kind=MitigationFundingAmountKind.PROJECT_AMOUNT,
                ),
                _funding_amount(
                    Decimal("187500.00"),
                    amount_kind=MitigationFundingAmountKind.PROJECT_AMOUNT,
                    share_kind=MitigationFundingShareKind.FEDERAL,
                    lifecycle=MitigationFundingEventType.OBLIGATION,
                ),
            ),
            "projectAmount",
            "federalShareObligated",
            quality=FieldQuality.STANDARDIZED,
        ),
        "benefit_cost_ratio": _mapped(Decimal("1.42"), "benefitCostRatio"),
        "net_benefits": _mapped(
            _money(Decimal("510000.00")),
            "netValueBenefits",
        ),
        "source_caveats": _mapped((_category("not-official-financial-reporting"),), "metadata"),
    }
    defaults.update(overrides)

    return HazardMitigationProject(**defaults)


def _transaction(**overrides: Any) -> MitigationFundingTransaction:
    defaults: dict[str, Any] = {
        "provenance": _provenance("hma-transaction-1"),
        "transaction_id": "TXN-1",
        "project_id": _mapped("HMA-P-1", "projectIdentifier"),
        "transaction_period": _mapped(
            _date_period(date(2026, 4, 30)),
            "transactionDate",
            quality=FieldQuality.STANDARDIZED,
        ),
        "fiscal_period": _mapped(_period(), "programFy", quality=FieldQuality.STANDARDIZED),
        "funding_programme": _mapped(_category("hmgp"), "fundCode"),
        "event_type": _mapped(None, quality=FieldQuality.UNMAPPED),
        "source_event_category": _mapped(None, quality=FieldQuality.UNMAPPED),
        "amount_components": _mapped(
            (
                _funding_amount(
                    Decimal("12000.00"),
                    amount_kind=MitigationFundingAmountKind.PROJECT_AMOUNT,
                    share_kind=MitigationFundingShareKind.FEDERAL,
                    lifecycle=MitigationFundingEventType.PAYMENT,
                ),
                _funding_amount(
                    Decimal("1500.00"),
                    amount_kind=MitigationFundingAmountKind.ADMINISTRATIVE_COST,
                    share_kind=MitigationFundingShareKind.RECIPIENT,
                ),
            ),
            "federalShareProjectCostAmt",
            "recipientAdminCostAmt",
            quality=FieldQuality.STANDARDIZED,
        ),
        "source_caveats": _mapped((_category("not-official-financial-reporting"),), "metadata"),
    }
    defaults.update(overrides)

    return MitigationFundingTransaction(**defaults)


class TestHazardMitigationProject:
    def test_minimum_valid_project(self) -> None:
        project = _project(
            description=_mapped(None, "projectDescription", quality=FieldQuality.NOT_PROVIDED),
            organizations=_mapped(None, quality=FieldQuality.UNMAPPED),
            source_hazards=_mapped((), "projectType"),
            source_interventions=_mapped((), "projectType"),
            geography=_mapped(None, "county", quality=FieldQuality.NOT_PROVIDED),
            benefit_cost_ratio=_mapped(None, quality=FieldQuality.UNMAPPED),
            net_benefits=_mapped(None, quality=FieldQuality.UNMAPPED),
            source_caveats=_mapped((), "metadata"),
        )

        assert project.project_id == "HMA-P-1"
        assert project.organizations.quality is FieldQuality.UNMAPPED
        assert project.funding_summaries.value is not None

    def test_full_project_record(self) -> None:
        project = _project()

        assert project.status.value is MitigationProjectStatus.APPROVED
        assert project.hazard_types.value == (MitigationHazardType.FLOOD,)
        assert project.intervention_types.value == (MitigationInterventionType.ELEVATION,)
        assert project.benefit_cost_ratio.value == Decimal("1.42")
        assert project.net_benefits.value == _money(Decimal("510000.00"))

    def test_project_funding_summary_does_not_require_transaction(self) -> None:
        project = _project(
            funding_summaries=_mapped(
                (
                    _funding_amount(
                        Decimal("1000000"),
                        currency="CAD",
                        amount_kind=MitigationFundingAmountKind.TOTAL_ELIGIBLE_COST,
                    ),
                ),
                "totalEligibleCost",
            )
        )

        assert project.funding_summaries.value is not None
        assert project.funding_summaries.value[0].money.currency == "CAD"

    def test_fcerm_shaped_project_allows_unmapped_status_and_no_transactions(self) -> None:
        project = _project(
            provenance=_provenance("fcerm-scheme-1"),
            project_id="FCERM-SCHEME-1",
            status=_mapped(None, quality=FieldQuality.UNMAPPED),
            source_status=_mapped(None, quality=FieldQuality.UNMAPPED),
            funding_summaries=_mapped(
                (
                    _funding_amount(
                        Decimal("350.00"),
                        currency="GBP",
                        amount_kind=MitigationFundingAmountKind.PROJECT_AMOUNT,
                        share_kind=MitigationFundingShareKind.GOVERNMENT,
                        lifecycle=MitigationFundingEventType.PLANNED_AMOUNT,
                    ),
                ),
                "Indicative Government Investment 2026/27 (GBP thousands)",
            ),
        )

        assert project.status.value is None
        assert project.funding_summaries.value is not None
        assert project.funding_summaries.value[0].money.currency == "GBP"

    def test_funding_summary_quality_states_are_distinct(self) -> None:
        unmapped = _project(funding_summaries=_mapped(None, quality=FieldQuality.UNMAPPED))
        not_provided = _project(
            funding_summaries=_mapped(None, "amount", quality=FieldQuality.NOT_PROVIDED)
        )
        explicitly_empty = _project(funding_summaries=_mapped((), "amount"))

        assert unmapped.funding_summaries.quality is FieldQuality.UNMAPPED
        assert not_provided.funding_summaries.quality is FieldQuality.NOT_PROVIDED
        assert explicitly_empty.funding_summaries.value == ()

    def test_tuple_quality_states_are_distinct_for_geography_organizations_and_caveats(
        self,
    ) -> None:
        unmapped = _project(
            organizations=_mapped(None, quality=FieldQuality.UNMAPPED),
            geography=_mapped(None, quality=FieldQuality.UNMAPPED),
            source_caveats=_mapped(None, quality=FieldQuality.UNMAPPED),
        )
        not_provided = _project(
            organizations=_mapped(None, "recipient", quality=FieldQuality.NOT_PROVIDED),
            geography=_mapped(None, "county", quality=FieldQuality.NOT_PROVIDED),
            source_caveats=_mapped(None, "metadata", quality=FieldQuality.NOT_PROVIDED),
        )
        explicitly_empty = _project(
            organizations=_mapped((), "recipient"),
            geography=_mapped((), "county"),
            source_caveats=_mapped((), "metadata"),
        )

        assert unmapped.organizations.quality is FieldQuality.UNMAPPED
        assert unmapped.geography.quality is FieldQuality.UNMAPPED
        assert unmapped.source_caveats.quality is FieldQuality.UNMAPPED
        assert not_provided.organizations.quality is FieldQuality.NOT_PROVIDED
        assert not_provided.geography.quality is FieldQuality.NOT_PROVIDED
        assert not_provided.source_caveats.quality is FieldQuality.NOT_PROVIDED
        assert explicitly_empty.organizations.value == ()
        assert explicitly_empty.geography.value == ()
        assert explicitly_empty.source_caveats.value == ()

    def test_frozen(self) -> None:
        project = _project()

        with pytest.raises(ValidationError):
            project.project_id = "HMA-P-2"  # type: ignore[misc]

    def test_extra_fields_rejected(self) -> None:
        with pytest.raises(ValidationError):
            HazardMitigationProject.model_validate(
                {**_project().model_dump(), "source_project_identifier": "source-field-name"}
            )

    def test_empty_project_id_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _project(project_id="")

    def test_project_id_with_surrounding_whitespace_rejected(self) -> None:
        with pytest.raises(ValidationError, match="whitespace"):
            _project(project_id=" HMA-P-1")

    def test_invalid_status_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _project(
                status=MappedField[MitigationProjectStatus].model_validate(
                    {
                        "value": "funded",
                        "quality": "standardized",
                        "source_fields": ("status",),
                    }
                )
            )


class TestMitigationFundingTransaction:
    def test_transaction_with_multiple_amount_components(self) -> None:
        transaction = _transaction()

        assert transaction.transaction_id == "TXN-1"
        assert transaction.project_id.value == "HMA-P-1"
        assert transaction.event_type.quality is FieldQuality.UNMAPPED
        assert len(transaction.amount_components.value or ()) == 2

    def test_signed_reversal_refund_adjustment_and_deobligation_amounts(self) -> None:
        transaction = _transaction(
            amount_components=_mapped(
                (
                    _funding_amount(
                        Decimal("-10.00"),
                        amount_kind=MitigationFundingAmountKind.PROJECT_AMOUNT,
                        lifecycle=MitigationFundingEventType.REVERSAL,
                    ),
                    _funding_amount(
                        Decimal("-20.00"),
                        amount_kind=MitigationFundingAmountKind.PROJECT_AMOUNT,
                        lifecycle=MitigationFundingEventType.REFUND,
                    ),
                    _funding_amount(
                        Decimal("-30.00"),
                        amount_kind=MitigationFundingAmountKind.PROJECT_AMOUNT,
                        lifecycle=MitigationFundingEventType.ADJUSTMENT,
                    ),
                    _funding_amount(
                        Decimal("-40.00"),
                        amount_kind=MitigationFundingAmountKind.PROJECT_AMOUNT,
                        lifecycle=MitigationFundingEventType.DEOBLIGATION,
                    ),
                ),
                "federalShareProjectCostAmt",
            ),
        )

        assert transaction.amount_components.value is not None
        assert [component.money.amount for component in transaction.amount_components.value] == [
            Decimal("-10.00"),
            Decimal("-20.00"),
            Decimal("-30.00"),
            Decimal("-40.00"),
        ]
        assert [component.lifecycle for component in transaction.amount_components.value] == [
            MitigationFundingEventType.REVERSAL,
            MitigationFundingEventType.REFUND,
            MitigationFundingEventType.ADJUSTMENT,
            MitigationFundingEventType.DEOBLIGATION,
        ]

    def test_empty_transaction_id_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _transaction(transaction_id="")

    def test_whitespace_only_stable_transaction_identifier_rejected(self) -> None:
        with pytest.raises(ValidationError, match="whitespace"):
            _transaction(transaction_id=" ")

    def test_unmapped_transaction_event_type(self) -> None:
        transaction = _transaction(
            event_type=_mapped(None, quality=FieldQuality.UNMAPPED),
            source_event_category=_mapped(None, quality=FieldQuality.UNMAPPED),
        )

        assert transaction.event_type.value is None
        assert transaction.source_event_category.quality is FieldQuality.UNMAPPED


class TestMitigationValueModels:
    def test_usd_cad_and_gbp_currency_shapes(self) -> None:
        assert _money(Decimal("1"), "USD").currency == "USD"
        assert _money(Decimal("1"), "CAD").currency == "CAD"
        assert _money(Decimal("1"), "GBP").currency == "GBP"

    def test_other_uppercase_three_letter_currency_shape_is_allowed(self) -> None:
        assert _money(Decimal("1"), "EUR").currency == "EUR"

    def test_invalid_currency_shapes_rejected(self) -> None:
        for currency in ("usd", "US", "USDD", "12D"):
            with pytest.raises(ValidationError):
                _money(Decimal("1"), currency)

    def test_non_decimal_amount_rejected_by_strict_model(self) -> None:
        with pytest.raises(ValidationError):
            MitigationMoneyAmount.model_validate({"amount": "1.00", "currency": "USD"})

    def test_non_finite_amount_rejected(self) -> None:
        with pytest.raises(ValidationError, match="finite"):
            _money(Decimal("NaN"), "USD")

    def test_geography_allows_no_descriptor_when_wrapped_quality_explains_absence(self) -> None:
        geography = MitigationProjectGeography(
            semantics=MitigationGeographySemantics.NON_SPATIAL_PROGRAMME
        )

        field = _mapped(
            (geography,),
            "programme",
            quality=FieldQuality.STANDARDIZED,
        )

        assert field.value == (geography,)

    def test_empty_organization_name_rejected(self) -> None:
        with pytest.raises(ValidationError):
            MitigationOrganization(role=MitigationOrganizationRole.RECIPIENT, name="")
