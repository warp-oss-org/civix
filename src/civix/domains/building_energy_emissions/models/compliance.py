"""Building compliance case models."""

from decimal import Decimal
from typing import Annotated

from pydantic import BaseModel, Field

from civix.core.provenance.models.provenance import ProvenanceRef
from civix.core.quality.models.fields import MappedField
from civix.core.taxonomy.models.category import CategoryRef
from civix.core.temporal import TemporalPeriod
from civix.domains.building_energy_emissions.models.common import (
    EMPTY_TUPLE_FIELD_DESCRIPTION,
    FROZEN_MODEL,
    ComplianceLifecycleStatus,
    SourceIdentifier,
)
from civix.domains.building_energy_emissions.models.keys import (
    BuildingComplianceCaseKey,
    BuildingEnergyReportKey,
    BuildingEnergySubjectKey,
    BuildingMetricValueKey,
)

NonNegativeDecimal = Annotated[Decimal, Field(ge=Decimal("0"))]


class BuildingComplianceCase(BaseModel):
    """One source-published compliance, exemption, or penalty case.

    Compliance cases have a distinct row grain from energy reports: the
    same subject can have multiple cases per period (covered-building
    dispute, extension, Article 320 report, Article 321 report, penalty
    mitigation), and compliance is BIN-centered for sources like LL97
    while reports are property-centered.

    Numeric facts (emissions limits, final emissions, excess emissions)
    are referenced as `BuildingMetricValue` records rather than embedded,
    so unit and methodology are preserved in one place. Those metric
    records may attach directly to this case via `case_key` when no
    paired benchmarking report exists.
    """

    model_config = FROZEN_MODEL

    provenance: ProvenanceRef
    case_key: BuildingComplianceCaseKey
    subject_key: BuildingEnergySubjectKey
    related_report_key: MappedField[BuildingEnergyReportKey]
    source_case_identifiers: MappedField[tuple[SourceIdentifier, ...]] = Field(
        description=EMPTY_TUPLE_FIELD_DESCRIPTION
    )
    covered_period: MappedField[TemporalPeriod]
    filing_period: MappedField[TemporalPeriod]
    covered_building_status: MappedField[ComplianceLifecycleStatus]
    source_covered_status: MappedField[CategoryRef]
    compliance_pathway: MappedField[CategoryRef]
    compliance_status: MappedField[ComplianceLifecycleStatus]
    source_compliance_status: MappedField[CategoryRef]
    exemption_status: MappedField[ComplianceLifecycleStatus]
    extension_status: MappedField[ComplianceLifecycleStatus]
    emissions_limit_metric_key: MappedField[BuildingMetricValueKey]
    final_emissions_metric_key: MappedField[BuildingMetricValueKey]
    excess_emissions_metric_key: MappedField[BuildingMetricValueKey]
    penalty_amount: MappedField[NonNegativeDecimal]
    penalty_currency: MappedField[CategoryRef]
    penalty_status: MappedField[ComplianceLifecycleStatus]
    dispute_status: MappedField[ComplianceLifecycleStatus]
    source_caveats: MappedField[tuple[CategoryRef, ...]] = Field(
        description=EMPTY_TUPLE_FIELD_DESCRIPTION
    )
