"""Building energy report models."""

from datetime import date

from pydantic import BaseModel, Field

from civix.core.provenance.models.provenance import ProvenanceRef
from civix.core.quality.models.fields import MappedField
from civix.core.taxonomy.models.category import CategoryRef
from civix.core.temporal import TemporalPeriod
from civix.domains.building_energy_emissions.models.common import (
    EMPTY_TUPLE_FIELD_DESCRIPTION,
    FROZEN_MODEL,
    ReportingPeriodPrecision,
    SourceIdentifier,
)
from civix.domains.building_energy_emissions.models.keys import (
    BuildingEnergyReportKey,
    BuildingEnergySubjectKey,
)


class BuildingEnergyReport(BaseModel):
    """One source-published periodic energy and benchmarking report row.

    A report is the header record for a reporting period; metric values
    live on `BuildingMetricValue` records that reference this report via
    `report_key`. Compliance lifecycle lives on `BuildingComplianceCase`
    records, and compliance-only metrics (LL97 emissions limits, excess
    emissions) attach to a case rather than to a report.

    `subject_key` is required (not a `MappedField`) because a benchmarking
    row without an identifiable subject is meaningless: every confirmed
    source publishes at least one identifier per row.
    """

    model_config = FROZEN_MODEL

    provenance: ProvenanceRef
    report_key: BuildingEnergyReportKey
    subject_key: BuildingEnergySubjectKey
    source_report_identifiers: MappedField[tuple[SourceIdentifier, ...]] = Field(
        description=EMPTY_TUPLE_FIELD_DESCRIPTION
    )
    reporting_period: MappedField[TemporalPeriod]
    reporting_period_precision: MappedField[ReportingPeriodPrecision]
    report_submission_date: MappedField[date]
    report_generation_date: MappedField[date]
    report_status: MappedField[CategoryRef]
    data_quality_caveats: MappedField[tuple[CategoryRef, ...]] = Field(
        description=EMPTY_TUPLE_FIELD_DESCRIPTION
    )
    source_caveats: MappedField[tuple[CategoryRef, ...]] = Field(
        description=EMPTY_TUPLE_FIELD_DESCRIPTION
    )
