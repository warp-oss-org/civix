"""Shared building energy and emissions model primitives."""

from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator

from civix.core.taxonomy.models.category import CategoryRef

FROZEN_MODEL = ConfigDict(frozen=True, extra="forbid", strict=True)
EMPTY_TUPLE_FIELD_DESCRIPTION = (
    "UNMAPPED means no source field or mapper support, NOT_PROVIDED means source fields "
    "exist but are blank, and an empty tuple with provided quality means the source explicitly "
    "reported no values."
)

NonEmptyString = Annotated[str, Field(min_length=1)]


class BuildingSubjectKind(StrEnum):
    """Portable identity grain for a source-published building energy subject.

    Sources publish reporting rows keyed to a Portfolio Manager property,
    a single physical building, a tax lot, a campus or parent property,
    or a portfolio-level reporting account. These grains are not
    interchangeable and must not collapse into a single building concept.
    """

    REPORTING_ACCOUNT = "reporting_account"
    PROPERTY = "property"
    BUILDING = "building"
    TAX_LOT = "tax_lot"
    CAMPUS = "campus"
    PORTFOLIO_PROPERTY = "portfolio_property"
    SOURCE_SPECIFIC = "source_specific"
    UNKNOWN = "unknown"


class IdentityCertainty(StrEnum):
    """How stable a source-published subject identifier is across reports."""

    STABLE_CROSS_YEAR = "stable_cross_year"
    STABLE_WITHIN_YEAR = "stable_within_year"
    REASSIGNED = "reassigned"
    AMBIGUOUS = "ambiguous"
    UNKNOWN = "unknown"


class ReportingPeriodPrecision(StrEnum):
    """Precision of a source-published reporting period."""

    CALENDAR_YEAR = "calendar_year"
    FISCAL_YEAR = "fiscal_year"
    PARTIAL_YEAR = "partial_year"
    MULTI_YEAR = "multi_year"
    UNKNOWN = "unknown"


class MetricFamily(StrEnum):
    """Top-level family of a building performance metric.

    Drives which typed metric-type slot is required on a metric record.
    SOURCE_SPECIFIC and UNKNOWN allow a record without a typed slot when
    the source publishes a value the engine cannot place in the portable
    taxonomy yet.
    """

    ENERGY = "energy"
    EMISSIONS = "emissions"
    WATER = "water"
    SOURCE_SPECIFIC = "source_specific"
    UNKNOWN = "unknown"


class EnergyMetricType(StrEnum):
    """Portable energy metric kinds.

    Site/source EUI, weather-normalized variants, fuel-specific use, and
    benchmarking scores must remain distinct rather than being collapsed
    into a single energy concept.
    """

    SITE_ENERGY = "site_energy"
    SOURCE_ENERGY = "source_energy"
    SITE_EUI = "site_eui"
    SOURCE_EUI = "source_eui"
    WEATHER_NORMALIZED_SITE_EUI = "weather_normalized_site_eui"
    WEATHER_NORMALIZED_SOURCE_EUI = "weather_normalized_source_eui"
    ELECTRICITY_USE = "electricity_use"
    NATURAL_GAS_USE = "natural_gas_use"
    DISTRICT_STEAM_USE = "district_steam_use"
    DISTRICT_HOT_WATER_USE = "district_hot_water_use"
    DISTRICT_CHILLED_WATER_USE = "district_chilled_water_use"
    FUEL_OIL_USE = "fuel_oil_use"
    OTHER_FUEL_USE = "other_fuel_use"
    RENEWABLE_ENERGY = "renewable_energy"
    ENERGY_STAR_SCORE = "energy_star_score"
    SOURCE_SPECIFIC = "source_specific"
    UNKNOWN = "unknown"


class EmissionsMetricType(StrEnum):
    """Portable emissions metric kinds.

    Limits, overages, and avoided emissions are distinct from reported
    totals and must not be confused with annual emissions metrics.
    """

    TOTAL_GHG = "total_ghg"
    DIRECT_GHG = "direct_ghg"
    INDIRECT_GHG = "indirect_ghg"
    LOCATION_BASED_GHG = "location_based_ghg"
    MARKET_BASED_GHG = "market_based_ghg"
    NET_GHG = "net_ghg"
    AVOIDED_EMISSIONS = "avoided_emissions"
    EMISSIONS_INTENSITY = "emissions_intensity"
    EMISSIONS_LIMIT = "emissions_limit"
    EMISSIONS_OVERAGE = "emissions_overage"
    OFFSETS = "offsets"
    SOURCE_SPECIFIC = "source_specific"
    UNKNOWN = "unknown"


class WaterMetricType(StrEnum):
    """Portable water metric kinds."""

    WATER_USE = "water_use"
    WATER_USE_INTENSITY = "water_use_intensity"
    INDOOR_WATER_USE = "indoor_water_use"
    OUTDOOR_WATER_USE = "outdoor_water_use"
    SOURCE_SPECIFIC = "source_specific"
    UNKNOWN = "unknown"


class SourceValueState(StrEnum):
    """Source-published state for a metric value alongside FieldQuality.

    Sources publish textual sentinels such as `Not Available`,
    `Not Applicable`, `Unable to Check`, `Possible Issue`, and `Ok` that
    convey source intent distinct from mapper-side missingness. This
    enum preserves that intent so it does not collapse into the engine's
    quality enum.
    """

    REPORTED = "reported"
    NOT_AVAILABLE = "not_available"
    NOT_APPLICABLE = "not_applicable"
    WITHHELD = "withheld"
    SUPPRESSED = "suppressed"
    PARTIALLY_DISCLOSED = "partially_disclosed"
    ESTIMATED = "estimated"
    FLAGGED_QUALITY_ISSUE = "flagged_quality_issue"
    UNABLE_TO_CHECK = "unable_to_check"
    OK = "ok"
    SOURCE_SPECIFIC = "source_specific"
    UNKNOWN = "unknown"


class MetricValueSource(StrEnum):
    """Where a metric value originated.

    SOURCE_PUBLISHED is the source's own published number, which is
    almost always preferred. MAPPER_DERIVED is only legitimate when the
    source publishes derivation inputs and the derivation is documented;
    mappers must not silently recompute totals. SOURCE_REPUBLISHED
    covers values a source restates from another publisher (e.g. ESPM
    values surfaced by a city dataset) when that distinction is needed.
    """

    SOURCE_PUBLISHED = "source_published"
    MAPPER_DERIVED = "mapper_derived"
    SOURCE_REPUBLISHED = "source_republished"
    UNKNOWN = "unknown"


class ComplianceLifecycleStatus(StrEnum):
    """Portable compliance lifecycle states for a building compliance case.

    Source-specific labels remain in `CategoryRef` taxonomy fields. This
    enum is the minimum portable vocabulary needed to compare compliance
    cases across jurisdictions without inventing source labels.
    """

    COVERED = "covered"
    NOT_COVERED = "not_covered"
    EXEMPT = "exempt"
    EXTENSION_GRANTED = "extension_granted"
    REPORT_RECEIVED = "report_received"
    REPORT_OUTSTANDING = "report_outstanding"
    IN_DISPUTE = "in_dispute"
    NON_COMPLIANT = "non_compliant"
    COMPLIANT = "compliant"
    PENALTY_ASSESSED = "penalty_assessed"
    PENALTY_MITIGATED = "penalty_mitigated"
    NOT_APPLICABLE = "not_applicable"
    SOURCE_SPECIFIC = "source_specific"
    UNKNOWN = "unknown"


class SourceIdentifier(BaseModel):
    """A source-published identifier preserved separately from Civix keys."""

    model_config = FROZEN_MODEL

    value: NonEmptyString
    identifier_kind: CategoryRef | None = None

    @field_validator("value")
    @classmethod
    def _no_surrounding_whitespace(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("source identifier value must not have surrounding whitespace")

        return value
