"""Shared hazard mitigation model primitives."""

from enum import StrEnum
from typing import Annotated

from pydantic import ConfigDict, Field

FROZEN_MODEL = ConfigDict(frozen=True, extra="forbid", strict=True)
EMPTY_TUPLE_FIELD_DESCRIPTION = (
    "UNMAPPED means no source field or mapper support, NOT_PROVIDED means source fields "
    "exist but are blank, and an empty tuple with provided quality means the source explicitly "
    "reported no values."
)

NonEmptyString = Annotated[str, Field(min_length=1)]
CurrencyCode = Annotated[str, Field(pattern=r"^[A-Z]{3}$")]


class MitigationHazardType(StrEnum):
    """Portable hazard or risk source addressed by mitigation work."""

    FLOOD = "flood"
    COASTAL_EROSION = "coastal_erosion"
    WILDFIRE = "wildfire"
    STORMWATER = "stormwater"
    HEAT = "heat"
    DROUGHT = "drought"
    EARTHQUAKE = "earthquake"
    LANDSLIDE = "landslide"
    SEA_LEVEL_RISE = "sea_level_rise"
    MULTI_HAZARD = "multi_hazard"
    SOURCE_SPECIFIC = "source_specific"
    UNKNOWN = "unknown"


class MitigationInterventionType(StrEnum):
    """Portable mitigation intervention or activity type."""

    PROPERTY_ACQUISITION = "property_acquisition"
    ELEVATION = "elevation"
    FLOOD_DEFENCE = "flood_defence"
    DRAINAGE = "drainage"
    GREEN_INFRASTRUCTURE = "green_infrastructure"
    STORMWATER = "stormwater"
    WILDFIRE_FUEL_TREATMENT = "wildfire_fuel_treatment"
    PLANNING = "planning"
    INFRASTRUCTURE_HARDENING = "infrastructure_hardening"
    SOURCE_SPECIFIC = "source_specific"
    UNKNOWN = "unknown"


class MitigationProjectStatus(StrEnum):
    """Portable lifecycle state for mitigation project records."""

    PLANNED = "planned"
    PENDING = "pending"
    APPROVED = "approved"
    ACTIVE = "active"
    IN_PROGRESS = "in_progress"
    COMPLETE = "complete"
    CLOSED = "closed"
    CANCELLED = "cancelled"
    SOURCE_SPECIFIC = "source_specific"
    UNKNOWN = "unknown"


class MitigationOrganizationRole(StrEnum):
    """Portable role for an organization named on a mitigation record."""

    APPLICANT = "applicant"
    SUBAPPLICANT = "subapplicant"
    RECIPIENT = "recipient"
    SUBRECIPIENT = "subrecipient"
    SPONSOR = "sponsor"
    AUTHORITY = "authority"
    LEAD_AUTHORITY = "lead_authority"
    DELIVERY_ORGANIZATION = "delivery_organization"
    SOURCE_SPECIFIC = "source_specific"
    UNKNOWN = "unknown"


class MitigationGeographySemantics(StrEnum):
    """What a source-published geography describes."""

    ADDRESS = "address"
    COORDINATE = "coordinate"
    JURISDICTION = "jurisdiction"
    COUNTY = "county"
    MUNICIPALITY = "municipality"
    STATE_OR_PROVINCE = "state_or_province"
    REGION = "region"
    WATERSHED = "watershed"
    RIVER_BASIN = "river_basin"
    PROJECT_AREA = "project_area"
    ASSET_SITE = "asset_site"
    MULTIPLE_PARCELS = "multiple_parcels"
    NON_SPATIAL_PROGRAMME = "non_spatial_programme"
    SOURCE_SPECIFIC = "source_specific"
    UNKNOWN = "unknown"


class MitigationFundingAmountKind(StrEnum):
    """Cost bucket or accounting component for a published funding amount.

    Lifecycle semantics such as obligation, payment, deobligation, refund,
    reversal, adjustment, and planned amount belong on `MitigationFundingEventType`.
    """

    TOTAL_PROJECT_COST = "total_project_cost"
    TOTAL_ELIGIBLE_COST = "total_eligible_cost"
    PROJECT_AMOUNT = "project_amount"
    ADMINISTRATIVE_COST = "administrative_cost"
    MANAGEMENT_COST = "management_cost"
    SOURCE_SPECIFIC = "source_specific"
    UNKNOWN = "unknown"


class MitigationFundingShareKind(StrEnum):
    """Whose share or accounting component a funding amount represents."""

    FEDERAL = "federal"
    RECIPIENT = "recipient"
    SUBRECIPIENT = "subrecipient"
    LOCAL_MATCH = "local_match"
    GOVERNMENT = "government"
    NON_FEDERAL = "non_federal"
    TOTAL = "total"
    SOURCE_SPECIFIC = "source_specific"
    UNKNOWN = "unknown"


class MitigationFundingEventType(StrEnum):
    """Portable event type for true transaction-like funding records."""

    AWARD = "award"
    OBLIGATION = "obligation"
    PAYMENT = "payment"
    DEOBLIGATION = "deobligation"
    REFUND = "refund"
    REVERSAL = "reversal"
    ADJUSTMENT = "adjustment"
    PLANNED_AMOUNT = "planned_amount"
    SOURCE_SPECIFIC = "source_specific"
    UNKNOWN = "unknown"
