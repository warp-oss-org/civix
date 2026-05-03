"""Building metric value models with unit and methodology preserved."""

from decimal import Decimal
from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from civix.core.provenance.models.provenance import ProvenanceRef
from civix.core.quality.models.fields import FieldQuality, MappedField
from civix.core.taxonomy.models.category import CategoryRef
from civix.core.temporal import TemporalPeriod
from civix.domains.building_energy_emissions.models.common import (
    EMPTY_TUPLE_FIELD_DESCRIPTION,
    FROZEN_MODEL,
    EmissionsMetricType,
    EnergyMetricType,
    MetricFamily,
    MetricValueSource,
    NonEmptyString,
    SourceValueState,
    WaterMetricType,
)
from civix.domains.building_energy_emissions.models.keys import (
    BuildingComplianceCaseKey,
    BuildingEnergyReportKey,
    BuildingEnergySubjectKey,
    BuildingMetricValueKey,
)


class NumericMetricMeasure(BaseModel):
    """A numeric performance metric value."""

    model_config = FROZEN_MODEL

    kind: Literal["numeric"] = "numeric"
    value: Decimal


class CategoryMetricMeasure(BaseModel):
    """A taxonomy-backed performance metric value (e.g. ENERGY STAR rating)."""

    model_config = FROZEN_MODEL

    kind: Literal["category"] = "category"
    value: CategoryRef


class TextMetricMeasure(BaseModel):
    """A source-published metric value not yet taxonomy-backed.

    Prefer `CategoryMetricMeasure` when a stable source taxonomy
    reference can be constructed. This variant preserves source-published
    text values such as benchmarking comments before such a taxonomy
    exists.
    """

    model_config = FROZEN_MODEL

    kind: Literal["text"] = "text"
    value: NonEmptyString

    @field_validator("value")
    @classmethod
    def _no_surrounding_whitespace(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("text metric measure must not have surrounding whitespace")

        return value


MetricMeasure = Annotated[
    NumericMetricMeasure | CategoryMetricMeasure | TextMetricMeasure,
    Field(discriminator="kind"),
]


_NEGATIVE_NUMERIC_ALLOWED_ENERGY: frozenset[EnergyMetricType] = frozenset()
_NEGATIVE_NUMERIC_ALLOWED_EMISSIONS: frozenset[EmissionsMetricType] = frozenset(
    {
        EmissionsMetricType.AVOIDED_EMISSIONS,
        EmissionsMetricType.EMISSIONS_OVERAGE,
        EmissionsMetricType.NET_GHG,
        EmissionsMetricType.OFFSETS,
    }
)
_NEGATIVE_NUMERIC_ALLOWED_WATER: frozenset[WaterMetricType] = frozenset()
_PARENT_UNPOPULATED_QUALITIES = frozenset(
    {FieldQuality.UNMAPPED, FieldQuality.NOT_PROVIDED, FieldQuality.REDACTED}
)


class BuildingMetricValue(BaseModel):
    """One source-published metric value attached to a report or case.

    Each metric carries its own value, unit, denominator, normalization,
    fuel/scope qualifier, and methodology version so that source EUI,
    weather-normalized EUI, location-based GHG, market-based GHG, LL97
    final emissions, and Vancouver thermal-energy GHGI cannot be confused
    with each other.

    Each metric attaches to exactly one parent: a `BuildingEnergyReport`
    via `report_key`, or a `BuildingComplianceCase` via `case_key`.
    Compliance-only metrics such as LL97 emissions limits and excess
    emissions attach to the case directly when no paired benchmarking
    report row exists.

    `value_source` records whether the value is source-published or
    mapper-derived. Mappers should prefer source-published values and
    only emit `MAPPER_DERIVED` when the source publishes derivation
    inputs and the derivation is documented.
    """

    model_config = FROZEN_MODEL

    provenance: ProvenanceRef
    metric_key: BuildingMetricValueKey
    report_key: MappedField[BuildingEnergyReportKey]
    case_key: MappedField[BuildingComplianceCaseKey]
    subject_key: BuildingEnergySubjectKey
    metric_family: MetricFamily
    energy_metric_type: MappedField[EnergyMetricType]
    emissions_metric_type: MappedField[EmissionsMetricType]
    water_metric_type: MappedField[WaterMetricType]
    source_metric_label: MappedField[CategoryRef]
    measure: MappedField[MetricMeasure]
    value_state: MappedField[SourceValueState]
    unit: MappedField[CategoryRef]
    denominator: MappedField[CategoryRef]
    normalization: MappedField[CategoryRef]
    fuel_or_scope: MappedField[CategoryRef]
    methodology_label: MappedField[NonEmptyString]
    methodology_version: MappedField[NonEmptyString]
    methodology_url: MappedField[NonEmptyString]
    emission_factor_version: MappedField[NonEmptyString]
    value_source: MappedField[MetricValueSource]
    effective_period: MappedField[TemporalPeriod]
    source_caveats: MappedField[tuple[CategoryRef, ...]] = Field(
        description=EMPTY_TUPLE_FIELD_DESCRIPTION
    )

    @model_validator(mode="after")
    def _validate(self) -> "BuildingMetricValue":
        self._check_parent_attachment()
        self._check_family_slot_consistency()
        self._check_numeric_sign()

        return self

    def _check_parent_attachment(self) -> None:
        report_populated = self.report_key.quality not in _PARENT_UNPOPULATED_QUALITIES
        case_populated = self.case_key.quality not in _PARENT_UNPOPULATED_QUALITIES

        if report_populated and case_populated:
            raise ValueError(
                "BuildingMetricValue must attach to a report or a case, not both"
            )

        if not report_populated and not case_populated:
            raise ValueError(
                "BuildingMetricValue must attach to either a report or a case"
            )

    def _check_family_slot_consistency(self) -> None:
        family = self.metric_family
        slots = {
            MetricFamily.ENERGY: self.energy_metric_type,
            MetricFamily.EMISSIONS: self.emissions_metric_type,
            MetricFamily.WATER: self.water_metric_type,
        }

        for slot_family, slot in slots.items():
            populated = slot.quality not in {
                FieldQuality.UNMAPPED,
                FieldQuality.NOT_PROVIDED,
                FieldQuality.REDACTED,
            }

            if slot_family == family and not populated:
                raise ValueError(
                    f"metric_family={family.value!r} requires the matching typed metric type"
                )

            if slot_family != family and populated:
                raise ValueError(
                    f"metric_family={family.value!r} forbids a populated "
                    f"{slot_family.value} metric type slot"
                )

    def _check_numeric_sign(self) -> None:
        measure = self.measure.value

        if not isinstance(measure, NumericMetricMeasure):
            return

        if measure.value >= 0:
            return

        family = self.metric_family

        if family is MetricFamily.ENERGY:
            allowed = _NEGATIVE_NUMERIC_ALLOWED_ENERGY
            metric_type = self.energy_metric_type.value
        elif family is MetricFamily.EMISSIONS:
            allowed = _NEGATIVE_NUMERIC_ALLOWED_EMISSIONS
            metric_type = self.emissions_metric_type.value
        elif family is MetricFamily.WATER:
            allowed = _NEGATIVE_NUMERIC_ALLOWED_WATER
            metric_type = self.water_metric_type.value
        else:
            return

        if metric_type not in allowed:
            raise ValueError(
                f"numeric metric value must be non-negative for metric_family={family.value!r}"
            )
