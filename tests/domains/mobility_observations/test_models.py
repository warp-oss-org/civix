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
from civix.core.spatial.models.geometry import BoundingBox, LineString, SpatialFootprint
from civix.core.spatial.models.location import Address, Coordinate
from civix.core.taxonomy.models.category import CategoryRef
from civix.core.temporal import TemporalPeriod, TemporalPeriodPrecision, TemporalTimezoneStatus
from civix.domains.mobility_observations.models.common import (
    AggregationWindow,
    CountMetricType,
    CountUnit,
    MeasurementMethod,
    MobilitySiteKind,
    MovementType,
    ObservationDirection,
    SpeedMetricType,
    SpeedUnit,
    TravelMode,
)
from civix.domains.mobility_observations.models.count import MobilityCountObservation
from civix.domains.mobility_observations.models.site import MobilityObservationSite
from civix.domains.mobility_observations.models.speed import (
    MobilitySpeedMetric,
    MobilitySpeedObservation,
)


def _mapped[T](
    value: T | None,
    *source_fields: str,
    quality: FieldQuality = FieldQuality.DIRECT,
) -> MappedField[T]:
    return MappedField[T](value=value, quality=quality, source_fields=source_fields)


def _provenance(source_record_id: str = "mobility-1") -> ProvenanceRef:
    return ProvenanceRef(
        snapshot_id=SnapshotId("snap-mobility-1"),
        source_id=SourceId("mobility-test-source"),
        dataset_id=DatasetId("mobility-observations"),
        jurisdiction=Jurisdiction(country="US", region="NY", locality="New York"),
        fetched_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
        mapper=MapperVersion(
            mapper_id=MapperId("mobility-test-mapper"),
            version="0.1.0",
        ),
        source_record_id=source_record_id,
    )


def _category(code: str = "published-status") -> CategoryRef:
    return CategoryRef(
        code=code,
        label=code.replace("-", " ").title(),
        taxonomy_id="civix.mobility-observations.test",
        taxonomy_version="2026-05-01",
    )


def _period() -> TemporalPeriod:
    return TemporalPeriod(
        precision=TemporalPeriodPrecision.DATE_HOUR,
        date_value=datetime(2026, 5, 1, tzinfo=UTC).date(),
        hour_value=8,
        timezone_status=TemporalTimezoneStatus.NAMED_LOCAL,
        timezone="America/New_York",
    )


def _footprint() -> SpatialFootprint:
    return SpatialFootprint(point=Coordinate(latitude=40.7128, longitude=-74.0060))


def _site(**overrides: Any) -> MobilityObservationSite:
    defaults: dict[str, Any] = {
        "provenance": _provenance("site-1"),
        "site_id": "site-1",
        "kind": _mapped(
            MobilitySiteKind.ROAD_SEGMENT,
            "site_kind",
            quality=FieldQuality.STANDARDIZED,
        ),
        "footprint": _mapped(_footprint(), "geometry", quality=FieldQuality.STANDARDIZED),
        "address": _mapped(Address(country="US", region="NY", locality="New York"), "boro"),
        "road_names": _mapped(("Broadway",), "street"),
        "direction": _mapped(
            ObservationDirection.NORTHBOUND,
            "direction",
            quality=FieldQuality.STANDARDIZED,
        ),
        "movement_type": _mapped(
            MovementType.THROUGH,
            "movement",
            quality=FieldQuality.STANDARDIZED,
        ),
        "active_period": _mapped(_period(), "active_start", quality=FieldQuality.DERIVED),
        "measurement_method": _mapped(
            MeasurementMethod.AUTOMATED_COUNTER,
            "method",
            quality=FieldQuality.STANDARDIZED,
        ),
        "source_caveats": _mapped((_category(),), "status", quality=FieldQuality.STANDARDIZED),
    }
    defaults.update(overrides)

    return MobilityObservationSite(**defaults)


def _count(**overrides: Any) -> MobilityCountObservation:
    defaults: dict[str, Any] = {
        "provenance": _provenance("count-1"),
        "observation_id": "count-1",
        "site_id": "site-1",
        "period": _mapped(_period(), "count_timestamp", quality=FieldQuality.DERIVED),
        "travel_mode": _mapped(
            TravelMode.VEHICLE,
            "mode",
            quality=FieldQuality.STANDARDIZED,
        ),
        "direction": _mapped(
            ObservationDirection.NORTHBOUND,
            "direction",
            quality=FieldQuality.STANDARDIZED,
        ),
        "movement_type": _mapped(
            MovementType.THROUGH,
            "movement",
            quality=FieldQuality.STANDARDIZED,
        ),
        "measurement_method": _mapped(
            MeasurementMethod.AUTOMATED_COUNTER,
            "method",
            quality=FieldQuality.STANDARDIZED,
        ),
        "aggregation_window": _mapped(
            AggregationWindow.HOURLY,
            "granularity",
            quality=FieldQuality.STANDARDIZED,
        ),
        "metric_type": _mapped(
            CountMetricType.RAW_COUNT,
            "metric_type",
            quality=FieldQuality.STANDARDIZED,
        ),
        "unit": _mapped(CountUnit.COUNT, "unit", quality=FieldQuality.STANDARDIZED),
        "value": _mapped(Decimal("42"), "volume"),
        "source_caveats": _mapped((), "status"),
    }
    defaults.update(overrides)

    return MobilityCountObservation(**defaults)


def _speed_metric(**overrides: Any) -> MobilitySpeedMetric:
    defaults: dict[str, Any] = {
        "metric_type": _mapped(
            SpeedMetricType.OBSERVED_SPEED,
            "speed",
            quality=FieldQuality.STANDARDIZED,
        ),
        "unit": _mapped(
            SpeedUnit.MILES_PER_HOUR,
            "speed",
            quality=FieldQuality.STANDARDIZED,
        ),
        "value": _mapped(Decimal("17.5"), "speed"),
    }
    defaults.update(overrides)

    return MobilitySpeedMetric(**defaults)


def _speed(**overrides: Any) -> MobilitySpeedObservation:
    defaults: dict[str, Any] = {
        "provenance": _provenance("speed-1"),
        "observation_id": "speed-1",
        "site_id": "link-1",
        "period": _mapped(_period(), "data_as_of", quality=FieldQuality.STANDARDIZED),
        "travel_mode": _mapped(
            TravelMode.MIXED_TRAFFIC,
            "mode",
            quality=FieldQuality.STANDARDIZED,
        ),
        "direction": _mapped(
            ObservationDirection.SOURCE_SPECIFIC,
            "link_name",
            quality=FieldQuality.INFERRED,
        ),
        "movement_type": _mapped(
            MovementType.THROUGH,
            "link_name",
            quality=FieldQuality.INFERRED,
        ),
        "measurement_method": _mapped(
            MeasurementMethod.SENSOR_FEED,
            "data_as_of",
            quality=FieldQuality.INFERRED,
        ),
        "aggregation_window": _mapped(
            AggregationWindow.RAW_INTERVAL,
            "data_as_of",
            quality=FieldQuality.INFERRED,
        ),
        "metrics": (_speed_metric(),),
        "source_caveats": _mapped(None, quality=FieldQuality.UNMAPPED),
    }
    defaults.update(overrides)

    return MobilitySpeedObservation(**defaults)


class TestMobilityObservationSite:
    def test_minimum_valid_site(self) -> None:
        site = _site(
            address=_mapped(None, "address", quality=FieldQuality.NOT_PROVIDED),
            road_names=_mapped(None, quality=FieldQuality.UNMAPPED),
            direction=_mapped(None, quality=FieldQuality.UNMAPPED),
            movement_type=_mapped(None, quality=FieldQuality.UNMAPPED),
            active_period=_mapped(None, "active_start", quality=FieldQuality.NOT_PROVIDED),
            source_caveats=_mapped((), "status"),
        )

        assert site.site_id == "site-1"
        assert site.road_names.quality is FieldQuality.UNMAPPED

    def test_full_site(self) -> None:
        site = _site()

        assert site.kind.value is MobilitySiteKind.ROAD_SEGMENT
        assert site.footprint.value == _footprint()
        assert site.source_caveats.value == (_category(),)

    def test_frozen(self) -> None:
        site = _site()

        with pytest.raises(ValidationError):
            site.site_id = "site-2"

    def test_empty_site_id_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _site(site_id="")

    def test_invalid_footprint_rejected(self) -> None:
        with pytest.raises(ValidationError, match="exactly one"):
            _site(
                footprint=MappedField[SpatialFootprint].model_validate(
                    {
                        "value": {},
                        "quality": "standardized",
                        "source_fields": ("geometry",),
                    }
                )
            )


class TestMobilityCountObservation:
    def test_minimum_valid_count_observation(self) -> None:
        observation = _count(
            direction=_mapped(None, quality=FieldQuality.UNMAPPED),
            movement_type=_mapped(None, quality=FieldQuality.UNMAPPED),
            measurement_method=_mapped(None, quality=FieldQuality.UNMAPPED),
            source_caveats=_mapped(None, "status", quality=FieldQuality.NOT_PROVIDED),
        )

        assert observation.observation_id == "count-1"
        assert observation.value.value == Decimal("42")

    def test_full_count_observation(self) -> None:
        observation = _count()

        assert observation.site_id == "site-1"
        assert observation.travel_mode.value is TravelMode.VEHICLE
        assert observation.metric_type.value is CountMetricType.RAW_COUNT

    def test_orphan_site_id_is_valid_at_model_layer(self) -> None:
        observation = _count(site_id="not-present-in-this-object-graph")

        assert observation.site_id == "not-present-in-this-object-graph"

    def test_site_and_observation_carry_independent_context(self) -> None:
        site = _site(
            direction=_mapped(
                ObservationDirection.NORTHBOUND,
                "site_direction",
                quality=FieldQuality.STANDARDIZED,
            ),
            movement_type=_mapped(
                MovementType.THROUGH,
                "site_movement",
                quality=FieldQuality.STANDARDIZED,
            ),
            measurement_method=_mapped(
                MeasurementMethod.AUTOMATED_COUNTER,
                "site_method",
                quality=FieldQuality.STANDARDIZED,
            ),
        )
        observation = _count(
            direction=_mapped(
                ObservationDirection.SOUTHBOUND,
                "row_direction",
                quality=FieldQuality.STANDARDIZED,
            ),
            movement_type=_mapped(
                MovementType.LEFT_TURN,
                "row_movement",
                quality=FieldQuality.STANDARDIZED,
            ),
            measurement_method=_mapped(
                MeasurementMethod.MANUAL_COUNT,
                "row_method",
                quality=FieldQuality.STANDARDIZED,
            ),
        )

        assert site.direction.value is ObservationDirection.NORTHBOUND
        assert observation.direction.value is ObservationDirection.SOUTHBOUND
        assert observation.movement_type.value is MovementType.LEFT_TURN
        assert observation.measurement_method.value is MeasurementMethod.MANUAL_COUNT

    def test_negative_count_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _count(value=_mapped(Decimal("-1"), "volume"))

    def test_empty_observation_id_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _count(observation_id="")

    def test_invalid_enum_value_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _count(
                travel_mode=MappedField[TravelMode].model_validate(
                    {
                        "value": "hoverboard",
                        "quality": "standardized",
                        "source_fields": ("mode",),
                    }
                )
            )

    def test_invalid_period_shape_rejected(self) -> None:
        with pytest.raises(ValidationError, match="date precision"):
            _count(
                period=MappedField[TemporalPeriod].model_validate(
                    {
                        "value": {
                            "precision": TemporalPeriodPrecision.DATE,
                            "date_value": date(2026, 5, 1),
                            "hour_value": 8,
                        },
                        "quality": FieldQuality.DERIVED,
                        "source_fields": ("timestamp",),
                    }
                )
            )

    def test_strict_extra_fields_rejected(self) -> None:
        with pytest.raises(ValidationError):
            MobilityCountObservation.model_validate(
                {
                    **_count().model_dump(),
                    "extra": "not allowed",
                }
            )


class TestMobilitySpeedObservation:
    def test_minimum_valid_speed_observation(self) -> None:
        observation = _speed(
            direction=_mapped(None, quality=FieldQuality.UNMAPPED),
            movement_type=_mapped(None, quality=FieldQuality.UNMAPPED),
            source_caveats=_mapped(None, quality=FieldQuality.UNMAPPED),
        )

        assert observation.observation_id == "speed-1"
        assert observation.metrics[0].value.value == Decimal("17.5")

    def test_multiple_metrics_in_one_speed_observation(self) -> None:
        observation = _speed(
            metrics=(
                _speed_metric(),
                _speed_metric(
                    metric_type=_mapped(
                        SpeedMetricType.TRAVEL_TIME,
                        "travel_time",
                        quality=FieldQuality.STANDARDIZED,
                    ),
                    unit=_mapped(
                        SpeedUnit.SECONDS,
                        "travel_time",
                        quality=FieldQuality.STANDARDIZED,
                    ),
                    value=_mapped(Decimal("88"), "travel_time"),
                ),
            )
        )

        assert [metric.metric_type.value for metric in observation.metrics] == [
            SpeedMetricType.OBSERVED_SPEED,
            SpeedMetricType.TRAVEL_TIME,
        ]

    def test_frozen(self) -> None:
        observation = _speed()

        with pytest.raises(ValidationError):
            observation.site_id = "link-2"

    def test_negative_speed_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _speed_metric(value=_mapped(Decimal("-0.1"), "speed"))

    def test_empty_metrics_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _speed(metrics=())

    def test_empty_observation_id_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _speed(observation_id="")

    def test_invalid_enum_value_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _speed_metric(
                metric_type=MappedField[SpeedMetricType].model_validate(
                    {
                        "value": "congestion_score",
                        "quality": "standardized",
                        "source_fields": ("speed",),
                    }
                )
            )


class TestJurisdictionRepresentability:
    def test_minimal_source_shapes(self) -> None:
        scenarios = (
            (
                "nyc-volume",
                SpatialFootprint(
                    line=LineString(
                        coordinates=(
                            Coordinate(latitude=40.758, longitude=-73.9855),
                            Coordinate(latitude=40.761, longitude=-73.981),
                        )
                    )
                ),
                TravelMode.VEHICLE,
                CountMetricType.RAW_COUNT,
                CountUnit.COUNT,
            ),
            (
                "chicago-region",
                SpatialFootprint(
                    bounding_box=BoundingBox(west=-87.9, south=41.6, east=-87.5, north=42.1)
                ),
                TravelMode.MIXED_TRAFFIC,
                CountMetricType.ESTIMATED_COUNT,
                CountUnit.VEHICLES_PER_PERIOD,
            ),
            (
                "toronto-counter",
                SpatialFootprint(point=Coordinate(latitude=43.6532, longitude=-79.3832)),
                TravelMode.BICYCLE,
                CountMetricType.RAW_COUNT,
                CountUnit.COUNT,
            ),
            (
                "gb-count-point",
                SpatialFootprint(point=Coordinate(latitude=51.5072, longitude=-0.1276)),
                TravelMode.VEHICLE,
                CountMetricType.AADF,
                CountUnit.VEHICLES_PER_DAY,
            ),
            (
                "france-channel",
                SpatialFootprint(point=Coordinate(latitude=48.8566, longitude=2.3522)),
                TravelMode.VEHICLE,
                CountMetricType.TMJA,
                CountUnit.VEHICLES_PER_DAY,
            ),
        )

        observations = tuple(
            _count(
                observation_id=name,
                site_id=f"{name}-site",
                travel_mode=_mapped(mode, "mode", quality=FieldQuality.STANDARDIZED),
                metric_type=_mapped(metric, "metric", quality=FieldQuality.STANDARDIZED),
                unit=_mapped(unit, "unit", quality=FieldQuality.STANDARDIZED),
            )
            for name, _footprint_value, mode, metric, unit in scenarios
        )
        sites = tuple(
            _site(
                site_id=f"{name}-site",
                footprint=_mapped(footprint, "geometry", quality=FieldQuality.STANDARDIZED),
            )
            for name, footprint, _mode, _metric, _unit in scenarios
        )

        assert len(observations) == 5
        assert len(sites) == 5

    def test_speed_source_shape(self) -> None:
        observation = _speed(
            observation_id="nyc-speed",
            site_id="link-123",
            metrics=(
                _speed_metric(
                    metric_type=_mapped(
                        SpeedMetricType.OBSERVED_SPEED,
                        "SPEED",
                        quality=FieldQuality.STANDARDIZED,
                    ),
                    unit=_mapped(
                        SpeedUnit.MILES_PER_HOUR,
                        "SPEED",
                        quality=FieldQuality.STANDARDIZED,
                    ),
                    value=_mapped(Decimal("22.4"), "SPEED"),
                ),
                _speed_metric(
                    metric_type=_mapped(
                        SpeedMetricType.TRAVEL_TIME,
                        "TRAVEL_TIME",
                        quality=FieldQuality.STANDARDIZED,
                    ),
                    unit=_mapped(
                        SpeedUnit.SECONDS,
                        "TRAVEL_TIME",
                        quality=FieldQuality.STANDARDIZED,
                    ),
                    value=_mapped(Decimal("140"), "TRAVEL_TIME"),
                ),
            ),
        )

        assert observation.metrics[1].unit.value is SpeedUnit.SECONDS
