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
from civix.core.spatial.models.geometry import GeometryRef, GeometryType, SpatialFootprint
from civix.core.spatial.models.location import Coordinate
from civix.core.taxonomy.models.category import CategoryRef
from civix.core.temporal import TemporalPeriod, TemporalPeriodPrecision, TemporalTimezoneStatus
from civix.domains.hazard_risk.models import (
    CategoryScoreMeasure,
    HazardRiskArea,
    HazardRiskAreaKind,
    HazardRiskHazardType,
    HazardRiskScore,
    HazardRiskScoreDirection,
    HazardRiskScoreType,
    HazardRiskZone,
    HazardRiskZoneStatus,
    NumericScoreMeasure,
    ScoreScale,
    SourceIdentifier,
    TextScoreMeasure,
    build_hazard_risk_area_key,
    build_hazard_risk_zone_key,
)

SOURCE_ID = SourceId("hazard-risk-test-source")
DATASET_ID = DatasetId("hazard-risk-test-dataset")


def _mapped[T](
    value: T | None,
    *source_fields: str,
    quality: FieldQuality = FieldQuality.DIRECT,
) -> MappedField[T]:
    return MappedField[T](value=value, quality=quality, source_fields=source_fields)


def _provenance(source_record_id: str = "risk-row-1") -> ProvenanceRef:
    return ProvenanceRef(
        snapshot_id=SnapshotId("snap-hazard-risk-1"),
        source_id=SOURCE_ID,
        dataset_id=DATASET_ID,
        jurisdiction=Jurisdiction(country="US"),
        fetched_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
        mapper=MapperVersion(
            mapper_id=MapperId("hazard-risk-test-mapper"),
            version="0.1.0",
        ),
        source_record_id=source_record_id,
    )


def _category(code: str = "source-category") -> CategoryRef:
    return CategoryRef(
        code=code,
        label=code.replace("-", " ").title(),
        taxonomy_id="civix.hazard-risk.test",
        taxonomy_version="2026-05-01",
    )


def _period(value: date = date(2026, 5, 1)) -> TemporalPeriod:
    return TemporalPeriod(
        precision=TemporalPeriodPrecision.DATE,
        date_value=value,
        timezone_status=TemporalTimezoneStatus.UNKNOWN,
    )


def _geometry_ref() -> GeometryRef:
    return GeometryRef(
        geometry_type=GeometryType.POLYGON,
        uri="https://example.test/arcgis/rest/services/hazards/MapServer",
        layer_name="Hazard Zones",
        geometry_id="zone-1",
        source_crs="EPSG:4326",
    )


def _area_key(source_area_id: str = "area-1") -> str:
    return build_hazard_risk_area_key(SOURCE_ID, DATASET_ID, source_area_id)


def _zone_key(source_zone_id: str = "zone-1") -> str:
    return build_hazard_risk_zone_key(SOURCE_ID, DATASET_ID, source_zone_id)


def _area(**overrides: Any) -> HazardRiskArea:
    defaults: dict[str, Any] = {
        "provenance": _provenance("area-row-1"),
        "area_key": _area_key(),
        "source_area_identifiers": _mapped(
            (SourceIdentifier(value="area-1", identifier_kind=_category("source-area-id")),),
            "sourceAreaId",
        ),
        "area_kind": _mapped(
            HazardRiskAreaKind.RISK_INDEX_AREA,
            "areaType",
            quality=FieldQuality.STANDARDIZED,
        ),
        "source_area_kind": _mapped(_category("tract"), "areaType"),
        "name": _mapped("Risk Area 1", "areaName"),
        "jurisdiction": _mapped(Jurisdiction(country="US", region="AL"), "state"),
        "administrative_areas": _mapped(("Autauga County",), "county"),
        "footprint": _mapped(
            SpatialFootprint(point=Coordinate(latitude=32.5, longitude=-86.5)),
            "latitude",
            "longitude",
            quality=FieldQuality.STANDARDIZED,
        ),
        "geometry_ref": _mapped(_geometry_ref(), "geometry"),
        "source_hazards": _mapped((_category("all-hazards"),), "hazards"),
        "source_caveats": _mapped((_category("planning-use-only"),), "metadata"),
    }
    defaults.update(overrides)

    return HazardRiskArea(**defaults)


def _score(**overrides: Any) -> HazardRiskScore:
    defaults: dict[str, Any] = {
        "provenance": _provenance("score-row-1"),
        "score_id": "score-1",
        "area_key": _area_key(),
        "hazard_type": _mapped(
            HazardRiskHazardType.MULTI_HAZARD,
            "hazard",
            quality=FieldQuality.STANDARDIZED,
        ),
        "source_hazard": _mapped(_category("all-hazards"), "hazard"),
        "score_type": _mapped(
            HazardRiskScoreType.COMPOSITE_INDEX,
            "scoreType",
            quality=FieldQuality.STANDARDIZED,
        ),
        "source_score_type": _mapped(_category("risk-index-score"), "scoreType"),
        "score_measure": _mapped(NumericScoreMeasure(value=Decimal("73.4")), "score"),
        "score_unit": _mapped(_category("score-points"), "scoreUnit"),
        "score_scale": _mapped(ScoreScale(minimum=Decimal("0"), maximum=Decimal("100")), "scale"),
        "score_direction": _mapped(
            HazardRiskScoreDirection.HIGHER_IS_HIGHER_RISK,
            "methodology",
            quality=FieldQuality.STANDARDIZED,
        ),
        "methodology_label": _mapped("Synthetic risk index", "methodology"),
        "methodology_version": _mapped("v1.0", "methodologyVersion"),
        "methodology_url": _mapped(
            None,
            quality=FieldQuality.UNMAPPED,
        ),
        "publication_vintage": _mapped(_period(), "publicationDate"),
        "effective_period": _mapped(
            None,
            "effectiveDate",
            quality=FieldQuality.NOT_PROVIDED,
        ),
        "source_caveats": _mapped((_category("planning-use-only"),), "metadata"),
    }
    defaults.update(overrides)

    return HazardRiskScore(**defaults)


def _zone(**overrides: Any) -> HazardRiskZone:
    defaults: dict[str, Any] = {
        "provenance": _provenance("zone-row-1"),
        "zone_key": _zone_key(),
        "source_zone_identifiers": _mapped(
            (SourceIdentifier(value="zone-1", identifier_kind=_category("source-zone-id")),),
            "sourceZoneId",
        ),
        "hazard_type": _mapped(
            HazardRiskHazardType.FLOOD,
            "hazard",
            quality=FieldQuality.STANDARDIZED,
        ),
        "source_hazard": _mapped(_category("flood"), "hazard"),
        "source_zone": _mapped(_category("AE"), "zone"),
        "status": _mapped(
            HazardRiskZoneStatus.EFFECTIVE,
            "status",
            quality=FieldQuality.STANDARDIZED,
        ),
        "source_status": _mapped(_category("effective"), "status"),
        "plan_identifier": _mapped("plan-1", "planId"),
        "plan_name": _mapped("Flood hazard plan", "planName"),
        "effective_period": _mapped(_period(), "effectiveDate"),
        "footprint": _mapped(
            None,
            "geometry",
            quality=FieldQuality.REDACTED,
        ),
        "geometry_ref": _mapped(_geometry_ref(), "geometry"),
        "source_caveats": _mapped((_category("regulatory-use"),), "metadata"),
    }
    defaults.update(overrides)

    return HazardRiskZone(**defaults)


class TestHazardRiskKeys:
    def test_area_key_is_deterministic_and_versioned(self) -> None:
        key = build_hazard_risk_area_key(SOURCE_ID, DATASET_ID, "area-1")

        assert key == build_hazard_risk_area_key(SOURCE_ID, DATASET_ID, "area-1")
        assert key.startswith("hr-area:v1:")
        assert len(key.removeprefix("hr-area:v1:")) == 64

    def test_zone_key_is_deterministic_and_versioned(self) -> None:
        key = build_hazard_risk_zone_key(SOURCE_ID, DATASET_ID, "zone-1")

        assert key == build_hazard_risk_zone_key(SOURCE_ID, DATASET_ID, "zone-1")
        assert key.startswith("hr-zone:v1:")
        assert len(key.removeprefix("hr-zone:v1:")) == 64

    def test_different_source_parts_produce_different_area_keys(self) -> None:
        key = build_hazard_risk_area_key(SOURCE_ID, DATASET_ID, "area-1")
        other = build_hazard_risk_area_key(SOURCE_ID, DATASET_ID, "area-2")

        assert key != other

    def test_empty_key_parts_rejected(self) -> None:
        with pytest.raises(ValueError, match="source_record_id"):
            build_hazard_risk_area_key(SOURCE_ID, DATASET_ID, "")


class TestHazardRiskArea:
    def test_minimum_valid_area(self) -> None:
        area = _area()

        assert area.area_key == _area_key()
        assert area.source_area_identifiers.value is not None
        assert area.source_area_identifiers.value[0].value == "area-1"

    def test_invalid_area_key_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _area(area_key="area-1")

    def test_frozen(self) -> None:
        area = _area()

        with pytest.raises(ValidationError):
            area.area_key = _area_key("area-2")  # type: ignore[misc]

    def test_extra_fields_rejected(self) -> None:
        payload = _area().model_dump(mode="python")
        payload["unexpected"] = True

        with pytest.raises(ValidationError):
            HazardRiskArea.model_validate(payload)


class TestHazardRiskScore:
    def test_minimum_valid_score(self) -> None:
        score = _score()

        assert score.area_key == _area_key()
        assert isinstance(score.score_measure.value, NumericScoreMeasure)
        assert score.methodology_version.value == "v1.0"

    def test_multi_metric_area_with_score_facts(self) -> None:
        area = _area()
        composite_score = _score(score_id="composite-score")
        rating_score = _score(
            score_id="composite-rating",
            score_type=_mapped(
                HazardRiskScoreType.RATING,
                "rating",
                quality=FieldQuality.STANDARDIZED,
            ),
            score_measure=_mapped(CategoryScoreMeasure(value=_category("very-high")), "rating"),
        )
        text_score = _score(
            score_id="source-text",
            score_type=_mapped(
                HazardRiskScoreType.SOURCE_SPECIFIC,
                "sourceScore",
                quality=FieldQuality.STANDARDIZED,
            ),
            score_measure=_mapped(TextScoreMeasure(value="Advisory"), "sourceScore"),
        )

        assert {score.area_key for score in (composite_score, rating_score, text_score)} == {
            area.area_key
        }
        assert isinstance(rating_score.score_measure.value, CategoryScoreMeasure)
        assert isinstance(text_score.score_measure.value, TextScoreMeasure)

    def test_unmapped_not_provided_and_redacted_field_quality_states(self) -> None:
        score = _score(
            methodology_url=_mapped(None, quality=FieldQuality.UNMAPPED),
            effective_period=_mapped(
                None,
                "effectiveDate",
                quality=FieldQuality.NOT_PROVIDED,
            ),
            source_caveats=_mapped(
                None,
                "caveat",
                quality=FieldQuality.REDACTED,
            ),
        )

        assert score.methodology_url.quality is FieldQuality.UNMAPPED
        assert score.effective_period.quality is FieldQuality.NOT_PROVIDED
        assert score.source_caveats.quality is FieldQuality.REDACTED

    def test_numeric_measure_must_fit_scale(self) -> None:
        with pytest.raises(ValidationError, match="within score scale"):
            _score(score_measure=_mapped(NumericScoreMeasure(value=Decimal("101")), "score"))

    def test_unmapped_measure_can_preserve_scale_metadata(self) -> None:
        score = _score(score_measure=_mapped(None, quality=FieldQuality.UNMAPPED))

        assert score.score_measure.value is None
        assert score.score_scale.value == ScoreScale(minimum=Decimal("0"), maximum=Decimal("100"))

    def test_invalid_score_scale_rejected(self) -> None:
        with pytest.raises(ValidationError, match="maximum"):
            ScoreScale(minimum=Decimal("100"), maximum=Decimal("0"))

    def test_score_id_whitespace_rejected(self) -> None:
        with pytest.raises(ValidationError, match="score_id"):
            _score(score_id=" score-1 ")

    def test_invalid_score_direction_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _score(
                score_direction=_mapped(
                    "higher-means-risk",
                    "methodology",
                    quality=FieldQuality.STANDARDIZED,
                )
            )

    def test_invalid_score_measure_discriminator_rejected(self) -> None:
        payload = _score().model_dump(mode="python")
        payload["score_measure"] = {
            "value": {"kind": "unsupported", "value": "x"},
            "quality": FieldQuality.DIRECT,
            "source_fields": ("score",),
        }

        with pytest.raises(ValidationError):
            HazardRiskScore.model_validate(payload)


class TestHazardRiskZone:
    def test_minimum_valid_zone(self) -> None:
        zone = _zone()

        assert zone.zone_key == _zone_key()
        assert zone.source_zone.value == _category("AE")
        assert zone.geometry_ref.value == _geometry_ref()

    def test_invalid_zone_key_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _zone(zone_key="zone-1")

    def test_regulatory_zone_preserves_category_status_and_geometry_ref(self) -> None:
        zone = _zone(
            source_zone=_mapped(_category("X"), "zone"),
            status=_mapped(
                HazardRiskZoneStatus.PRELIMINARY,
                "status",
                quality=FieldQuality.STANDARDIZED,
            ),
            source_status=_mapped(_category("preliminary"), "status"),
        )

        assert zone.source_zone.value == _category("X")
        assert zone.status.value is HazardRiskZoneStatus.PRELIMINARY
        assert zone.geometry_ref.value is not None

    def test_extra_fields_rejected(self) -> None:
        payload = _zone().model_dump(mode="python")
        payload["unexpected"] = "AE"

        with pytest.raises(ValidationError):
            HazardRiskZone.model_validate(payload)
