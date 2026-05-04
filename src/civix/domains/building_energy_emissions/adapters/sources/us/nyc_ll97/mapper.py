"""NYC LL97 Covered Buildings List mappers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final, cast

from pydantic import BaseModel

from civix.core.identity.models.identifiers import DatasetId, MapperId, SourceId
from civix.core.mapping.errors import MappingError
from civix.core.mapping.models.mapper import MappingReport, MapResult
from civix.core.mapping.parsers import require_text, str_or_none
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
from civix.domains.building_energy_emissions.adapters.sources.us.nyc_ll97.caveats import (
    LL97_CAVEAT_SOURCE_FIELD,
    nyc_ll97_caveat_categories,
)
from civix.domains.building_energy_emissions.adapters.sources.us.nyc_ll97.schema import (
    LL97_TAXONOMY_VERSION,
)
from civix.domains.building_energy_emissions.models import (
    BuildingComplianceCase,
    BuildingEnergySubject,
    BuildingSubjectKind,
    ComplianceLifecycleStatus,
    IdentityCertainty,
    SourceIdentifier,
    build_building_compliance_case_key,
    build_building_energy_subject_key,
)

SUBJECT_MAPPER_ID: Final[MapperId] = MapperId("nyc-ll97-subject")
CASE_MAPPER_ID: Final[MapperId] = MapperId("nyc-ll97-case")
SUBJECT_MAPPER_VERSION: Final[str] = "0.1.0"
CASE_MAPPER_VERSION: Final[str] = "0.1.0"

_IDENTIFIER_TAXONOMY_ID: Final[str] = "nyc-ll97-source-identifier-kind"
_SUBJECT_KIND_TAXONOMY_ID: Final[str] = "nyc-ll97-subject-kind"
_COVERED_TAXONOMY_ID: Final[str] = "nyc-ll97-on-cbl"
_PATHWAY_TAXONOMY_ID: Final[str] = "nyc-ll97-compliance-pathway"

_FILING_YEAR_PARAM: Final[str] = "filing_year"
_ADDRESS_FIELDS: Final[tuple[str, ...]] = ("dof_bbl_address", "dof_bbl_zip_code")


@dataclass(frozen=True, slots=True)
class NycLl97SubjectMapper:
    """Maps one NYC LL97 CBL row to a `BuildingEnergySubject`."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=SUBJECT_MAPPER_ID, version=SUBJECT_MAPPER_VERSION)

    def __call__(
        self, record: RawRecord, snapshot: SourceSnapshot
    ) -> MapResult[BuildingEnergySubject]:
        raw = record.raw_data
        bin_value = _required_text(raw, "bin", self.version, record)
        bbl_value = _required_text(raw, "bbl", self.version, record)
        source_id = SourceId(str(snapshot.source_id))
        dataset_id = DatasetId(str(snapshot.dataset_id))

        subject = BuildingEnergySubject(
            provenance=_build_provenance(record=record, snapshot=snapshot, mapper=self.version),
            subject_key=build_building_energy_subject_key(source_id, dataset_id, bin_value),
            source_subject_identifiers=_map_source_identifiers(bin_value, bbl_value),
            subject_kind=MappedField(
                value=BuildingSubjectKind.BUILDING,
                quality=FieldQuality.STANDARDIZED,
                source_fields=("bin",),
            ),
            source_subject_kind=MappedField(
                value=_category(
                    "dob-bin",
                    label="DOB Building Identification Number",
                    taxonomy_id=_SUBJECT_KIND_TAXONOMY_ID,
                ),
                quality=FieldQuality.STANDARDIZED,
                source_fields=("bin",),
            ),
            identity_certainty=MappedField(
                value=IdentityCertainty.STABLE_CROSS_YEAR,
                quality=FieldQuality.STANDARDIZED,
                source_fields=("bin",),
            ),
            parent_subject_key=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            name=MappedField(
                value=None,
                quality=FieldQuality.NOT_PROVIDED,
                source_fields=("bin",),
            ),
            jurisdiction=MappedField(
                value=snapshot.jurisdiction,
                quality=FieldQuality.STANDARDIZED,
                source_fields=("bin",),
            ),
            address=_map_address(raw, snapshot),
            coordinate=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            property_types=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            floor_area=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            floor_area_unit=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            year_built=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            occupancy_label=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            ownership_label=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            source_caveats=_map_source_caveats(),
        )

        return MapResult(record=subject, report=_mapping_report(raw, subject))


@dataclass(frozen=True, slots=True)
class NycLl97CaseMapper:
    """Maps one NYC LL97 CBL row to a `BuildingComplianceCase`."""

    @property
    def version(self) -> MapperVersion:
        return MapperVersion(mapper_id=CASE_MAPPER_ID, version=CASE_MAPPER_VERSION)

    def __call__(
        self, record: RawRecord, snapshot: SourceSnapshot
    ) -> MapResult[BuildingComplianceCase]:
        raw = record.raw_data
        bin_value = _required_text(raw, "bin", self.version, record)
        bbl_value = _required_text(raw, "bbl", self.version, record)
        filing_year = _require_filing_year(snapshot, self.version, record)
        source_id = SourceId(str(snapshot.source_id))
        dataset_id = DatasetId(str(snapshot.dataset_id))
        subject_key = build_building_energy_subject_key(source_id, dataset_id, bin_value)
        case_key = build_building_compliance_case_key(
            source_id,
            dataset_id,
            f"{bbl_value}:{bin_value}",
            f"filing-year-{filing_year}",
        )

        covered_text = _required_text(raw, "on_ll97_cbl", self.version, record)
        covered_status = _portable_covered_status(covered_text, self.version, record)
        is_covered = covered_status is ComplianceLifecycleStatus.COVERED

        case = BuildingComplianceCase(
            provenance=_build_provenance(record=record, snapshot=snapshot, mapper=self.version),
            case_key=case_key,
            subject_key=subject_key,
            related_report_key=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            source_case_identifiers=_map_source_identifiers(bin_value, bbl_value),
            covered_period=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            filing_period=MappedField(
                value=TemporalPeriod(
                    precision=TemporalPeriodPrecision.YEAR,
                    year_value=filing_year,
                    timezone_status=TemporalTimezoneStatus.UNKNOWN,
                ),
                quality=FieldQuality.STANDARDIZED,
                source_fields=("bin",),
            ),
            covered_building_status=MappedField(
                value=covered_status,
                quality=FieldQuality.STANDARDIZED,
                source_fields=("on_ll97_cbl",),
            ),
            source_covered_status=MappedField(
                value=_covered_category(covered_text),
                quality=FieldQuality.STANDARDIZED,
                source_fields=("on_ll97_cbl",),
            ),
            compliance_pathway=_map_pathway(raw, is_covered=is_covered),
            compliance_status=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            source_compliance_status=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            exemption_status=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            extension_status=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            emissions_limit_metric_key=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            final_emissions_metric_key=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            excess_emissions_metric_key=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            penalty_amount=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            penalty_currency=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            penalty_status=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            dispute_status=MappedField(
                value=None,
                quality=FieldQuality.UNMAPPED,
                source_fields=(),
            ),
            source_caveats=_map_source_caveats(),
        )

        return MapResult(record=case, report=_mapping_report(raw, case))


def _portable_covered_status(
    raw_value: str,
    mapper: MapperVersion,
    record: RawRecord,
) -> ComplianceLifecycleStatus:
    normalized = raw_value.strip().lower()

    if normalized == "y":
        return ComplianceLifecycleStatus.COVERED

    if normalized == "n":
        return ComplianceLifecycleStatus.NOT_COVERED

    raise MappingError(
        f"unrecognized on_ll97_cbl value {raw_value!r}",
        mapper=mapper,
        source_record_id=record.source_record_id,
        source_fields=("on_ll97_cbl",),
    )


def _covered_category(raw_value: str) -> CategoryRef:
    normalized = raw_value.strip().lower()
    label = "Yes" if normalized == "y" else "No"

    return _category(normalized, label=label, taxonomy_id=_COVERED_TAXONOMY_ID)


def _map_pathway(
    raw: Mapping[str, Any],
    *,
    is_covered: bool,
) -> MappedField[CategoryRef]:
    pathway = str_or_none(raw.get("ll97_compliance_pathway"))

    if pathway is None:
        quality = FieldQuality.NOT_PROVIDED if is_covered else FieldQuality.UNMAPPED
        source_fields: tuple[str, ...] = ("ll97_compliance_pathway",) if is_covered else ()

        return MappedField(value=None, quality=quality, source_fields=source_fields)

    return MappedField(
        value=_category(
            f"pathway-{pathway}",
            label=f"LL97 Compliance Pathway {pathway}",
            taxonomy_id=_PATHWAY_TAXONOMY_ID,
        ),
        quality=FieldQuality.STANDARDIZED,
        source_fields=("ll97_compliance_pathway",),
    )


def _map_address(raw: Mapping[str, Any], snapshot: SourceSnapshot) -> MappedField[Address]:
    street = str_or_none(raw.get("dof_bbl_address"))
    postal_code = str_or_none(raw.get("dof_bbl_zip_code"))

    if street is None and postal_code is None:
        return MappedField(
            value=None,
            quality=FieldQuality.NOT_PROVIDED,
            source_fields=_ADDRESS_FIELDS,
        )

    return MappedField(
        value=Address(
            country=snapshot.jurisdiction.country,
            region=snapshot.jurisdiction.region,
            locality=snapshot.jurisdiction.locality,
            street=street,
            postal_code=postal_code,
        ),
        quality=FieldQuality.DIRECT,
        source_fields=_ADDRESS_FIELDS,
    )


def _map_source_identifiers(
    bin_value: str,
    bbl_value: str,
) -> MappedField[tuple[SourceIdentifier, ...]]:
    return MappedField(
        value=(
            SourceIdentifier(
                value=bin_value,
                identifier_kind=_category(
                    "dob-bin",
                    label="DOB Building Identification Number",
                    taxonomy_id=_IDENTIFIER_TAXONOMY_ID,
                ),
            ),
            SourceIdentifier(
                value=bbl_value,
                identifier_kind=_category(
                    "dof-bbl",
                    label="DOF Borough-Block-Lot",
                    taxonomy_id=_IDENTIFIER_TAXONOMY_ID,
                ),
            ),
        ),
        quality=FieldQuality.DIRECT,
        source_fields=("bbl", "bin"),
    )


def _map_source_caveats() -> MappedField[tuple[CategoryRef, ...]]:
    return MappedField(
        value=nyc_ll97_caveat_categories(),
        quality=FieldQuality.STANDARDIZED,
        source_fields=(LL97_CAVEAT_SOURCE_FIELD,),
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


def _require_filing_year(
    snapshot: SourceSnapshot,
    mapper: MapperVersion,
    record: RawRecord,
) -> int:
    fetch_params = snapshot.fetch_params or {}
    raw = fetch_params.get(_FILING_YEAR_PARAM)

    if raw is None:
        raise MappingError(
            f"snapshot.fetch_params is missing {_FILING_YEAR_PARAM!r}",
            mapper=mapper,
            source_record_id=record.source_record_id,
            source_fields=("bin",),
        )

    try:
        return int(raw)
    except ValueError as e:
        raise MappingError(
            f"snapshot.fetch_params {_FILING_YEAR_PARAM!r} is not an integer",
            mapper=mapper,
            source_record_id=record.source_record_id,
            source_fields=("bin",),
        ) from e


def _category(
    code: str,
    *,
    taxonomy_id: str,
    label: str,
) -> CategoryRef:
    return CategoryRef(
        code=code,
        label=label,
        taxonomy_id=taxonomy_id,
        taxonomy_version=LL97_TAXONOMY_VERSION,
    )


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


def _mapping_report(
    raw: Mapping[str, Any],
    record: BaseModel | tuple[BaseModel, ...],
) -> MappingReport:
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
