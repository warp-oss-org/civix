from datetime import UTC, datetime

import pytest
from pydantic import BaseModel, ConfigDict, ValidationError

from civix.core.identity import (
    DatasetId,
    Jurisdiction,
    MapperId,
    SnapshotId,
    SourceId,
)
from civix.core.mapping import FieldConflict, Mapper, MappingReport, MapResult
from civix.core.observations import RawRecord, SourceSnapshot
from civix.core.provenance import MapperVersion, ProvenanceRef
from civix.core.quality import FieldQuality, MappedField


class TestFieldConflict:
    def test_minimum_fields(self) -> None:
        c = FieldConflict(
            field_name="status",
            candidates=("Issued", "Active"),
            source_fields=("status", "status_legacy"),
        )
        assert c.field_name == "status"
        assert c.candidates == ("Issued", "Active")

    def test_field_name_required(self) -> None:
        with pytest.raises(ValidationError, match="non-empty"):
            FieldConflict(
                field_name="",
                candidates=("a", "b"),
                source_fields=("x", "y"),
            )

    def test_field_name_whitespace_rejected(self) -> None:
        with pytest.raises(ValidationError, match="whitespace"):
            FieldConflict(
                field_name=" status",
                candidates=("a", "b"),
                source_fields=("x", "y"),
            )

    def test_at_least_two_candidates_required(self) -> None:
        with pytest.raises(ValidationError, match="two candidates"):
            FieldConflict(
                field_name="status",
                candidates=("only",),
                source_fields=("x", "y"),
            )

    def test_at_least_two_source_fields_required(self) -> None:
        with pytest.raises(ValidationError, match="two source fields"):
            FieldConflict(
                field_name="status",
                candidates=("a", "b"),
                source_fields=("x",),
            )

    def test_source_field_whitespace_rejected(self) -> None:
        with pytest.raises(ValidationError, match="whitespace"):
            FieldConflict(
                field_name="status",
                candidates=("a", "b"),
                source_fields=("x", " y"),
            )

    def test_candidates_can_be_heterogeneous_types(self) -> None:
        c = FieldConflict(
            field_name="value",
            candidates=("yes", 1, None),
            source_fields=("a", "b", "c"),
        )
        assert c.candidates == ("yes", 1, None)

    def test_frozen(self) -> None:
        c = FieldConflict(
            field_name="status",
            candidates=("a", "b"),
            source_fields=("x", "y"),
        )
        with pytest.raises(ValidationError):
            c.field_name = "other"  # type: ignore[misc]


class TestMappingReport:
    def test_defaults_are_empty(self) -> None:
        r = MappingReport()
        assert r.unmapped_source_fields == ()
        assert r.conflicts == ()

    def test_with_unmapped_source_fields(self) -> None:
        r = MappingReport(unmapped_source_fields=("internal_id", "raw_blob"))
        assert r.unmapped_source_fields == ("internal_id", "raw_blob")

    def test_with_conflicts(self) -> None:
        conflict = FieldConflict(
            field_name="status",
            candidates=("Issued", "Active"),
            source_fields=("status", "status_legacy"),
        )
        r = MappingReport(conflicts=(conflict,))
        assert r.conflicts == (conflict,)

    def test_unmapped_source_fields_whitespace_rejected(self) -> None:
        with pytest.raises(ValidationError, match="whitespace"):
            MappingReport(unmapped_source_fields=(" internal_id",))

    def test_unmapped_source_fields_empty_rejected(self) -> None:
        with pytest.raises(ValidationError, match="non-empty"):
            MappingReport(unmapped_source_fields=("",))

    def test_extra_fields_rejected(self) -> None:
        with pytest.raises(ValidationError):
            MappingReport.model_validate({"unexpected": "nope"})

    def test_frozen(self) -> None:
        r = MappingReport()
        with pytest.raises(ValidationError):
            r.unmapped_source_fields = ("x",)  # type: ignore[misc]


class _FakeNormalized(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    provenance: ProvenanceRef
    name: MappedField[str]


def _snapshot() -> SourceSnapshot:
    return SourceSnapshot(
        snapshot_id=SnapshotId("snap-1"),
        source_id=SourceId("vancouver-open-data"),
        dataset_id=DatasetId("business-licences"),
        jurisdiction=Jurisdiction(country="CA", region="BC", locality="Vancouver"),
        fetched_at=datetime(2026, 4, 25, 12, 0, tzinfo=UTC),
        record_count=1,
    )


class _FakeMapper:
    def __init__(self, version: MapperVersion) -> None:
        self._version = version

    @property
    def version(self) -> MapperVersion:
        return self._version

    def __call__(self, record: RawRecord, snapshot: SourceSnapshot) -> MapResult[_FakeNormalized]:
        provenance = ProvenanceRef(
            snapshot_id=snapshot.snapshot_id,
            source_id=snapshot.source_id,
            dataset_id=snapshot.dataset_id,
            jurisdiction=snapshot.jurisdiction,
            fetched_at=snapshot.fetched_at,
            mapper=self._version,
            source_record_id=record.source_record_id,
        )
        normalized = _FakeNormalized(
            provenance=provenance,
            name=MappedField[str](
                value=str(record.raw_data["name"]),
                quality=FieldQuality.DIRECT,
                source_fields=("name",),
            ),
        )
        return MapResult[_FakeNormalized](record=normalized, report=MappingReport())


class TestMapResult:
    def test_holds_record_and_report(self) -> None:
        snap = _snapshot()
        raw = RawRecord(
            snapshot_id=snap.snapshot_id,
            raw_data={"name": "Joe's Cafe"},
            source_record_id="abc-1",
        )
        mapper = _FakeMapper(
            MapperVersion(
                mapper_id=MapperId("vancouver-business-licences"),
                version="0.1.0",
            )
        )
        result = mapper(raw, snap)
        assert result.record.name.value == "Joe's Cafe"
        assert result.record.provenance.source_record_id == "abc-1"
        assert result.report.unmapped_source_fields == ()

    def test_frozen(self) -> None:
        snap = _snapshot()
        raw = RawRecord(snapshot_id=snap.snapshot_id, raw_data={"name": "X"})
        mapper = _FakeMapper(MapperVersion(mapper_id=MapperId("m"), version="0.1.0"))
        result = mapper(raw, snap)
        with pytest.raises(ValidationError):
            result.report = MappingReport()  # type: ignore[misc]


class TestMapperProtocol:
    def test_fake_mapper_satisfies_protocol_at_runtime(self) -> None:
        mapper = _FakeMapper(MapperVersion(mapper_id=MapperId("m"), version="0.1.0"))
        assert isinstance(mapper, Mapper)

    def test_object_missing_methods_does_not_satisfy_protocol(self) -> None:
        class _NotAMapper:
            pass

        assert not isinstance(_NotAMapper(), Mapper)

    def test_object_missing_version_does_not_satisfy_protocol(self) -> None:
        class _NoVersion:
            def __call__(
                self, record: RawRecord, snapshot: SourceSnapshot
            ) -> MapResult[_FakeNormalized]:
                raise NotImplementedError

        assert not isinstance(_NoVersion(), Mapper)
