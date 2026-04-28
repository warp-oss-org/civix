from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest
from pydantic import ValidationError

from civix.core.identity import (
    DatasetId,
    Jurisdiction,
    MapperId,
    SnapshotId,
    SourceId,
)
from civix.core.provenance import MapperVersion, ProvenanceRef


def _mapper(**overrides: Any) -> MapperVersion:
    defaults: dict[str, Any] = {
        "mapper_id": MapperId("vancouver-business-licences"),
        "version": "0.1.0",
    }
    defaults.update(overrides)

    return MapperVersion(**defaults)


def _ref(**overrides: Any) -> ProvenanceRef:
    defaults: dict[str, Any] = {
        "snapshot_id": SnapshotId("snap-1"),
        "source_id": SourceId("vancouver-open-data"),
        "dataset_id": DatasetId("business-licences"),
        "jurisdiction": Jurisdiction(country="CA", region="BC", locality="Vancouver"),
        "fetched_at": datetime(2026, 4, 24, 12, 0, tzinfo=UTC),
        "mapper": _mapper(),
    }
    defaults.update(overrides)

    return ProvenanceRef(**defaults)


class TestMapperVersion:
    def test_minimum_fields(self) -> None:
        m = _mapper()

        assert m.mapper_id == "vancouver-business-licences"
        assert m.version == "0.1.0"

    def test_accepts_commit_hash_as_version(self) -> None:
        m = _mapper(version="9845bf7")

        assert m.version == "9845bf7"

    def test_accepts_date_stamp_as_version(self) -> None:
        m = _mapper(version="2026-04-25")

        assert m.version == "2026-04-25"

    def test_empty_mapper_id_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _mapper(mapper_id=MapperId(""))

    def test_empty_version_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _mapper(version="")

    def test_whitespace_rejected(self) -> None:
        with pytest.raises(ValidationError, match="whitespace"):
            _mapper(version=" 0.1.0 ")

    def test_frozen(self) -> None:
        m = _mapper()

        with pytest.raises(ValidationError):
            m.version = "0.2.0"  # type: ignore[misc]

    def test_extra_fields_rejected(self) -> None:
        with pytest.raises(ValidationError):
            MapperVersion.model_validate({"mapper_id": "x", "version": "0.1.0", "extra": "nope"})


class TestProvenanceRef:
    def test_minimum_fields(self) -> None:
        r = _ref()

        assert r.source_record_id is None
        assert r.mapper.mapper_id == "vancouver-business-licences"

    def test_optional_source_record_id(self) -> None:
        r = _ref(source_record_id="abc-123")

        assert r.source_record_id == "abc-123"

    def test_naive_fetched_at_rejected(self) -> None:
        with pytest.raises(ValidationError, match="UTC"):
            _ref(fetched_at=datetime(2026, 4, 24, 12, 0))

    def test_non_utc_fetched_at_rejected(self) -> None:
        eastern = timezone(timedelta(hours=-5))

        with pytest.raises(ValidationError, match="UTC"):
            _ref(fetched_at=datetime(2026, 4, 24, 12, 0, tzinfo=eastern))

    def test_extra_fields_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _ref(unexpected="nope")

    def test_frozen(self) -> None:
        r = _ref()

        with pytest.raises(ValidationError):
            r.source_record_id = "x"  # type: ignore[misc]

    def test_equality_for_dedup(self) -> None:
        # Two refs built from the same fields should compare equal so
        # consumers can dedup or group on provenance without writing
        # bespoke comparison code.
        a = _ref(source_record_id="abc-123")
        b = _ref(source_record_id="abc-123")

        assert a == b
        assert hash(a) == hash(b)
