from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest
from pydantic import ValidationError

from civix.core.identity import DatasetId, Jurisdiction, SnapshotId, SourceId
from civix.core.snapshots import RawRecord, SourceSnapshot


def _snapshot(**overrides: Any) -> SourceSnapshot:
    defaults: dict[str, Any] = {
        "snapshot_id": SnapshotId("snap-1"),
        "source_id": SourceId("vancouver-open-data"),
        "dataset_id": DatasetId("business-licences"),
        "jurisdiction": Jurisdiction(country="CA", region="BC", locality="Vancouver"),
        "fetched_at": datetime(2026, 4, 24, 12, 0, tzinfo=UTC),
        "record_count": 2,
    }
    defaults.update(overrides)

    return SourceSnapshot(**defaults)


class TestSourceSnapshot:
    def test_minimum_fields(self) -> None:
        s = _snapshot()

        assert s.source_url is None
        assert s.fetch_params is None
        assert s.content_hash is None

    def test_optional_fields_round_trip(self) -> None:
        s = _snapshot(
            source_url="https://opendata.vancouver.ca/api/records/1.0/search/",
            fetch_params={"dataset": "business-licences", "rows": "100"},
            content_hash="sha256:deadbeef",
        )

        assert s.source_url is not None
        assert s.fetch_params == {"dataset": "business-licences", "rows": "100"}
        assert s.content_hash == "sha256:deadbeef"

    def test_naive_datetime_rejected(self) -> None:
        with pytest.raises(ValidationError, match="UTC"):
            _snapshot(fetched_at=datetime(2026, 4, 24, 12, 0))

    def test_non_utc_datetime_rejected(self) -> None:
        eastern = timezone(timedelta(hours=-5))

        with pytest.raises(ValidationError, match="UTC"):
            _snapshot(fetched_at=datetime(2026, 4, 24, 12, 0, tzinfo=eastern))

    def test_negative_record_count_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _snapshot(record_count=-1)

    def test_extra_fields_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _snapshot(unexpected="nope")  # type: ignore[arg-type]

    def test_frozen(self) -> None:
        s = _snapshot()

        with pytest.raises(ValidationError):
            s.record_count = 10  # type: ignore[misc]


class TestRawRecord:
    def test_minimum_fields(self) -> None:
        r = RawRecord(
            snapshot_id=SnapshotId("snap-1"),
            raw_data={"businessname": "Joe's Cafe", "status": "Issued"},
        )

        assert r.source_record_id is None
        assert r.source_updated_at is None
        assert r.record_hash is None
        assert r.raw_data["businessname"] == "Joe's Cafe"

    def test_back_reference_to_snapshot(self) -> None:
        snap = _snapshot()
        r = RawRecord(snapshot_id=snap.snapshot_id, raw_data={})

        assert r.snapshot_id == snap.snapshot_id

    def test_naive_source_updated_at_rejected(self) -> None:
        with pytest.raises(ValidationError, match="UTC"):
            RawRecord(
                snapshot_id=SnapshotId("snap-1"),
                raw_data={},
                source_updated_at=datetime(2026, 4, 24, 12, 0),
            )

    def test_extra_fields_rejected(self) -> None:
        with pytest.raises(ValidationError):
            RawRecord(
                snapshot_id=SnapshotId("snap-1"),
                raw_data={},
                surprise="nope",  # type: ignore[call-arg]
            )

    def test_frozen(self) -> None:
        r = RawRecord(snapshot_id=SnapshotId("snap-1"), raw_data={"a": 1})

        with pytest.raises(ValidationError):
            r.source_record_id = "x"  # type: ignore[misc]

    def test_raw_data_preserves_arbitrary_payload(self) -> None:
        payload = {
            "nested": {"category": "Restaurant"},
            "coords": [49.28, -123.12],
            "issued": "2024-05-06",
        }
        r = RawRecord(snapshot_id=SnapshotId("snap-1"), raw_data=payload)

        assert r.raw_data == payload
