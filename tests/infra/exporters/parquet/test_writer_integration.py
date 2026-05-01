"""End-to-end test: real Vancouver pipeline -> Parquet export -> reload from disk."""

from __future__ import annotations

import importlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import respx

from civix.core.export.models.manifest import ExportManifest
from civix.core.identity.models.identifiers import DatasetId, Jurisdiction
from civix.core.pipeline import run
from civix.core.quality.models.fields import FieldQuality
from civix.domains.business_licences.models.licence import BusinessLicence, LicenceStatus
from civix.infra.exporters.parquet import write_snapshot
from civix.infra.sources.ca.vancouver_business_licences import (
    DEFAULT_BASE_URL,
    VancouverBusinessLicencesAdapter,
    VancouverBusinessLicencesMapper,
)

PINNED_NOW = datetime(2026, 4, 25, 12, 0, tzinfo=UTC)
DATASET = DatasetId("business-licences")
JURISDICTION = Jurisdiction(country="CA", region="BC", locality="Vancouver")
RECORDS_URL = f"{DEFAULT_BASE_URL}catalog/datasets/{DATASET}/records"
EXPORTS_URL = f"{DEFAULT_BASE_URL}catalog/datasets/{DATASET}/exports/jsonl"
PQ: Any = importlib.import_module("pyarrow.parquet")

FIXTURES = (
    Path(__file__).parent.parent.parent
    / "sources"
    / "ca"
    / "vancouver_business_licences"
    / "fixtures"
)


async def _export(tmp_path: Path) -> tuple[ExportManifest, Path]:
    count_payload = json.loads((FIXTURES / "count_response.json").read_text())
    records_body = (FIXTURES / "records.jsonl").read_text()

    async with respx.mock(assert_all_called=True) as respx_mock:
        respx_mock.get(RECORDS_URL).mock(return_value=httpx.Response(200, json=count_payload))
        respx_mock.get(EXPORTS_URL).mock(return_value=httpx.Response(200, text=records_body))

        async with httpx.AsyncClient() as client:
            adapter = VancouverBusinessLicencesAdapter(
                dataset_id=DATASET,
                jurisdiction=JURISDICTION,
                client=client,
                clock=lambda: PINNED_NOW,
            )
            mapper = VancouverBusinessLicencesMapper()
            pipeline_result = await run(adapter, mapper)
            manifest = await write_snapshot(
                pipeline_result,
                output_dir=tmp_path,
                record_type=BusinessLicence,
            )

    return manifest, tmp_path / pipeline_result.snapshot.snapshot_id


class TestVancouverEndToEnd:
    async def test_manifest_records_three_records(self, tmp_path: Path) -> None:
        manifest, _ = await _export(tmp_path)

        assert manifest.record_count == 3
        assert manifest.mapper is not None
        assert manifest.mapper.mapper_id == "vancouver-business-licences"

    async def test_records_parquet_round_trips_through_business_licence(
        self, tmp_path: Path
    ) -> None:
        _, snap_dir = await _export(tmp_path)

        rows = PQ.read_table(snap_dir / "records.parquet").to_pylist()
        licences = [BusinessLicence.model_validate_json(json.dumps(row)) for row in rows]

        assert len(licences) == 3
        assert licences[0].business_name.value == "Joe's Cafe"
        assert licences[0].status.value is LicenceStatus.ACTIVE
        assert licences[2].status.value is LicenceStatus.CANCELLED

    async def test_reports_jsonl_keyed_by_source_record_id(self, tmp_path: Path) -> None:
        _, snap_dir = await _export(tmp_path)

        lines = [json.loads(line) for line in (snap_dir / "reports.jsonl").read_text().splitlines()]

        assert [line["source_record_id"] for line in lines] == [
            "1234567",
            "1234568",
            "1234569",
        ]

    async def test_schema_json_is_business_licence_schema(self, tmp_path: Path) -> None:
        _, snap_dir = await _export(tmp_path)

        on_disk = json.loads((snap_dir / "schema.json").read_text())

        assert on_disk == BusinessLicence.model_json_schema()

    async def test_quality_summary_reflects_known_redaction_in_fixture(
        self, tmp_path: Path
    ) -> None:
        manifest, _ = await _export(tmp_path)

        assert manifest.mapping_summary.quality_counts.get(FieldQuality.REDACTED, 0) >= 1
