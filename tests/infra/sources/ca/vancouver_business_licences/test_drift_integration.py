"""End-to-end: Vancouver pipeline → observers → drift.json.

Runs the real adapter + mapper against the fixture, attaches schema and
taxonomy observers via `attach_observers`, drives the pipeline through
the JSON exporter, then writes a drift artifact alongside.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import httpx
import respx

from civix.core.drift import (
    DriftSeverity,
    SchemaObserver,
    TaxonomyDriftKind,
    TaxonomyObserver,
)
from civix.core.identity.models.identifiers import DatasetId, Jurisdiction
from civix.core.pipeline import attach_observers, run
from civix.domains.business_licences.models.licence import BusinessLicence
from civix.infra.exporters.drift import write_drift
from civix.infra.exporters.json import write_snapshot
from civix.infra.sources.ca.vancouver_business_licences import (
    DEFAULT_BASE_URL,
    VANCOUVER_BUSINESS_LICENCES_SCHEMA,
    VANCOUVER_TAXONOMIES,
    VancouverBusinessLicencesAdapter,
    VancouverBusinessLicencesMapper,
)

PINNED_NOW = datetime(2026, 4, 25, 12, 0, tzinfo=UTC)
DATASET = DatasetId("business-licences")
JURISDICTION = Jurisdiction(country="CA", region="BC", locality="Vancouver")
RECORDS_URL = f"{DEFAULT_BASE_URL}catalog/datasets/{DATASET}/records"
EXPORTS_URL = f"{DEFAULT_BASE_URL}catalog/datasets/{DATASET}/exports/jsonl"

FIXTURES = Path(__file__).parent / "fixtures"


async def _run(tmp_path: Path) -> tuple[Path, dict[str, object]]:
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
            schema_obs = SchemaObserver(spec=VANCOUVER_BUSINESS_LICENCES_SCHEMA)
            taxonomy_obs = TaxonomyObserver(specs=VANCOUVER_TAXONOMIES)

            pipeline_result = await run(adapter, mapper)
            pipeline_result = attach_observers(pipeline_result, [schema_obs, taxonomy_obs])
            manifest = await write_snapshot(
                pipeline_result, output_dir=tmp_path, record_type=BusinessLicence
            )

            schema_report = schema_obs.finalize(pipeline_result.snapshot)
            taxonomy_report = taxonomy_obs.finalize(pipeline_result.snapshot)
            snap_dir = tmp_path / manifest.snapshot_id
            write_drift(snapshot_dir=snap_dir, schema=schema_report, taxonomy=taxonomy_report)

    return snap_dir, json.loads((snap_dir / "drift.json").read_text())


def _payload_section(payload: dict[str, object], section_name: str) -> dict[str, Any]:
    section = payload[section_name]

    assert isinstance(section, dict)

    return cast("dict[str, Any]", section)


class TestVancouverDriftEndToEnd:
    async def test_drift_json_written_alongside_records(self, tmp_path: Path) -> None:
        snap_dir, _ = await _run(tmp_path)

        assert {p.name for p in snap_dir.iterdir()} >= {
            "records.jsonl",
            "reports.jsonl",
            "schema.json",
            "manifest.json",
            "drift.json",
        }

    async def test_schema_drift_clean_against_pinned_spec(self, tmp_path: Path) -> None:
        _, payload = await _run(tmp_path)
        schema = _payload_section(payload, "schema")

        assert schema["findings"] == []
        assert schema["has_errors"] is False

    async def test_taxonomy_drift_clean_for_known_fixture_values(self, tmp_path: Path) -> None:
        _, payload = await _run(tmp_path)
        taxonomy = _payload_section(payload, "taxonomy")

        assert taxonomy["findings"] == []
        assert taxonomy["has_errors"] is False

    async def test_unknown_status_value_surfaces_as_error(self, tmp_path: Path) -> None:
        # Inject one record with a status the spec doesn't know.
        rows = [
            json.loads(line)
            for line in (FIXTURES / "records.jsonl").read_text().splitlines()
            if line
        ]
        rows.append({**rows[0], "licencersn": "9999999", "status": "Surrendered"})
        body = "\n".join(json.dumps(r) for r in rows)
        count_payload: dict[str, object] = {"total_count": len(rows), "results": []}

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RECORDS_URL).mock(return_value=httpx.Response(200, json=count_payload))
            respx_mock.get(EXPORTS_URL).mock(return_value=httpx.Response(200, text=body))

            async with httpx.AsyncClient() as client:
                adapter = VancouverBusinessLicencesAdapter(
                    dataset_id=DATASET,
                    jurisdiction=JURISDICTION,
                    client=client,
                    clock=lambda: PINNED_NOW,
                )
                mapper = VancouverBusinessLicencesMapper()
                taxonomy_obs = TaxonomyObserver(specs=VANCOUVER_TAXONOMIES)

                pipeline_result = await run(adapter, mapper)
                pipeline_result = attach_observers(pipeline_result, [taxonomy_obs])
                async for _ in pipeline_result.records:
                    pass
                report = taxonomy_obs.finalize(pipeline_result.snapshot)

        unrecognized = [
            f for f in report.findings if f.kind is TaxonomyDriftKind.UNRECOGNIZED_VALUE
        ]

        assert len(unrecognized) == 1
        finding = unrecognized[0]

        assert finding.taxonomy_id == "vancouver-business-licence-status"
        assert finding.observed_value == "surrendered"
        assert finding.severity is DriftSeverity.ERROR
        assert finding.raw_samples == ("Surrendered",)
        assert report.has_errors
