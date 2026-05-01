"""End-to-end: Toronto pipeline → observers → drift.json.

Runs the real adapter + mapper against the fixture, attaches schema and
taxonomy observers via `attach_observers`, drives the pipeline through
the JSON exporter, then writes a drift artifact alongside.

Toronto has no taxonomy spec yet (Category is open-vocabulary across
hundreds of values; status is derived, not source-direct), so the
canary covers schema drift instead of taxonomy drift: an injected raw
record carrying an unexpected field must surface as an error finding.
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
    SchemaDriftKind,
    SchemaObserver,
    TaxonomyObserver,
)
from civix.core.identity.models.identifiers import DatasetId, Jurisdiction
from civix.core.pipeline import attach_observers, run
from civix.domains.business_licences.models.licence import BusinessLicence
from civix.infra.exporters.drift import write_drift
from civix.infra.exporters.json import write_snapshot
from civix.infra.sources.ca.toronto_business_licences import (
    DEFAULT_BASE_URL,
    TORONTO_BUSINESS_LICENCES_SCHEMA,
    TORONTO_TAXONOMIES,
    TorontoBusinessLicencesAdapter,
    TorontoBusinessLicencesMapper,
)

PINNED_NOW = datetime(2026, 4, 28, 12, 0, tzinfo=UTC)
DATASET = DatasetId("municipal-licensing-and-standards-business-licences-and-permits")
JURISDICTION = Jurisdiction(country="CA", region="ON", locality="Toronto")
PACKAGE_SHOW_URL = f"{DEFAULT_BASE_URL}package_show"
DATASTORE_SEARCH_URL = f"{DEFAULT_BASE_URL}datastore_search"

FIXTURES = Path(__file__).parent / "fixtures"


async def _run(tmp_path: Path) -> tuple[Path, dict[str, object]]:
    catalog = json.loads((FIXTURES / "catalog_response.json").read_text())
    page = json.loads((FIXTURES / "records_page.json").read_text())

    async with respx.mock(assert_all_called=True) as respx_mock:
        respx_mock.get(PACKAGE_SHOW_URL).mock(return_value=httpx.Response(200, json=catalog))
        respx_mock.get(DATASTORE_SEARCH_URL).mock(return_value=httpx.Response(200, json=page))

        async with httpx.AsyncClient() as client:
            adapter = TorontoBusinessLicencesAdapter(
                dataset_id=DATASET,
                jurisdiction=JURISDICTION,
                client=client,
                clock=lambda: PINNED_NOW,
            )
            mapper = TorontoBusinessLicencesMapper()
            schema_obs = SchemaObserver(spec=TORONTO_BUSINESS_LICENCES_SCHEMA)
            taxonomy_obs = TaxonomyObserver(specs=TORONTO_TAXONOMIES)

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


class TestTorontoDriftEndToEnd:
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

    async def test_taxonomy_drift_empty_when_no_specs_configured(self, tmp_path: Path) -> None:
        _, payload = await _run(tmp_path)
        taxonomy = _payload_section(payload, "taxonomy")

        assert taxonomy["findings"] == []
        assert taxonomy["has_errors"] is False

    async def test_unexpected_source_field_surfaces_as_schema_finding(self, tmp_path: Path) -> None:
        catalog = json.loads((FIXTURES / "catalog_response.json").read_text())
        page = json.loads((FIXTURES / "records_page.json").read_text())
        # Inject a record with a brand-new field the schema spec does not know.
        rows = list(page["result"]["records"])
        rows.append({**rows[0], "_id": 9999, "Licence No.": "B99-9999999", "Mystery Column": "abc"})
        page["result"]["records"] = rows
        page["result"]["total"] = len(rows)

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(PACKAGE_SHOW_URL).mock(return_value=httpx.Response(200, json=catalog))
            respx_mock.get(DATASTORE_SEARCH_URL).mock(return_value=httpx.Response(200, json=page))

            async with httpx.AsyncClient() as client:
                adapter = TorontoBusinessLicencesAdapter(
                    dataset_id=DATASET,
                    jurisdiction=JURISDICTION,
                    client=client,
                    clock=lambda: PINNED_NOW,
                )
                mapper = TorontoBusinessLicencesMapper()
                schema_obs = SchemaObserver(spec=TORONTO_BUSINESS_LICENCES_SCHEMA)

                pipeline_result = await run(adapter, mapper)
                pipeline_result = attach_observers(pipeline_result, [schema_obs])
                async for _ in pipeline_result.records:
                    pass
                report = schema_obs.finalize(pipeline_result.snapshot)

        unexpected = [f for f in report.findings if f.kind is SchemaDriftKind.UNEXPECTED_FIELD]

        assert len(unexpected) == 1
        finding = unexpected[0]

        assert finding.field_name == "Mystery Column"
        assert finding.severity is DriftSeverity.WARNING
        assert finding.sample_source_record_ids == ("B99-9999999",)
