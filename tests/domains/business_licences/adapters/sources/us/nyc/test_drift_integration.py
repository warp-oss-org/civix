"""End-to-end: NYC pipeline -> observers -> drift.json."""

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
    TaxonomyDriftKind,
    TaxonomyObserver,
)
from civix.core.identity.models.identifiers import DatasetId, Jurisdiction
from civix.core.pipeline import attach_observers, run
from civix.domains.business_licences.adapters.sources.us.nyc import (
    DEFAULT_BASE_URL,
    NYC_BUSINESS_LICENCES_SCHEMA,
    NYC_TAXONOMIES,
    NycBusinessLicencesAdapter,
    NycBusinessLicencesMapper,
)
from civix.domains.business_licences.models.licence import BusinessLicence
from civix.infra.exporters.drift import write_drift
from civix.infra.exporters.json import write_snapshot

PINNED_NOW = datetime(2026, 4, 30, 12, 0, tzinfo=UTC)
DATASET = DatasetId("w7w3-xahh")
JURISDICTION = Jurisdiction(country="US", region="NY", locality="New York")
RESOURCE_URL = f"{DEFAULT_BASE_URL}{DATASET}.json"

FIXTURES = Path(__file__).parent / "fixtures"


async def _run(tmp_path: Path) -> tuple[Path, dict[str, object]]:
    count = json.loads((FIXTURES / "count_response.json").read_text())
    page = json.loads((FIXTURES / "records_page.json").read_text())

    async with respx.mock(assert_all_called=True) as respx_mock:
        respx_mock.get(RESOURCE_URL).mock(
            side_effect=[
                httpx.Response(200, json=count),
                httpx.Response(200, json=page),
            ]
        )

        async with httpx.AsyncClient() as client:
            adapter = NycBusinessLicencesAdapter(
                dataset_id=DATASET,
                jurisdiction=JURISDICTION,
                client=client,
                clock=lambda: PINNED_NOW,
            )
            mapper = NycBusinessLicencesMapper()
            schema_obs = SchemaObserver(spec=NYC_BUSINESS_LICENCES_SCHEMA)
            taxonomy_obs = TaxonomyObserver(specs=NYC_TAXONOMIES)

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


class TestNycDriftEndToEnd:
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

    async def test_unexpected_source_field_surfaces_as_schema_finding(self) -> None:
        count = json.loads((FIXTURES / "count_response.json").read_text())
        page = json.loads((FIXTURES / "records_page.json").read_text())
        rows = list(page)
        rows.append({**rows[0], "license_nbr": "999999-DCA", "Mystery Column": "abc"})
        count[0]["count"] = str(len(rows))

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[
                    httpx.Response(200, json=count),
                    httpx.Response(200, json=rows),
                ]
            )

            async with httpx.AsyncClient() as client:
                adapter = NycBusinessLicencesAdapter(
                    dataset_id=DATASET,
                    jurisdiction=JURISDICTION,
                    client=client,
                    clock=lambda: PINNED_NOW,
                )
                mapper = NycBusinessLicencesMapper()
                schema_obs = SchemaObserver(spec=NYC_BUSINESS_LICENCES_SCHEMA)

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
        assert finding.sample_source_record_ids == ("999999-DCA",)

    async def test_unknown_license_status_value_surfaces_as_taxonomy_error(self) -> None:
        count = json.loads((FIXTURES / "count_response.json").read_text())
        page = json.loads((FIXTURES / "records_page.json").read_text())
        rows = list(page)
        rows.append({**rows[0], "license_nbr": "999998-DCA", "license_status": "Paused"})
        count[0]["count"] = str(len(rows))

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[
                    httpx.Response(200, json=count),
                    httpx.Response(200, json=rows),
                ]
            )

            async with httpx.AsyncClient() as client:
                adapter = NycBusinessLicencesAdapter(
                    dataset_id=DATASET,
                    jurisdiction=JURISDICTION,
                    client=client,
                    clock=lambda: PINNED_NOW,
                )
                mapper = NycBusinessLicencesMapper()
                taxonomy_obs = TaxonomyObserver(specs=NYC_TAXONOMIES)

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

        assert finding.taxonomy_id == "nyc-dcwp-license-status"
        assert finding.observed_value == "paused"
        assert finding.severity is DriftSeverity.ERROR
        assert finding.raw_samples == ("Paused",)
        assert report.has_errors

    async def test_unknown_license_type_value_surfaces_as_taxonomy_error(self) -> None:
        count = json.loads((FIXTURES / "count_response.json").read_text())
        page = json.loads((FIXTURES / "records_page.json").read_text())
        rows = list(page)
        rows.append({**rows[0], "license_nbr": "999997-DCA", "license_type": "Individual"})
        count[0]["count"] = str(len(rows))

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[
                    httpx.Response(200, json=count),
                    httpx.Response(200, json=rows),
                ]
            )

            async with httpx.AsyncClient() as client:
                adapter = NycBusinessLicencesAdapter(
                    dataset_id=DATASET,
                    jurisdiction=JURISDICTION,
                    client=client,
                    clock=lambda: PINNED_NOW,
                )
                mapper = NycBusinessLicencesMapper()
                taxonomy_obs = TaxonomyObserver(specs=NYC_TAXONOMIES)

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

        assert finding.taxonomy_id == "nyc-dcwp-license-type"
        assert finding.observed_value == "individual"
        assert finding.severity is DriftSeverity.ERROR
        assert finding.raw_samples == ("Individual",)
        assert report.has_errors
