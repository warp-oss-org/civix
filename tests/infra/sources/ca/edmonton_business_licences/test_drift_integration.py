"""End-to-end: Edmonton pipeline -> observers -> drift.json."""

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
from civix.domains.business_licences.models.licence import BusinessLicence
from civix.infra.exporters.drift import write_drift
from civix.infra.exporters.json import write_snapshot
from civix.infra.sources.ca.edmonton_business_licences import (
    DEFAULT_BASE_URL,
    EDMONTON_BUSINESS_LICENCES_SCHEMA,
    EDMONTON_TAXONOMIES,
    EdmontonBusinessLicencesAdapter,
    EdmontonBusinessLicencesMapper,
)

PINNED_NOW = datetime(2026, 4, 30, 12, 0, tzinfo=UTC)
DATASET = DatasetId("qhi4-bdpu")
JURISDICTION = Jurisdiction(country="CA", region="AB", locality="Edmonton")
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
            adapter = EdmontonBusinessLicencesAdapter(
                dataset_id=DATASET,
                jurisdiction=JURISDICTION,
                client=client,
                clock=lambda: PINNED_NOW,
            )
            mapper = EdmontonBusinessLicencesMapper()
            schema_obs = SchemaObserver(spec=EDMONTON_BUSINESS_LICENCES_SCHEMA)
            taxonomy_obs = TaxonomyObserver(specs=EDMONTON_TAXONOMIES)

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


class TestEdmontonDriftEndToEnd:
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
        rows.append({**rows[0], "externalid": "999999-001", "Mystery Column": "abc"})
        count[0]["count"] = str(len(rows))

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[
                    httpx.Response(200, json=count),
                    httpx.Response(200, json=rows),
                ]
            )

            async with httpx.AsyncClient() as client:
                adapter = EdmontonBusinessLicencesAdapter(
                    dataset_id=DATASET,
                    jurisdiction=JURISDICTION,
                    client=client,
                    clock=lambda: PINNED_NOW,
                )
                mapper = EdmontonBusinessLicencesMapper()
                schema_obs = SchemaObserver(spec=EDMONTON_BUSINESS_LICENCES_SCHEMA)

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
        assert finding.sample_source_record_ids == ("999999-001",)

    async def test_unknown_licence_type_value_surfaces_as_taxonomy_error(self) -> None:
        count = json.loads((FIXTURES / "count_response.json").read_text())
        page = json.loads((FIXTURES / "records_page.json").read_text())
        rows = list(page)
        rows.append({**rows[0], "externalid": "999998-001", "licencetype": "Temporary"})
        count[0]["count"] = str(len(rows))

        async with respx.mock(assert_all_called=True) as respx_mock:
            respx_mock.get(RESOURCE_URL).mock(
                side_effect=[
                    httpx.Response(200, json=count),
                    httpx.Response(200, json=rows),
                ]
            )

            async with httpx.AsyncClient() as client:
                adapter = EdmontonBusinessLicencesAdapter(
                    dataset_id=DATASET,
                    jurisdiction=JURISDICTION,
                    client=client,
                    clock=lambda: PINNED_NOW,
                )
                mapper = EdmontonBusinessLicencesMapper()
                taxonomy_obs = TaxonomyObserver(specs=EDMONTON_TAXONOMIES)

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

        assert finding.taxonomy_id == "edmonton-business-licence-type"
        assert finding.observed_value == "temporary"
        assert finding.severity is DriftSeverity.ERROR
        assert finding.raw_samples == ("Temporary",)
        assert report.has_errors
