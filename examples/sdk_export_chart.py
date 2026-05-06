"""Export an SDK dataset and build a small Altair chart.

This example hits Vancouver's live public open-data API.
Install the example visualization dependencies with:

    uv run --extra notebook python examples/sdk_export_chart.py
"""

from __future__ import annotations

import asyncio
import json
from collections import Counter
from pathlib import Path
from typing import Final

import altair as alt
import pandas as pd

from civix.domains.business_licences.models import BusinessLicence
from civix.infra.exporters.json import write_snapshot
from civix.sdk import Civix

OUTPUT_DIR: Final[Path] = Path("examples/out/vancouver_business_licences")
CHART_FILE: Final[Path] = OUTPUT_DIR / "licence_status_by_neighbourhood.svg"
TOP_NEIGHBOURHOOD_COUNT: Final[int] = 12


async def _export() -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    async with Civix() as client:
        product = client.datasets.ca.business_licences.licence.vancouver
        result = await client.fetch(product)
        manifest = await write_snapshot(
            result,
            output_dir=OUTPUT_DIR,
            record_type=BusinessLicence,
        )

    return OUTPUT_DIR / manifest.snapshot_id


def _read_records(records_path: Path) -> list[BusinessLicence]:
    return [
        BusinessLicence.model_validate_json(line)
        for line in records_path.read_text().splitlines()
        if line
    ]


def _chart_rows(records: list[BusinessLicence]) -> list[dict[str, object]]:
    neighbourhood_counts: Counter[str] = Counter()
    status_by_neighbourhood: Counter[tuple[str, str]] = Counter()

    for record in records:
        neighbourhood = record.neighbourhood.value or "Not provided"
        status = record.status.value.value if record.status.value is not None else "not_provided"

        neighbourhood_counts[neighbourhood] += 1
        status_by_neighbourhood[(neighbourhood, status)] += 1

    top_neighbourhoods = {
        name for name, _ in neighbourhood_counts.most_common(TOP_NEIGHBOURHOOD_COUNT)
    }

    return [
        {
            "neighbourhood": neighbourhood,
            "status": status,
            "licences": count,
        }
        for (neighbourhood, status), count in status_by_neighbourhood.items()
        if neighbourhood in top_neighbourhoods
    ]


def _save_chart(rows: list[dict[str, object]], *, chart_file: Path) -> None:
    data = pd.DataFrame(rows)
    status_order = list(data.groupby("status")["licences"].sum().sort_values().index)

    chart = (
        alt.Chart(data)
        .mark_bar()
        .encode(
            x=alt.X("sum(licences):Q", title="Licences"),
            y=alt.Y(
                "neighbourhood:N",
                sort="-x",
                title="Neighbourhood",
            ),
            color=alt.Color("status:N", sort=status_order, title="Status"),
            tooltip=[
                alt.Tooltip("neighbourhood:N", title="Neighbourhood"),
                alt.Tooltip("status:N", title="Status"),
                alt.Tooltip("sum(licences):Q", title="Licences"),
            ],
        )
        .properties(
            title="Vancouver business licences by neighbourhood and status",
            width=820,
            height=420,
        )
    )

    chart.save(str(chart_file))


async def main() -> None:
    snapshot_dir = await _export()
    records_path = snapshot_dir / "records.jsonl"
    manifest_path = snapshot_dir / "manifest.json"

    records = _read_records(records_path)
    rows = _chart_rows(records)
    _save_chart(rows, chart_file=CHART_FILE)

    manifest = json.loads(manifest_path.read_text())

    print(f"Exported {manifest['record_count']} records to {snapshot_dir}")
    print(f"Saved chart to {CHART_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
