# Civix SDK Examples

These examples demonstrate the public SDK against live public civic APIs.
They fetch Vancouver business licences through `Civix`, preserve the raw
snapshot boundary, map records into `BusinessLicence`, and write local
artifacts under `examples/out/`.

## Fetch And Preview

```bash
uv run python examples/sdk_fetch_preview.py
```

Expected output:

- snapshot source, dataset, jurisdiction, fetch time, and source record count
- the first few normalized business licence records
- aggregate mapping quality counts
- aggregate unmapped source-field counts

## Export And Chart

```bash
uv run --extra notebook python examples/sdk_export_chart.py
```

Expected output:

- a JSON snapshot directory under `examples/out/vancouver_business_licences/`
- `records.jsonl`, `reports.jsonl`, `schema.json`, and `manifest.json`
- an Altair chart saved to `examples/out/vancouver_business_licences/licence_status_by_neighbourhood.svg`

The chart example uses Altair from the `notebook` optional extra. The
core SDK does not depend on visualization libraries.

The notebook displays the same Altair chart inline as SVG using
`vl-convert-python`; the script saves the SVG file for command-line use.
