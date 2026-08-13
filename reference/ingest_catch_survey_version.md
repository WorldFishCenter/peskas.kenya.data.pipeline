# Core ingestion logic for catch survey data

Core ingestion logic for catch survey data

## Usage

``` r
ingest_catch_survey_version(version, kobo_config, storage_config)
```

## Arguments

- version:

  Version identifier (e.g., "v1", "v2")

- kobo_config:

  Configuration object containing Kobo connection details

- storage_config:

  Configuration object containing storage details, including the
  destination bucket in `storage_config$options`.

## Value

No return value. Processes and uploads data.

## Details

This helper is shared by
[`ingest_wcs_surveys()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/ingest_wcs_surveys.md),
[`ingest_kefs_surveys_v1()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/ingest_kefs_surveys_v1.md)
and
[`ingest_kefs_surveys_v2()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/ingest_kefs_surveys_v2.md).
The destination bucket is therefore whatever the caller puts in
`storage_config$options` and is never chosen here: WCS callers pass
`options_wcs` (the dedicated peskas-wcs bucket), KEFS callers pass
`options`. Hardcoding a bucket here would put KEFS raw surveys into the
WCS-only bucket.
