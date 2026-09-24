# peskas.kenya.data.pipeline

R package for the Kenya node of Peskas: ingests, preprocesses, validates
and exports WCS catch and price surveys and KEFS surveys, matches
surveys to PDS trips, and feeds the Kenya app database, the validation
portal, the peskas-api bucket and the coasts portal. Ecosystem context
(other repos, data flow, cross-repo contracts): see PESKAS.md, loaded
via CLAUDE.local.md.

## Commands

``` r

devtools::load_all()
devtools::document()    # after any roxygen change; man/ is committed
devtools::check()
```

- Pipeline steps: read `.github/workflows/data-pipeline.yaml`
  (scheduled, one exported function per step).
- `.github/workflows/wcs-pipeline.yaml` runs the WCS chain alone (manual
  dispatch or push to WCS files; no cron).
- There is no `tests/` directory, so R-CMD-check and test-coverage check
  nothing behaviour-specific. Verify changes by running the function
  against the dev profile.

## Architecture

Two independent survey chains share the package: - **WCS** (catch +
price): `ingest_wcs_surveys` (legacy, v1 on eu.kobotoolbox.org, v2 on
kf.kobotoolbox.org) and `ingest_landings_price` -\>
`preprocess_legacy_landings`, `preprocess_landings_v1/v2`,
`preprocess_price_landings` -\> `merge_landings`, `merge_prices` -\>
`validate_landings` -\> `export_summaries`. Every WCS step reads and
writes `storage.google.options_wcs`: the `kenya-wcs-*` buckets in the
separate `peskas-wcs` GCP project. `WCS-GUIDE.md` is the guide for
external WCS collaborators. - **KEFS v2** (Kenya Fisheries Service, its
own Kobo server `kf.fims.kefs.go.ke`): `ingest_kefs_surveys_v2` -\>
`preprocess_kefs_surveys_v2` -\> `validate_kefs_surveys_v2`, which also
syncs KoBo validation status and pushes flags to Mongo `validation-*`. -
**KEFS v1** (`ingest_kefs_surveys_v1`, `preprocess_kefs_surveys_v1`)
still exists, but its job is commented out in `data-pipeline.yaml`. -
Joint steps (main pipeline only): `merge_trips` (surveys to PDS trips),
`export_api_raw/validated` (both chains to the peskas-api bucket),
`export_coasts_metrics` (cross-country portal artifacts), then
[`coasts::summarize_data`](https://rdrr.io/pkg/coasts/man/summarize_data.html),
[`coasts::generate_fleet_analysis`](https://rdrr.io/pkg/coasts/man/generate_fleet_analysis.html)
and
[`coasts::export_portal`](https://rdrr.io/pkg/coasts/man/export_portal.html)
with `package = "peskas.kenya.data.pipeline"`. - Outputs:
`export_summaries` writes Mongo `app[-dev]` (`dashboard_wcs`), read by
`peskas.kenya.bmu.dashboard`; `validate_kefs_surveys_v2` writes
`validation-*`, read by `peskas-validation`.

## Rules

- Keep the WCS chain inside `options_wcs`. Anything mixing WCS with
  KEFS, PDS or Airtable (API export, matched trips, portal aggregates)
  writes to `storage.google.options`, and cross-country artifacts go in
  `export_coasts_metrics`, never `export_summaries`.
- Never add a `schedule:` to `wcs-pipeline.yaml`. `data-pipeline.yaml`
  already writes the WCS prefixes; a second cron would race it, and
  since every read resolves `version = "latest"` the loser is silently
  shadowed mid-chain.
- Treat `wcs-surveys-validated` columns as a contract:
  `export_api_validated` and `merge_trips` read them.
- Prefer `coasts::` over the local copies of coasts helpers in
  `R/airtable.R` (`airtable_to_df`, `df_to_airtable`,
  `bulk_update_airtable`, `fetch_asset`, …) and `get_validation_status`
  / `update_validation_status` in `R/validation-functions.R`; flag the
  duplicate when you touch one.

## Gotchas

- WCS outputs use the `kenya_wcs_*` prefixes on purpose. The bare
  `kenya_fishery_metrics` and `kenya_monthly_summaries_map` prefixes
  belong to
  [`coasts::summarize_data()`](https://rdrr.io/pkg/coasts/man/summarize_data.html)
  /
  [`coasts::export_portal()`](https://rdrr.io/pkg/coasts/man/export_portal.html);
  writing a differently-keyed frame under them shadows the coasts portal
  input.
- `surveys.summaries.exclude_dashboard_ids` repeats `!expr` values
  instead of YAML anchors: undefined anchors silently resolved to a
  placeholder and excluded nothing.
- `airtable_to_df` is defined twice in this package (`R/airtable.R` and
  `R/ingestion.R`).
