# Export Kenya Metrics to the Cross-Country Coasts Bucket

Publishes the three WCS-derived Kenya artifacts that live in the shared
coasts bucket:

- `kenya_wcs_fishery_metrics` – long-format fishery metrics, keyed by
  landing site and species

- `kenya_wcs_region_summaries_map` – monthly summaries aggregated to WCS
  coast regions

- `KE_regions` – region boundaries as GeoJSON

## Usage

``` r
export_coasts_metrics(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  The logging threshold level (default:
  [`logger::DEBUG`](https://daroczig.github.io/logger/reference/log_levels.html)).

## Value

No return value. Uploads three files to the coasts bucket.

## Details

Split out from
[`export_summaries()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/export_summaries.md)
because these are the only outputs of the WCS chain that land outside
the WCS bucket. They go to `storage.google.options_coasts`
(`peskas-coasts*`), the shared bucket holding the equivalent Mozambique,
Timor and Zanzibar artifacts, so writing them requires access no
WCS-only collaborator has.

Keeping them here means
[`export_summaries()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/export_summaries.md)
touches nothing but `options_wcs` and MongoDB, and a WCS collaborator
can run the whole chain with no special configuration.

## Prefixes owned by peskas.coasts

These outputs are deliberately namespaced `kenya_wcs_*`. The bare
`kenya_fishery_metrics` and `kenya_monthly_summaries_map` prefixes
belong to
[`coasts::summarize_data()`](https://rdrr.io/pkg/coasts/man/summarize_data.html)
and
[`coasts::export_portal()`](https://rdrr.io/pkg/coasts/man/export_portal.html),
which build the GAUL-keyed frames that
[`coasts::export_geos()`](https://rdrr.io/pkg/coasts/man/export_geos.html)
binds together with the Zanzibar and Mozambique equivalents to drive the
coasts portal. Writing a differently-keyed frame under those names
shadows them, because every read resolves by `version = "latest"`.

## Examples

``` r
if (FALSE) { # \dontrun{
export_coasts_metrics()
} # }
```
