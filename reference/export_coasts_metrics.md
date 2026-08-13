# Export Kenya Metrics to the Cross-Country Coasts Bucket

Publishes the three Kenya artifacts consumed by the multi-country Peskas
portal, derived from validated WCS surveys:

- `kenya_fishery_metrics` – long-format fishery metrics

- `kenya_monthly_summaries_map` – monthly summaries with geography

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
can run the whole chain with no special configuration. The scheduled
pipeline calls both functions, so the portal keeps receiving the same
artifacts as before.

## Examples

``` r
if (FALSE) { # \dontrun{
export_coasts_metrics()
} # }
```
