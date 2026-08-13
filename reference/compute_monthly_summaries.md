# Monthly fishery summaries, gap-filled and with fishing days

Shared by
[`export_summaries()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/export_summaries.md)
(which pushes them to MongoDB) and
[`export_coasts_metrics()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/export_coasts_metrics.md)
(which derives the portal geo exports from them).

## Usage

``` r
compute_monthly_summaries(valid_data, bmu_size)
```

## Arguments

- valid_data:

  Validated WCS surveys.

- bmu_size:

  BMU sizes in km2.

## Value

A data frame of monthly metrics per BMU.
