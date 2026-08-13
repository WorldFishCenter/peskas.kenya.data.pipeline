# Load the validated WCS surveys and BMU sizes used by the summary exports

Shared by
[`export_summaries()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/export_summaries.md)
and
[`export_coasts_metrics()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/export_coasts_metrics.md)
so the two can run independently without duplicating the download, the
metadata lookup or the landing-site filter.

## Usage

``` r
load_wcs_summary_inputs(conf)
```

## Arguments

- conf:

  Configuration object from
  [`read_config()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/read_config.md).

## Value

A list with `valid_data` (validated WCS surveys from 2023 onwards,
restricted to landing sites present in the BMU metadata) and `bmu_size`.
