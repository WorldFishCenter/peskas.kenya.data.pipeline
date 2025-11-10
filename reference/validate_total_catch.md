# Validate Total Catch Data

Compares the total catch (in `total_catch_kg`) to the upper bounds and
flags values that exceed the bound. Values exceeding bounds are set to
NA in the total_catch_kg column. Bounds are calculated based on landing
site and gear type combinations.

## Usage

``` r
validate_total_catch(data = NULL, k = NULL, flag_value = NULL)
```

## Arguments

- data:

  A data frame containing columns: `submission_id`, `landing_site`,
  `gear`, `total_catch_kg`.

- k:

  A numeric value passed to
  [`get_total_catch_bounds`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_total_catch_bounds.md)
  for outlier detection.

- flag_value:

  A numeric value to use as the flag for catches exceeding the upper
  bound. Default is 4.

## Value

A data frame with columns: `submission_id`, `total_catch_kg`, and
`alert_catch`.
