# Validate Individual Catch Data

Compares each fish group catch (in `catch_kg`) to the upper bounds and
flags values that exceed the bound. Values exceeding bounds are set to
NA in the catch_kg column.

## Usage

``` r
validate_catch(data = NULL, k = NULL, flag_value = NULL)
```

## Arguments

- data:

  A data frame containing columns: `catch_id`, `gear`, `fish_category`,
  `catch_kg`.

- k:

  A numeric value passed to
  [`get_catch_bounds`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_catch_bounds.md)
  for outlier detection.

- flag_value:

  A numeric value to use as the flag for catches exceeding the upper
  bound. Default is 4.

## Value

A data frame with columns: `submission_id`, `catch_id`, `catch_kg`, and
`alert_catch`.
