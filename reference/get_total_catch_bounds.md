# Get Total Catch Bounds

Calculates the upper bounds for *total* catch data (using
`total_catch_kg`) based on landing site and gear type combinations. NA
values in total_catch_kg are filtered out before analysis. The function
groups data by combined landing_site and gear identifiers before
calculating bounds.

## Usage

``` r
get_total_catch_bounds(data = NULL, k = NULL)
```

## Arguments

- data:

  A data frame containing columns: `gear`, `landing_site`,
  `submission_id` and `total_catch_kg`.

- k:

  A numeric value used in the
  [`univOutl::LocScaleB`](https://rdrr.io/pkg/univOutl/man/LocScaleB.html)
  function for outlier detection.

## Value

A data frame with columns: `landing_site`, `gear` and `upper.up` (the
upper bound).
