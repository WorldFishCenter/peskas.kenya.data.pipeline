# Get fish groups Catch Bounds

Calculates the upper bounds for *fish groups* catch data (using
`catch_kg`) based on gear type and fish category. Data is grouped by the
interaction of gear and fish category, and category "0" is excluded from
the analysis.

## Usage

``` r
get_catch_bounds(data = NULL, k = NULL)
```

## Arguments

- data:

  A data frame containing columns: `gear`, `fish_category`, `catch_kg`.

- k:

  A numeric value used in the
  [`univOutl::LocScaleB`](https://rdrr.io/pkg/univOutl/man/LocScaleB.html)
  function for outlier detection.

## Value

A data frame with columns: `gear`, `fish_category`, and `upper.up` (the
upper bound).
