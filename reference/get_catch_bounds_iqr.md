# Get fish groups Catch Bounds using IQR method

Get fish groups Catch Bounds using IQR method

## Usage

``` r
get_catch_bounds_iqr(data = NULL, multiplier = 1.5)
```

## Arguments

- data:

  A data frame containing columns: gear, fish_category, catch_kg

- multiplier:

  multiplier for IQR range (default is 1.5)

## Value

A data frame with columns: gear, fish_category, upper.up
