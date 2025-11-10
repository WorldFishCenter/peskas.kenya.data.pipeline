# Summarize Catch Price Data

This function aggregates catch price data by a specified time unit,
calculating median prices per kilogram for each fish category at each
landing site.

## Usage

``` r
summarise_catch_price(data = NULL, unit = NULL)
```

## Arguments

- data:

  A tibble containing catch price data with columns: landing_date,
  landing_site, fish_category, and ksh_kg

- unit:

  Character string specifying the time unit for aggregation (e.g.,
  "year", "month", "week"). Passed to lubridate::floor_date()

## Value

A tibble containing summarized price data with columns: date,
landing_site, fish_category, size, and median_ksh_kg

## Details

The function:

1.  Floors dates to the specified unit using lubridate

2.  Groups data by date, landing site, fish category and size

3.  Calculates median price per kilogram for each group

## Examples

``` r
if (FALSE) { # \dontrun{
summarise_catch_price(data = price_data, unit = "year")
summarise_catch_price(data = price_data, unit = "month")
} # }
```
