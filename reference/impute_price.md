# Impute Missing Fish Prices Using Median Values

This function imputes missing fish prices in two steps:

1.  For fish with size (small/large): uses median price from other
    landing sites

2.  For fish with NA size: uses median between small and large sizes

## Usage

``` r
impute_price(price_table = NULL)
```

## Arguments

- price_table:

  A tibble containing fish price data with columns:

  - date: Date of the record

  - landing_site: Name of the landing site

  - fish_category: Type of fish

  - size: Size category of fish (large, small, or NA)

  - median_ksh_kg: Original price in Kenyan Shillings per kg

## Value

A tibble with the same structure as input, but with:

- All possible combinations of date, landing_site, fish_category and
  their valid sizes

- Original median_ksh_kg column removed

- New median_ksh_kg_imputed column containing original and imputed
  prices

## Examples

``` r
if (FALSE) { # \dontrun{
imputed_data <- impute_price(price_table = fish_prices)
} # }
```
