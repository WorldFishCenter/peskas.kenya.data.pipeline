# Validate Number of Boats using IQR method

Validate Number of Boats using IQR method

## Usage

``` r
validate_nboats_iqr(data = NULL, multiplier = 1.5, flag_value = NULL)
```

## Arguments

- data:

  A data frame containing the n_boats column

- multiplier:

  multiplier for IQR range (default is 1.5)

- flag_value:

  A numeric value to use as the flag for values outside bounds

## Value

A data frame with validated n_boats and alert flags

## Examples

``` r
if (FALSE) { # \dontrun{
validate_nboats_iqr(data, multiplier = 1.5, flag_value = 8)
} # }
```
