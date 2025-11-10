# Validate Total Catch Data using IQR method

Validate Total Catch Data using IQR method

## Usage

``` r
validate_total_catch_iqr(data = NULL, multiplier = 1.5, flag_value = NULL)
```

## Arguments

- data:

  A data frame containing required columns

- multiplier:

  multiplier for IQR range (default is 1.5)

- flag_value:

  A numeric value to use as the flag for catches exceeding bounds

## Value

A data frame with validated total catch data and alert flags
