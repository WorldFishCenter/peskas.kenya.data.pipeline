# Validate Number of Fishers using IQR method

Validate Number of Fishers using IQR method

## Usage

``` r
validate_nfishers_iqr(data = NULL, multiplier = 1.5, flag_value = NULL)
```

## Arguments

- data:

  A data frame containing the no_of_fishers column

- multiplier:

  multiplier for IQR range (default is 1.5)

- flag_value:

  A numeric value to use as the flag for values outside bounds

## Value

A data frame with validated no_of_fishers and alert flags

## Examples

``` r
if (FALSE) { # \dontrun{
validate_nfishers_iqr(data, multiplier = 1.5, flag_value = 7)
} # }
```
