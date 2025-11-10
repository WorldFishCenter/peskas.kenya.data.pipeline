# Validate Number of Fishers

This function validates the `no_of_fishers` column in the provided
dataset. An alert (error label) is triggered if the number of fishers is
an outlier, determined by the `alert_outlier` function with specified
parameters. The alert number is stored in `alert_n_fishers`. If an alert
is triggered, the `no_of_fishers` value is set to `NA`.

## Usage

``` r
validate_nfishers(data = NULL, k = NULL, flag_value = NULL)
```

## Arguments

- data:

  A data frame containing the `no_of_fishers` column.

- k:

  a numeric value used in the LocScaleB function for outlier detection.

- flag_value:

  A numeric value to use as the flag for catches exceeding the upper
  bound.

## Value

A data frame with two columns: `no_of_fishers` and `alert_n_fishers`.

- `no_of_fishers`: The original number of fishers if valid, otherwise
  `NA`.

- `alert_n_fishers`: A numeric value indicating the alert (error label)
  number.

## Examples

``` r
if (FALSE) { # \dontrun{
validate_nfishers(data, k = 3, flag_value = 2)
} # }
```
