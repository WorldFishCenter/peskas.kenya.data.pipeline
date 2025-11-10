# Validate Number of Boats

This function validates the `n_boats` column in the provided dataset. An
alert (error label) is triggered if the number of boats is an outlier,
determined by the `alert_outlier` function with specified parameters.
The alert number is stored in `alert_n_boats`. If an alert is triggered,
the `n_boats` value is set to `NA`.

## Usage

``` r
validate_nboats(data = NULL, k = NULL, flag_value = NULL)
```

## Arguments

- data:

  A data frame containing the `n_boats` column.

- k:

  a numeric value used in the LocScaleB function for outlier detection.

- flag_value:

  A numeric value to use as the flag for catches exceeding the upper
  bound.

## Value

A data frame with two columns: `n_boats` and `alert_n_boats`.

- `n_boats`: The original number of boats if valid, otherwise `NA`.

- `alert_n_boats`: A numeric value indicating the alert (error label)
  number.

## Examples

``` r
if (FALSE) { # \dontrun{
validate_nboats(data, k = 2, flag_value = 3)
} # }
```
