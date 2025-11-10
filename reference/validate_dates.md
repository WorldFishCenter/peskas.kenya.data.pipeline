# Validate Landing Dates

This function checks the validity of the `landing_date` in the provided
dataset. If the `landing_date` is before 1990-01-01, an alert (error
label) with the number 1 is triggered. The `landing_date` is then set to
`NA` for those records.

## Usage

``` r
validate_dates(data = NULL, flag_value = NULL)
```

## Arguments

- data:

  A data frame containing the `landing_date` column.

- flag_value:

  A numeric value to use as the flag for catches exceeding the upper
  bound.

## Value

A data frame with two columns: `landing_date` and `alert_date`.

- `landing_date`: The original date if valid, otherwise `NA`.

- `alert_date`: A numeric value indicating the alert (error label)
  number, where 1 represents an invalid date.

## Examples

``` r
if (FALSE) { # \dontrun{
validate_dates(data, flag_value = 1)
} # }
```
