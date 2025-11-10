# Validate Catch per Fisher

This function validates the relationship between total catch and number
of fishers. It flags cases where a single fisher reports a catch
exceeding the specified maximum. When flagged, the total catch value is
set to NA.

## Usage

``` r
validate_fishers_catch(data = NULL, max_kg = NULL, flag_value = NULL)
```

## Arguments

- data:

  A data frame containing the columns:

  - submission_id: Unique identifier for the submission

  - no_of_fishers: Number of fishers

  - total_catch_kg: Total catch in kilograms

- max_kg:

  Numeric value specifying the maximum catch (in kg) allowed for a
  single fisher

- flag_value:

  A numeric value to use as the flag for catches exceeding the maximum
  per fisher

## Value

A data frame with columns:

- submission_id: The original submission identifier

- total_catch_kg: The original catch if valid, otherwise NA

- alert_catch: Flag value if catch per fisher exceeds maximum, otherwise
  NA

## Examples

``` r
if (FALSE) { # \dontrun{
validate_fishers_catch(data, max_kg = 100, flag_value = 5)
} # }
```
