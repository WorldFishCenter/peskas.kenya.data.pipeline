# Generate Composite Indicator Validation Flags

This function validates derived fisheries indicators (CPUE, RPUE, and
price per kg) from KEFS survey data. It calculates these performance
metrics and flags values that exceed specified maximum thresholds. This
validation is applied only to submissions that have passed previous
validation checks.

## Usage

``` r
get_indicators_flags(dat = NULL, limits = NULL, clean_ids = NULL)
```

## Arguments

- dat:

  A data frame containing KEFS survey data with columns:

  - submission_id: Unique identifier for the submission

  - no_of_fishers: Number of fishers on the trip

  - trip_duration: Duration of the trip in hours

  - total_catch_weight: Total weight of catch in kilograms

  - total_catch_price: Total price of catch in Kenyan Shillings

- limits:

  A list containing threshold values for validation:

  - max_cpue: Maximum acceptable catch per unit effort (kg/fisher/hour)

  - max_rpue: Maximum acceptable revenue per unit effort
    (KSH/fisher/hour)

  - max_price_kg: Maximum acceptable price per kilogram (KSH/kg)

- clean_ids:

  A vector of submission IDs that have passed previous validation
  checks. Only these submissions will be evaluated for indicator-based
  anomalies.

## Value

A data frame with columns:

- submission_id: The original submission identifier

- alert_flag_indicators: Comma-separated string of alert codes (NA if no
  alerts):

  - "6.1": CPUE (catch per unit effort) exceeds maximum

  - "6.2": RPUE (revenue per unit effort) exceeds maximum

  - "6.3": Price per kilogram exceeds maximum

## Details

The function calculates three key fisheries performance indicators:

1.  CPUE (Catch Per Unit Effort): total_catch_weight / no_of_fishers /
    trip_duration

2.  RPUE (Revenue Per Unit Effort): total_catch_price / no_of_fishers /
    trip_duration

3.  Price per kg: total_catch_price / total_catch_weight

These indicators help identify submissions with unrealistically high
productivity or pricing that may indicate data entry errors or
exceptional circumstances requiring review.

This function is typically called after trip-level and catch-level
validations have been performed, applying additional scrutiny only to
submissions that have already been determined to be generally valid.

## Examples

``` r
if (FALSE) { # \dontrun{
# Get IDs that passed previous validation
clean_ids <- dplyr::full_join(trip_flags, catch_flags, by = "submission_id") |>
  dplyr::filter(is.na(alert_flag_trip) & is.na(alert_flag_catch)) |>
  dplyr::pull(submission_id) |>
  unique()

indicator_limits <- list(
  max_cpue = 20,
  max_rpue = 3876,
  max_price_kg = 3876
)
indicator_flags <- get_indicators_flags(
  dat = survey_data,
  limits = indicator_limits,
  clean_ids = clean_ids
)
} # }
```
