# Generate Trip-Level Validation Flags

This function validates trip-level characteristics from KEFS survey data
and generates alert flags for anomalous values. It checks for
unrealistic or inconsistent values in horse power, number of fishers,
trip duration, and total catch price.

## Usage

``` r
get_trips_flags(dat = NULL, limits = NULL)
```

## Arguments

- dat:

  A data frame containing KEFS survey trip data with columns:

  - submission_id: Unique identifier for the submission

  - hp: Horse power of the boat

  - no_of_fishers: Number of fishers on the trip

  - trip_duration: Duration of the trip in hours

  - catch_outcome: Whether catch was recorded ("yes" or "no")

  - total_catch_price: Total price of catch in Kenyan Shillings

  - total_catch_weight: Total weight of catch in kilograms

  - mesh_size: Mesh size used (if applicable)

- limits:

  A list containing threshold values for validation:

  - max_hp: Maximum acceptable horse power

  - max_n_fishers: Maximum acceptable number of fishers

  - max_trip_duration: Maximum acceptable trip duration in hours

  - max_revenue: Maximum acceptable revenue in Kenyan Shillings

## Value

A data frame with columns:

- submission_id: The original submission identifier

- alert_flag_trip: Comma-separated string of alert codes (NA if no
  alerts):

  - "1": Horse power is \<= 0 or exceeds maximum

  - "2": Number of fishers is \<= 0 or exceeds maximum

  - "3": Trip duration is \<= 0 or exceeds maximum

  - "4.1": Catch outcome is "yes" but total catch price is \<= 0

  - "4.2": Catch outcome is "no" but total catch price is \>= 0

  - "4.3": Total catch price exceeds maximum revenue limit

## Examples

``` r
if (FALSE) { # \dontrun{
trip_limits <- list(
  max_hp = 150,
  max_n_fishers = 100,
  max_trip_duration = 96,
  max_revenue = 387600
)
trip_flags <- get_trips_flags(dat = survey_data, limits = trip_limits)
} # }
```
