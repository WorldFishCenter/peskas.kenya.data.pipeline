# Generate Catch-Level Validation Flags

This function validates catch-level data from KEFS surveys and generates
alert flags for inconsistencies in sample weights relative to total
catch weights. It focuses on submissions where catch outcome was "yes"
and checks for logical inconsistencies in the relationship between
sample weights and total weights.

## Usage

``` r
get_catch_flags(dat = NULL, max_length_cm = 500)
```

## Arguments

- dat:

  A data frame containing KEFS survey catch data with columns:

  - submission_id: Unique identifier for the submission

  - catch_outcome: Whether catch was recorded ("yes" or "no")

  - total_sample_weight: Total weight of all samples in kilograms

  - total_catch_weight: Total weight of the entire catch in kilograms

  - sample_weight: Weight of individual sample in kilograms

  - sample_price: Price of the sample

  - length_cm: Mean measured length for the row's species, if any

- max_length_cm:

  Numeric. Absolute upper bound on `length_cm`, in centimetres. Defaults
  to 500.

## Value

A data frame with columns:

- submission_id: The original submission identifier

- alert_flag_catch: Comma-separated string of alert codes (NA if no
  alerts):

  - "5.1": Total sample weight exceeds total catch weight

  - "5.2": Individual sample weight exceeds either total sample weight
    or total catch weight

  - "5.3": Length exceeds `max_length_cm`

## Details

The function only processes submissions where catch_outcome is "yes". It
identifies two types of weight inconsistencies:

1.  The sum of all samples exceeds the total reported catch

2.  An individual sample weighs more than the total it's supposed to be
    part of

Alert 5.3 is an absolute ceiling, not a per-species check. Zanzibar and
Mozambique bound length per taxon from FishBase morphometrics; Kenya has
no morphometrics table, so there is no species-specific bound to apply
here. The default only catches the physically impossible

## Examples

``` r
if (FALSE) { # \dontrun{
catch_flags <- get_catch_flags(dat = survey_data)
} # }
```
