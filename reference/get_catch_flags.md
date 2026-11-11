# Generate Catch-Level Validation Flags

This function validates catch-level data from KEFS surveys and generates
alert flags for inconsistencies in sample weights relative to total
catch weights. It focuses on submissions where catch outcome was "yes"
and checks for logical inconsistencies in the relationship between
sample weights and total weights.

## Usage

``` r
get_catch_flags(dat = NULL, species_lengths, max_length_cm = 500)
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

  - sample_alpha3_code, sample_scientific_name: The row's species

  - length_type: How the fish were measured (e.g. `total_length`)

  - length_min_cm, length_max_cm: Smallest and largest fish measured for
    the row's species, if any

- species_lengths:

  The hub table of FishBase traits written by
  [`coasts::enrich_taxa()`](https://rdrr.io/pkg/coasts/man/enrich_taxa.html),
  with `alpha3_code`, `original_name`, `server`, `length_max_tl` and
  `length_common_tl`.

- max_length_cm:

  Numeric. Absolute upper bound on `length_max_cm`, in centimetres, for
  every species. Defaults to 500. The largest and smallest fish are
  tested rather than the mean, which one impossible fish among normal
  ones can stay inside.

## Value

A data frame with columns:

- submission_id: The original submission identifier

- alert_flag_catch: Comma-separated string of alert codes (NA if no
  alerts):

  - "5.1": Total sample weight exceeds total catch weight

  - "5.2": Individual sample weight exceeds either total sample weight
    or total catch weight

  - "5.3": A fish is longer than its species grows, or than
    `max_length_cm`

  - "5.4": A fish is shorter than its species plausibly lands

- alert_catches: For 5.2, 5.3 and 5.4, the species sample each was
  raised on: its number in the form (`catch_number`, 1 for the first
  species), `code`, `species` and the `value` recorded. NULL when none
  was.

## Details

The function only processes submissions where catch_outcome is "yes". It
identifies two types of weight inconsistencies:

1.  The sum of all samples exceeds the total reported catch

2.  An individual sample weighs more than the total it's supposed to be
    part of

Alerts 5.3 and 5.4 bound each fish by its taxon with
[`coasts::get_length_limits()`](https://rdrr.io/pkg/coasts/man/get_length_limits.html),
the rule Zanzibar and Mozambique use for their alerts 4 and 3. The
limits come from FishBase species only, so they bound fish whatever
length type was ticked; rays, crabs, lobsters and cephalopods keep the
`max_length_cm` ceiling.

## Examples

``` r
if (FALSE) { # \dontrun{
catch_flags <- get_catch_flags(dat = survey_data, species_lengths = traits)
} # }
```
