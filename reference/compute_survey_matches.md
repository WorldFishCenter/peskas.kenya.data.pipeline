# Run Matching Pipeline for a Single Survey

Loads validated surveys for one survey type, runs fuzzy matching against
the device registry and GPS trips, and returns enriched records plus the
set of trip IDs that were claimed by the match. Called once per survey
by
[`merge_trips()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/merge_trips.md),
which combines results across all surveys.

## Usage

``` r
compute_survey_matches(survey, conf, registry, all_trips)
```

## Arguments

- survey:

  Character. Survey identifier: "kefs" or "wcs".

- conf:

  List. Configuration object from
  [`read_config()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/read_config.md).

- registry:

  Data frame. Device registry (pre-loaded by the caller).

- all_trips:

  Data frame. GPS trips (pre-loaded by the caller).

## Value

A list with two elements:

- **records**: Data frame of matched survey-trip pairs, surveys
  unmatched to any trip, and any multi-trip-day trip rows produced
  during matching. Includes a `survey` column identifying the source.

- **matched_trip_ids**: Character vector of trip IDs that appeared in
  the matching output (used by the caller to identify truly unmatched
  trips).
