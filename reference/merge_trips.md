# Merge Survey and GPS Trip Data

Runs the matching pipeline for all surveys (KEFS and WCS), combines the
results into a single country-level dataset, appends truly unmatched GPS
trips, and uploads to cloud storage.

## Usage

``` r
merge_trips(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  Logger threshold level. Default is
  [`logger::DEBUG`](https://daroczig.github.io/logger/reference/log_levels.html).

## Value

Invisible NULL. Uploads a merged parquet file to
`conf$surveys$matched_trips$file_prefix` containing:

- Matched survey-trip pairs (both submission_id and trip are non-NA)

- Unmatched surveys (trip = NA)

- Unmatched trips (submission_id = NA)

- Match quality indicators: n_fields_used, n_fields_ok, match_ok

- A `survey` column identifying the source ("kefs" or "wcs"; NA for
  unmatched trips)

## Details

The function executes a six-step pipeline:

1.  Load the shared device registry from cloud storage

2.  Load GPS trips from cloud storage

3.  Run
    [`compute_survey_matches()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/compute_survey_matches.md)
    for KEFS (uses registration number, boat name, and fisher name for
    fuzzy matching)

4.  Run
    [`compute_survey_matches()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/compute_survey_matches.md)
    for WCS (uses boat name only)

5.  Identify trips not claimed by either survey's matching output

6.  Combine all records and upload to
    `conf$surveys$matched_trips$file_prefix`

The device registry and GPS trips are loaded once and shared across both
survey pipelines.

## Examples

``` r
if (FALSE) { # \dontrun{
merge_trips()
} # }
```
