# Merge Survey and GPS Trip Data

End-to-end workflow for matching KEFS catch surveys to PDS GPS trips for
Kenya data. Loads device registry, validated surveys, and GPS trips,
then performs fuzzy matching, merges all records (matched and
unmatched), and uploads the result to cloud storage.

## Usage

``` r
merge_trips(conf)
```

## Arguments

- conf:

  Configuration list from
  [`read_config()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/read_config.md)
  containing:

  - metadata\$airtable\$assets: Path to device registry

  - surveys\$kefs\$v2\$validated\$file_prefix: Path to validated surveys

  - surveys\$kefs\$v2\$merged: Output path for merged data

  - pds\$pds_trips\$file_prefix: Path to preprocessed PDS trips data

  - pds\$pds_trips\$version: Version of PDS trips data to use

  - storage\$google: Cloud storage settings (key, options,
    options_coasts)

## Value

Invisible NULL. The function uploads a parquet file to cloud storage
containing all merged records with the following structure:

- submission_id: Survey identifier (NA for unmatched trips)

- landing_date: Landing date

- imei: Device IMEI

- n_fields_used, n_fields_ok, match_ok: Match quality indicators

- trip: GPS trip identifier (NA for unmatched surveys)

- started, ended: Trip timestamps

- registration_number_survey, registration_number_trip

- boat_name_survey, boat_name_trip

- fisher_name_survey, fisher_name_trip

- Additional survey and trip metadata

The uploaded dataset includes:

- Matched survey-trip pairs (where both submission_id and trip are
  non-NA)

- Unmatched surveys (trip = NA)

- Unmatched trips (submission_id = NA)

## Details

The function executes the following pipeline:

1.  **Load device registry**: Downloads Airtable assets from cloud
    storage and filters for Kenya devices (WorldFish - Kenya, Kenya,
    Kenya AABS)

2.  **Load validated surveys**: Downloads preprocessed and validated
    KEFS v2 surveys from cloud storage

3.  **Load GPS trips**: Downloads preprocessed PDS trips data from cloud
    storage using configured file prefix and version

4.  **Filter PDS surveys**: Selects only surveys where pds = "yes" or
    "pds"

5.  **Match surveys to trips**: Runs
    [`match_surveys_to_gps_trips()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/match_surveys_to_gps_trips.md)
    with two-step fuzzy matching (surveys -\> registry -\> trips)

6.  **Merge with full datasets**: Combines matched subset with all
    unmatched surveys and trips

7.  **Upload to cloud storage**: Saves merged data as versioned parquet
    file

## Logging

The function logs progress at each step:

- Loading device registry

- Loading validated surveys

- Loading GPS trips from cloud storage

- Number of PDS surveys being matched

- Merging with full datasets

- Final counts (total records and matched pairs)

- Uploading merged data to cloud storage

## Pipeline Integration

This function is typically run after:

1.  [`ingest_kefs_surveys_v2()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/ingest_kefs_surveys_v2.md) -
    Downloads raw data from Kobo

2.  [`preprocess_kefs_surveys_v2()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/preprocess_kefs_surveys_v2.md) -
    Cleans and standardizes surveys

3.  [`validate_kefs_surveys_v2()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/validate_kefs_surveys_v2.md) -
    Validates catch data

4.  [`ingest_pds_trips()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/ingest_pds_trips.md)
    and
    [`preprocess_pds_tracks()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/preprocess_pds_tracks.md) -
    Preprocessed PDS trips data must be available in cloud storage

The output can be downloaded using:

    merged_data <- download_parquet_from_cloud(
      prefix = conf$surveys$kefs$v2$merged,
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    )

## Examples

``` r
if (FALSE) { # \dontrun{
# Standard usage - merges and uploads to cloud
conf <- read_config()
merge_trips(conf)

# Download the merged data to analyze
merged_data <- download_parquet_from_cloud(
  prefix = conf$surveys$kefs$v2$merged,
  provider = conf$storage$google$key,
  options = conf$storage$google$options
)

# Count matched vs unmatched
table(
  survey = !is.na(merged_data$submission_id),
  trip = !is.na(merged_data$trip)
)
} # }
```
