# Validate Fisheries Data

This function imports and validates preprocessed fisheries data from
Google Cloud Storage. It conducts a series of validation checks to
ensure data integrity, including checks on dates, fisher counts, boat
numbers, and catch weights. The function then compiles the validated
data and corresponding alert flags, which are subsequently uploaded back
to Google Cloud Storage.

## Usage

``` r
validate_landings()
```

## Value

No return value. Function processes the data and uploads the validated
results as Parquet files to Google Cloud Storage.

## Details

The function performs the following main operations:

1.  Downloads preprocessed landings data from Google Cloud Storage.

2.  Validates the data for consistency and accuracy, focusing on:

    - Date validation

    - Number of fishers

    - Number of boats

    - Catch weight

3.  Generates a validated dataset that integrates the results of the
    validation checks.

4.  Creates alert flags to identify and track any data issues discovered
    during validation.

5.  Merges the validated data with additional metadata.

6.  Uploads the validated dataset and alert flags as Parquet files to
    Google Cloud Storage.

## Note

This function requires a configuration file with Google Cloud Storage
credentials and parameters for validation.
