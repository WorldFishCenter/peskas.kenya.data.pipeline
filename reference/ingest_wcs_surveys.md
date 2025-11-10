# Download and Process WCS Catch Surveys from Kobotoolbox

This function retrieves WCS survey data (v1 and v2) from Kobotoolbox,
processes it, and uploads the raw data as Parquet files to Google Cloud
Storage. This function retrieves WCS survey data (v1 and v2) from
Kobotoolbox, processes it, and uploads the raw data as Parquet files to
Google Cloud Storage.

## Usage

``` r
ingest_wcs_surveys(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  Logging threshold level (default: logger::DEBUG)

## Value

No return value. Function downloads data, processes it, and uploads to
Google Cloud Storage.

## Details

The function performs the following steps:

1.  Reads configuration settings.

2.  Downloads survey data from Kobotoolbox using `get_kobo_data`.

3.  Checks for uniqueness of submissions.

4.  Converts data to tabular format.

5.  Uploads raw data as Parquet files to Google Cloud Storage.

6.  Uploads raw data as Parquet files to Google Cloud Storage.

This function processes WCS v1 (eu.kobotoolbox.org) and v2
(kf.kobotoolbox.org) catch surveys. This function processes WCS v1
(eu.kobotoolbox.org) and v2 (kf.kobotoolbox.org) catch surveys.

## Examples

``` r
if (FALSE) { # \dontrun{
ingest_wcs_surveys()
} # }
```
