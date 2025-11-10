# Download and Process WCS Price Surveys from Kobotoolbox

This function retrieves survey data from Kobotoolbox for a specific
project, processes it, and uploads the raw data as a Parquet file to
Google Cloud Storage. It uses the `get_kobo_data` function, which is a
wrapper for `kobotools_kpi_data` from the KoboconnectR package.

## Usage

``` r
ingest_landings_price(
  versions = c("v1", "v2"),
  url = NULL,
  project_id = NULL,
  username = NULL,
  psswd = NULL,
  encoding = NULL
)
```

## Arguments

- versions:

  Character vector of versions to process (default: c("v1", "v2"))

- url:

  The URL of Kobotoolbox (default is NULL, uses value from
  configuration).

- project_id:

  The asset ID of the project to download data from (default is NULL,
  uses value from configuration).

- username:

  Username for Kobotoolbox account (default is NULL, uses value from
  configuration).

- psswd:

  Password for Kobotoolbox account (default is NULL, uses value from
  configuration).

- encoding:

  Encoding to be used for data retrieval (default is NULL, uses
  "UTF-8").

## Value

No return value. Function downloads data, processes it, and uploads to
Google Cloud Storage.

## Details

The function performs the following steps:

1.  Reads configuration settings.

2.  Downloads survey data from Kobotoolbox using `get_kobo_data`.

3.  Checks for uniqueness of submissions.

4.  Converts data to tabular format.

5.  Uploads raw data as a Parquet file to Google Cloud Storage.

Note that while parameters are provided for customization, the function
currently uses hardcoded values and configuration settings for some
parameters.

## Examples

``` r
if (FALSE) { # \dontrun{
# Process both versions
ingest_landings_price()

# Process only v1
ingest_landings_price(versions = "v1")

# Process specific versions
ingest_landings_price(versions = c("v1", "v2"))
} # }
```
