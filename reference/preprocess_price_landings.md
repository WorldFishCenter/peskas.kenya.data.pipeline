# Preprocess Price Data

This function preprocesses raw price data from Google Cloud Storage. It
performs various data cleaning and transformation operations, including
column renaming, data pivoting, and standardization of fish categories
and prices.

## Usage

``` r
preprocess_price_landings(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  Logging threshold level (default: logger::DEBUG)

## Value

No return value. Function processes the data and uploads the result as a
Parquet file to Google Cloud Storage.

## Details

The function performs the following main operations:

1.  Downloads raw price data from Google Cloud Storage

2.  Renames columns and selects relevant fields (submission_id,
    landing_site, landing_date, and price fields)

3.  Cleans and standardizes text fields

4.  Pivots price data from wide to long format

5.  Standardizes fish category names and separates size information

6.  Converts data types (datetime, character, numeric)

7.  Removes duplicate entries

8.  Uploads the processed data as a Parquet file to Google Cloud Storage

## Examples

``` r
if (FALSE) { # \dontrun{
preprocess_price_landings()
} # }
```
