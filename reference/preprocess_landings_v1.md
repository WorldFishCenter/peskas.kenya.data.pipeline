# Preprocess Landings Data (Version 1)

This function preprocesses raw landings data from Google Cloud Storage.
It performs various data cleaning and transformation operations,
including column renaming, data pivoting, and standardization of catch
names.

## Usage

``` r
preprocess_landings_v1(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  Logging threshold level (default: logger::DEBUG)

## Value

No return value. Function processes the data and uploads the result as a
Parquet file to Google Cloud Storage.

## Details

The function performs the following main operations:

1.  Downloads raw data from Google Cloud Storage

2.  Renames columns and selects relevant fields

3.  Generates unique survey IDs

4.  Cleans and standardizes text fields

5.  Pivots catch data from wide to long format

6.  Standardizes catch names and separates size information

7.  Converts data types and handles cases with no catch data

8.  Uploads the processed data as a Parquet file to Google Cloud Storage

## Examples

``` r
if (FALSE) { # \dontrun{
preprocessed_data <- preprocess_landings_v1()
} # }
```
