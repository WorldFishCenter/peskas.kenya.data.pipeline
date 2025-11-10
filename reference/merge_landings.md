# Merge Legacy and Ongoing Landings Data

This function merges preprocessed legacy landings data with ongoing
landings data from Google Cloud Storage. It combines the datasets,
performs minimal transformations, and uploads the merged result as a
Parquet file.

## Usage

``` r
merge_landings(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  Logging threshold level (default: logger::DEBUG)

## Value

No return value. Function processes the data and uploads the result as a
Parquet file to Google Cloud Storage.

## Details

The function performs the following main operations:

1.  Downloads preprocessed legacy data from Google Cloud Storage.

2.  Downloads preprocessed ongoing data from Google Cloud Storage.

3.  Combines the two datasets using
    [`dplyr::bind_rows()`](https://dplyr.tidyverse.org/reference/bind_rows.html),
    adding a 'version' column to distinguish the sources.

4.  Selects and orders relevant columns for the final merged dataset.

5.  Uploads the merged data as a Parquet file to Google Cloud Storage.

## Note

This function requires a configuration file with Google Cloud Storage
credentials and file prefix settings.

## Examples

``` r
if (FALSE) { # \dontrun{
merge_landings()
} # }
```
