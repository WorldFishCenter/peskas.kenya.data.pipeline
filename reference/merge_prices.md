# Merge Price Data

This function combines and processes legacy and ongoing catch price data
from MongoDB collections, aggregating prices by year and uploading the
results back to MongoDB.

## Usage

``` r
merge_prices(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  Logging threshold level (default: logger::DEBUG)

## Value

A tibble containing the processed and combined price data

## Details

The function performs the following main operations:

1.  Pulls legacy price data from MongoDB and summarizes it yearly

2.  Pulls ongoing price data from MongoDB and summarizes it yearly

3.  Combines legacy and ongoing data

4.  Filters data after 1990

5.  Removes duplicate entries

6.  Uploads the processed data back to MongoDB

## Examples

``` r
if (FALSE) { # \dontrun{
merge_prices()
} # }
```
