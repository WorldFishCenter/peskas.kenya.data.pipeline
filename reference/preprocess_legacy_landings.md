# Preprocess Legacy Landings Data

This function imports, preprocesses, and cleans legacy landings data
from a MongoDB collection. It performs various data cleaning and
transformation operations, including column renaming, removal of
unnecessary columns, generation of unique identifiers, and data type
conversions. The processed data is then uploaded back to MongoDB.

## Usage

``` r
preprocess_legacy_landings(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  Logging threshold level (default: logger::DEBUG)

## Value

This function does not return a value. Instead, it processes the data
and uploads the result to a MongoDB collection in the pipeline database.

## Details

The function performs the following main operations:

1.  Pulls raw data from the raw data MongoDB collection.

2.  Removes several unnecessary columns.

3.  Renames columns for clarity (e.g., 'site' to 'landing_site').

4.  Generates unique 'survey_id' and 'catch_id' fields.

5.  Converts several string fields to lowercase.

6.  Cleans catch names using a separate function 'clean_catch_names'.

7.  Uploads the processed data to the preprocessed MongoDB collection.

## Note

This function requires a configuration file to be present and readable
by the 'read_config' function, which should provide MongoDB connection
details.

## Examples

``` r
if (FALSE) { # \dontrun{
preprocess_legacy_landings()
} # }
```
