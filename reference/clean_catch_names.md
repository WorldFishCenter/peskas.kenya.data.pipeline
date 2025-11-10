# Clean Catch Names

This function standardizes catch names in the dataset by correcting
common misspellings and inconsistencies.

## Usage

``` r
clean_catch_names(data = NULL)
```

## Arguments

- data:

  A data frame or tibble containing a column named `catch_name`.

## Value

A data frame or tibble with standardized catch names in the `catch_name`
column.

## Examples

``` r
if (FALSE) { # \dontrun{
cleaned_data <- clean_catch_names(data)
} # }
```
