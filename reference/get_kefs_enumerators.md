# KEFS enumerators from Airtable

Reads the rows of the Airtable `enumerators` table linked to the KEFS
form: each person's name, as on the form's list of enumerators, and
their aliases.

## Usage

``` r
get_kefs_enumerators(conf = NULL)
```

## Arguments

- conf:

  Configuration object from read_config().

## Value

A data frame with columns 'name' and 'aliases'.
