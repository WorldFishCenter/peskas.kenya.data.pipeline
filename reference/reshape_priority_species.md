# Reshape Priority Species Catch Data from Wide to Long Format

Transforms priority species catch data from wide to long format.
Extracts columns containing "PrioritySpeciesCatch", reshapes them into
rows, and converts to numeric types.

## Usage

``` r
reshape_priority_species(raw_data = NULL)
```

## Arguments

- raw_data:

  Data frame with priority species columns following pattern
  `PrioritySpeciesCatch.{i}.{field}`.

## Value

Tibble with columns: submission_id, n_priority, priority_species,
length_type, length_cm, weight_priority.

## Details

Iterates through priority species numbers (0-based in raw data, 1-based
in output), reshapes each group to standardized column names, filters
out incomplete records, and combines into long format.
