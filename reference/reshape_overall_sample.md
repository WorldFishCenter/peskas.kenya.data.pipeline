# Reshape Overall Sample Weight Data from Wide to Long Format

Transforms overall sample weight data from wide to long format. Extracts
columns containing "OverallSampleWeight" (excluding calculation
columns), reshapes them into rows, and converts to numeric types.

## Usage

``` r
reshape_overall_sample(raw_data = NULL)
```

## Arguments

- raw_data:

  Data frame with sample weight columns following pattern
  `OverallSampleWeight.{i}.{field}`.

## Value

Tibble with columns: submission_id, n_sample, sample_species,
weight_sample, price_sample.

## Details

Iterates through sample numbers (0-based in raw data, 1-based in
output), reshapes each group to standardized column names, filters out
incomplete records, and combines into long format.
