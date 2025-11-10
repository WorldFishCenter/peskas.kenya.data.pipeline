# Calculate Individual Fisher Performance Metrics by Gear Type

Calculates key fishery performance metrics at the individual fisher
level from validated catch data, stratified by gear type. This function
provides the same metrics as `get_individual_metrics` but includes gear
type as an additional grouping variable, allowing for analysis of
gear-specific performance patterns.

## Usage

``` r
get_individual_gear_metrics(valid_data = NULL)
```

## Arguments

- valid_data:

  A data frame containing validated fishery data with columns:

  - landing_site: Name of the landing site (will be renamed to BMU)

  - landing_date: Date of landing

  - fisher_id: Unique identifier for each fisher

  - gear: Type of fishing gear used

  - no_of_fishers: Number of fishers

  - total_catch_kg: Total catch in kilograms

  - total_catch_price: Total catch value in currency

  - trip_cost: Cost of fishing trip in currency

## Value

A data frame with monthly aggregated metrics by BMU, gear type, and
individual fisher:

- BMU: Name of the Beach Management Unit (title case)

- date: First day of the month

- fisher_id: Unique identifier for each fisher

- gear: Type of fishing gear used

- mean_cpue: Average catch (kg) per fisher

- mean_rpue: Average revenue per fisher

- mean_price_kg: Average price per kg

- mean_cost: Average trip costs per fisher

- mean_profit: Average profit per fisher (revenue minus costs)

## Details

This function extends the individual fisher analysis by incorporating
gear type as an additional stratification variable. Individual fisher
metrics are calculated as follows:

- CPUE = Aggregated catch (kg) / Total fishers

- RPUE = Aggregated revenue / Total fishers

- Price per kg = Total revenue / Total catch (median across trips)

- Costs = Median trip cost

- Profit = RPUE - Costs

The function filters for records with non-missing landing dates and
fisher IDs, then aggregates data by landing date, BMU, gear type, and
fisher ID before calculating monthly averages. This allows for
comparison of performance across different gear types for the same
fisher, as well as gear-specific benchmarking across fishers. Only
fishers with individual identification are included in the analysis.

## See also

[`get_individual_metrics`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_individual_metrics.md)
for metrics without gear stratification
