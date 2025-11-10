# Calculate Individual Fisher Performance Metrics

Calculates key fishery performance metrics at the individual fisher
level from validated catch data, including CPUE (Catch Per Unit Effort),
RPUE (Revenue Per Unit Effort), price per kg, trip costs, and profit
margins for each fisher.

## Usage

``` r
get_individual_metrics(valid_data = NULL)
```

## Arguments

- valid_data:

  A data frame containing validated fishery data with columns:

  - landing_site: Name of the landing site (will be renamed to BMU)

  - landing_date: Date of landing

  - fisher_id: Unique identifier for each fisher

  - no_of_fishers: Number of fishers

  - total_catch_kg: Total catch in kilograms

  - total_catch_price: Total catch value in currency

  - trip_cost: Cost of fishing trip in currency

## Value

A data frame with monthly aggregated metrics by BMU and individual
fisher:

- BMU: Name of the Beach Management Unit (title case)

- date: First day of the month

- fisher_id: Unique identifier for each fisher

- mean_cpue: Average catch (kg) per fisher

- mean_rpue: Average revenue per fisher

- mean_price_kg: Average price per kg

- mean_cost: Average trip costs per fisher

- mean_profit: Average profit per fisher (revenue minus costs)

## Details

Individual fisher metrics are calculated as follows:

- CPUE = Aggregated catch (kg) / Total fishers

- RPUE = Aggregated revenue / Total fishers

- Price per kg = Total revenue / Total catch (median across trips)

- Costs = Median trip cost

- Profit = RPUE - Costs

The function filters for records with non-missing landing dates and
fisher IDs, then aggregates data by landing date, BMU, and fisher ID
before calculating monthly averages. Only fishers with individual
identification are included in the analysis.
