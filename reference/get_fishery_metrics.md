# Calculate Fishery Performance Metrics

Calculates key fishery performance metrics from validated catch data,
including effort, CPUE (Catch Per Unit Effort), CPUA (Catch Per Unit
Area), RPUE (Revenue Per Unit Effort), RPUA (Revenue Per Unit Area) and
Price per kg.

## Usage

``` r
get_fishery_metrics(valid_data = NULL, bmu_size = NULL)
```

## Arguments

- valid_data:

  A data frame containing validated fishery data with columns:

  - landing_site: Name of the landing site (will be renamed to BMU)

  - landing_date: Date of landing

  - no_of_fishers: Number of fishers

  - total_catch_kg: Total catch in kilograms

  - total_catch_price: Total catch value in currency

- bmu_size:

  A data frame containing BMU size information with columns:

  - BMU: Name of the Beach Management Unit (lowercase)

  - size_km: Size of the BMU in square kilometers

## Value

A data frame with monthly aggregated metrics by BMU:

- BMU: Name of the Beach Management Unit (title case)

- date: First day of the month

- mean_effort: Average fishers per square kilometer

- mean_cpue: Average catch (kg) per fisher

- mean_cpua: Average catch (kg) per square kilometer

- mean_rpue: Average revenue per fisher

- mean_rpua: Average revenue per square kilometer

- mean_price_kg: Average price per kg

## Details

Metrics are calculated as follows:

- Effort = Total fishers / BMU size (km²)

- CPUE = Total catch (kg) / Total fishers

- CPUA = Total catch (kg) / BMU size (km²)

- RPUE = Total revenue / Total fishers

- RPUA = Total revenue / BMU size (km²)

- Price per kg = Total revenue / Total catch (on single submissions
  level)
