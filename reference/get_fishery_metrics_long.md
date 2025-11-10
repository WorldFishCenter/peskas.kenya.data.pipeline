# Calculate key fishery metrics by landing site and month in normalized long format

Summarizes fishery data to extract main characteristics including catch
rates, gear usage, species composition, CPUE and RPUE by gear type.
Returns data in a fully normalized long format for maximum
interoperability and analytical flexibility.

## Usage

``` r
get_fishery_metrics_long(data = NULL)
```

## Arguments

- data:

  A dataframe containing fishery landing data with columns:
  submission_id, landing_date, landing_site, gear, no_of_fishers,
  fish_category, catch_kg, and total_catch_price

## Value

A dataframe in long format with columns:

- landing_site: Name of the landing site

- year_month: First day of the month

- metric_type: Type of metric (e.g., "avg_fishers_per_trip", "cpue",
  "rpue", "species_pct")

- metric_value: Numeric value of the metric

- gear_type: Type of fishing gear (for gear-specific metrics, NA for
  site-level metrics)

- species: Fish species name (for species-specific metrics, NA for other
  metrics)

- rank: Rank order (for ranked metrics like top species, NA for others)

## Details

The function creates a fully normalized dataset where each row
represents a single metric observation. This format enables:

- Easy filtering by metric type, gear, or species

- Flexible aggregation and comparison across dimensions

- Database-friendly structure for storage and querying

- Simplified visualization and statistical analysis

## Examples

``` r
if (FALSE) { # \dontrun{
fishery_metrics <- get_fishery_metrics_long(data = valid_data)

# Filter for CPUE metrics only
cpue_data <- fishery_metrics %>% filter(metric_type == "cpue")

# Filter for RPUE metrics only
rpue_data <- fishery_metrics %>% filter(metric_type == "rpue")

# Get predominant gear by site
main_gear <- fishery_metrics %>% filter(metric_type == "predominant_gear")
} # }
```
