# Export Summarized Fishery Data for Dashboard Integration

This function processes and exports validated fishery data by
calculating various summary metrics and distributions, which are then
uploaded to MongoDB collections for usage in a dashboard.

## Usage

``` r
export_summaries(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  The logging threshold level for monitoring operations (default:
  [`logger::DEBUG`](https://daroczig.github.io/logger/reference/log_levels.html)).

## Value

This function does not return a value. It uploads the following
collections to MongoDB:

- Monthly catch summaries (`monthly_stats` and `monthly_summaries`)

- Gear distribution statistics (`gear_distribution`)

- Fish distribution statistics (`fish_distribution`)

- Landing site mapping data (`map_distribution`)

## Details

The function performs the following operations:

1.  **Data Retrieval**: Pulls validated fishery data from the
    "legacy-validated" MongoDB collection.

2.  **Summary Dataset Generation**: Creates the following summary
    datasets:

    - **Monthly Statistics**: Aggregates metrics like catch, effort, and
      CPUE by BMU (Beach Management Unit) and month.

    - **Gear Distribution**: Calculates the percentage usage of each
      gear type by landing site.

    - **Fish Distribution**: Calculates the percentage of each fish
      category by landing site.

    - **Mapping Distribution**: Prepares a dataset of landing sites with
      geographic coordinates for spatial mapping.

3.  **Data Upload**: Uploads each of the summary datasets to its
    designated MongoDB collection.

**Calculated Metrics**:

- **Effort** = Number of fishers / Size of BMU in km²

- **CPUE** = Total catch in kg / Effort

- **Monthly Aggregations**:

  - Total catch (kg)

  - Mean catch per trip

  - Mean effort

  - Mean CPUE (Catch Per Unit Effort)

- **Gear Distribution**:

  - Count of each gear type used

  - Percentage distribution of gear types by landing site

- **Fish Distribution**:

  - Total catch by fish category

  - Percentage of each fish category within the total catch

## Note

**Dependencies**:

- Requires a configuration file compatible with the `read_config`
  function, containing MongoDB connection information.

- Access to a `bmu_size` dataset, which provides size details of BMUs,
  retrieved via the
  [`get_metadata()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_metadata.md)
  function.

## Examples

``` r
if (FALSE) { # \dontrun{
export_summaries()
} # }
```
