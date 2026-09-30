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

This function does not return a value. It pushes these collections to
MongoDB: `individual_stats`, `individual_gear_stats`,
`individual_fish_distribution`, `monthly_stats`, `catch_monthly`,
`fish_distribution` and `gear_summaries`.

## Details

The function performs the following operations:

1.  **Data Retrieval**: Reads the latest validated WCS catch table
    (`surveys.wcs.catch.validated`) from cloud storage, and BMU sizes
    from `get_metadata()$BMUs`; landing sites without a size are
    dropped.

2.  **Summary Dataset Generation**: Creates the following summary
    datasets:

    - **Individual metrics**: catch and gear metrics per fisher.

    - **Monthly Statistics**: catch, effort and CPUE by BMU (Beach
      Management Unit) for the last six months.

    - **Monthly Summaries**: monthly metrics by BMU.

    - **Fish Distribution**: the share of each fish category by landing
      site, overall and per fisher.

    - **Gear Summaries**: metrics by gear type.

3.  **Data Upload**: Pushes each dataset to its collection in the
    `dashboard_wcs` MongoDB database
    (`storage.mongodb.databases.dashboard_wcs.collections.v1`).

**Calculated Metrics**:

- **Effort** = Number of fishers / Size of BMU in km²

- **CPUE** = Total catch in kg / Effort

- **Monthly Aggregations**:

  - Total catch (kg)

  - Mean catch per trip

  - Mean effort

  - Mean CPUE (Catch Per Unit Effort)

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
