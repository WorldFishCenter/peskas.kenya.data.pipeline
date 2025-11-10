# Generate Geographic Regional Summaries of Fishery Data

This function creates geospatial representations of fishery metrics by
aggregating BMU (Beach Management Unit) data to regional levels along
the Kenyan coast. It assigns each BMU to its nearest coastal region,
calculates regional summaries of fishery performance metrics, and
exports the results as a GeoJSON file for spatial visualization.

## Usage

``` r
create_geos(monthly_summaries_dat = NULL, conf = conf)
```

## Arguments

- monthly_summaries_dat:

  A data frame containing monthly fishery metrics by BMU, typically the
  output from the
  [`get_fishery_metrics()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_fishery_metrics.md)
  function, with columns: BMU, date, mean_effort, mean_cpue, mean_cpua,
  mean_rpue, mean_rpua.

- conf:

  A configuration object containing the path to the GeoJSON file
  "KEN_coast_regions.geojson" included in the peskas.kenya.data.pipeline
  package

## Value

This function does not return a value. It writes a GeoJSON file named
"kenya_monthly_summaries.geojson" to the inst/ directory of the package,
containing regional polygons with associated monthly fishery metrics.#'

## Details

The function performs the following operations:

1.  **BMU Coordinate Extraction**: Retrieves geographic coordinates
    (latitude/longitude) for all BMUs.

2.  **Spatial Conversion**: Converts BMU coordinates to spatial point
    objects.

3.  **Regional Assignment**: Uses spatial analysis to assign each BMU to
    its nearest coastal region.

4.  **Regional Aggregation**: Calculates monthly summary statistics for
    each region by aggregating BMU data.

5.  **GeoJSON Creation**: Combines regional polygon geometries with
    summary statistics and exports as GeoJSON.

**Calculated Regional Metrics** (using median values across BMUs in each
region):

- Mean effort (fishers per square kilometer)

- Mean CPUE (Catch Per Unit Effort, kg per fisher)

- Mean CPUA (Catch Per Unit Area, kg per square kilometer)

- Mean RPUE (Revenue Per Unit Effort, currency per fisher)

- Mean RPUA (Revenue Per Unit Area, currency per square kilometer)

## Note

**Dependencies**:

- Requires the `sf` package for spatial operations.

- Requires a GeoJSON file named "KEN_coast_regions.geojson" included in
  the peskas.kenya.data.pipeline package.

- Uses the
  [`get_metadata()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_metadata.md)
  function to retrieve BMU location information.

## Examples

``` r
if (FALSE) { # \dontrun{
# First generate monthly summaries
monthly_data <- get_fishery_metrics(validated_data, bmu_size)

# Then create regional geospatial summary
create_geos(monthly_summaries_dat = monthly_data)
} # }
```
