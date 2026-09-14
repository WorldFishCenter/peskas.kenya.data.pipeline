# Export Raw API-Ready Trip Data

Downloads preprocessed KEFS and WCS survey data, transforms both into
the canonical API schema, and uploads a parquet file to cloud storage.
This is the **raw/preprocessed** stage of the two-stage API export
pipeline.

## Usage

``` r
export_api_raw(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  Logging level (default
  [`logger::DEBUG`](https://daroczig.github.io/logger/reference/log_levels.html)).

## Value

NULL invisibly. Side effect: uploads merged parquet to cloud storage.

## Details

Both stages cover the same two sources, so the validated export is a
strict subset of this one and the API's `status` parameter selects two
processing stages of one population rather than two different
populations. The WCS preprocessed stage is
`conf$surveys$wcs$catch$merged$file_prefix`, the merged landings written
by
[`merge_landings()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/merge_landings.md):
the legacy, v1 and v2 sources bound together before
[`validate_landings()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/validate_landings.md)
drops alerting trips and joins prices. Trips present here but absent
from the validated export are those validation rejected.

**Output Schema**:

- `survey_id`: Kobo asset ID identifying the source survey form

- `trip_id`: Unique identifier (`TRIP_<submission_id>` format)

- `landing_date`: Date of landing

- `gaul_1_code`, `gaul_1_name`: GAUL level 1 region

- `gaul_2_code`, `gaul_2_name`: GAUL level 2 district

- `landing_site`, `landing_site`: Landing site name

- `n_fishers`: Total fishers (men + women + children)

- `trip_duration_hrs`: Trip duration in hours (NA for WCS — not
  collected by any WCS form)

- `gear`: Standardised gear type

- `vessel_type`: Standardised vessel type

- `catch_habitat`: Habitat where catch occurred

- `catch_outcome`: Outcome of catch

- `n_catch`: Number of catch items

- `catch_taxon`: Species alpha-3 code

- `scientific_name`: Scientific name

- `length_cm`: Length in cm (NA for WCS surveys)

- `catch_kg`: Catch weight in kg

- `catch_price`: Individual-level price (NA for WCS — prices are
  resolved only at the validated stage)

- `tot_catch_kg`: Total catch weight per trip

- `tot_catch_price`: Total catch price per trip (NA for WCS, as above)

**Cloud Storage Location**: `conf$api$trips$raw$cloud_path` /
`{file_prefix}__{timestamp}_{git_sha}__.parquet`
