# Preprocess KEFS (CATCH ASSESSMENT QUESTIONNAIRE) Survey Data

This function preprocesses raw KEFS (CATCH ASSESSMENT QUESTIONNAIRE)
survey data from Google Cloud Storage. It performs data cleaning,
transformation, standardization of field names, type conversions, and
mapping to standardized taxonomic and gear names using Airtable
reference tables.

## Usage

``` r
preprocess_kefs_surveys_v2(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  Logging threshold level (default: logger::DEBUG)

## Value

No return value. Function processes the data and uploads the result as a
Parquet file to Google Cloud Storage.

## Details

The function performs the following main operations:

1.  **Fetches metadata assets**: Retrieves taxonomic, gear, vessel, and
    landing site mappings from Airtable based on the KEFS Kobo form
    asset ID

2.  **Downloads raw data**: Retrieves raw survey data from Google Cloud
    Storage

3.  **Extracts trip information**: Selects and renames relevant
    trip-level fields including:

    - Landing details (date, site, district, BMU)

    - Fishing ground and JCMA (Joint Community Management Area)
      information

    - Vessel details (type, name, registration, motorization,
      horsepower)

    - Trip details (crew size, start/end times, gear, mesh size, fuel)

    - Catch outcome indicators

4.  **Reshapes catch data**: Transforms catch details from wide to long
    format using
    [`reshape_priority_species()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/reshape_priority_species.md)
    and
    [`reshape_overall_sample()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/reshape_overall_sample.md)

5.  **Type conversions and calculations**:

    - Converts date/time fields to proper datetime format

    - Calculates trip duration in hours from start and end times

    - Converts numeric fields (hp, fishers, mesh size, fuel) to
      appropriate types

6.  **Joins trip and catch data**: Combines trip information with catch
    records using full join on submission_id

7.  **Standardizes names**: Maps survey labels to standardized names
    using
    [`map_surveys()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/map_surveys.md):

    - Taxonomic names to scientific names and alpha3 codes

    - Gear types to standardized gear names

    - Vessel types to standardized vessel categories

    - Landing site codes to full site names

8.  **Uploads processed data**: Saves preprocessed data as a Parquet
    file to Google Cloud Storage

## Data Structure

The preprocessed output includes the following key fields:

- **Trip identifiers**: submission_id

- **Temporal**: landing_date, fishing_trip_start, fishing_trip_end,
  trip_duration

- **Spatial**: district, BMU, landing_site, fishing_ground, jcma,
  jcma_site

- **Vessel**: vessel_type, boat_name, vessel_reg_number, motorized, hp

- **Crew**: captain_name, no_of_fishers

- **Gear**: gear, mesh_size

- **Catch**: scientific_name, alpha3_code, total_catch_weight,
  price_per_kg, total_value

- **Operations**: fuel, catch_outcome, catch_shark

## Pipeline Integration

This function is part of the KEFS data pipeline sequence:

1.  [`ingest_kefs_surveys_v2()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/ingest_kefs_surveys_v2.md) -
    Downloads raw data from Kobo

2.  **`preprocess_kefs_surveys_v2()`** - Cleans and standardizes data
    (this function)

3.  Validation step (to be implemented)

4.  Export step (to be implemented)

## Examples

``` r
if (FALSE) { # \dontrun{
# Preprocess KEFS survey data
preprocess_kefs_surveys_v2()

# Run with custom logging level
preprocess_kefs_surveys_v2(log_threshold = logger::INFO)
} # }
```
