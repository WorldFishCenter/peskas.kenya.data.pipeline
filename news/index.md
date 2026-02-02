# Changelog

## peskas.kenya.data.pipeline 4.5.0

### New Features

- **API KEFS Data Export Pipeline**: Added new
  [`export_api_raw()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/export_api_raw.md)
  function to export raw preprocessed survey data in API-friendly format
  - Exports raw/preprocessed trip data (before validation) to cloud
    storage
  - Part of a two-stage API export pipeline (raw and validated exports)
  - Transforms nested survey data into flat structure with standardized
    trip-level records
  - Generates unique trip IDs using xxhash64 algorithm
  - Exports versioned parquet files to `kenya/raw/` path for external
    API consumption
  - Includes comprehensive output schema with 14 standardized fields
    (trip_id, landing_date, gear, catch metrics, etc.)

### Improvements

- **Configuration Enhancements**:
  - Added `api` configuration section for trip data exports with
    separate raw/validated paths
  - Configured cloud storage paths for API exports (kenya/raw,
    kenya/validated)
  - Added Airtable base ID and token configuration for metadata
    management
  - Enhanced `options_api` storage configuration for peskas-coasts
    bucket
- **GitHub Actions Workflow**:
  - Added new `export-api-data` job to automated pipeline workflow
  - Configured API export job to run after survey preprocessing step
  - Added production environment configuration for API data exports

## peskas.kenya.data.pipeline 4.4.0

### New features

- **KEFS V2 Validation Pipeline**:
  - Added
    [`validate_kefs_surveys_v2()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/validate_kefs_surveys_v2.md)
    function for comprehensive validation of KEFS catch assessment
    surveys
  - Integrated KoboToolbox API validation status querying with
    [`get_validation_status()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_validation_status.md)
    and
    [`update_validation_status()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/update_validation_status.md)
    functions
  - Implemented multi-dimensional validation system with information,
    trip, catch, and indicator flags
  - Added support for manual validation override to preserve
    human-reviewed approvals while applying automated checks
  - Exported new validation helper functions:
    [`get_trips_flags()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_trips_flags.md),
    [`get_catch_flags()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_catch_flags.md),
    and
    [`get_indicators_flags()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_indicators_flags.md)
  - Added GitHub Actions workflow job `validate-kefs-catch-v2` for
    automated validation processing

### Enhancements

- **KEFS Data Preprocessing Improvements**:
  - Enhanced
    [`preprocess_kefs_surveys_v2()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/preprocess_kefs_surveys_v2.md)
    to include `submission_date` field for tracking when data was
    submitted
  - Added `fishing_per_week` field extraction and parsing from survey
    data
  - Improved catch outcome handling: automatically sets catch_outcome to
    “yes” when total_catch_weight \> 0 but catch_outcome is NA
  - Renamed weight and price columns for clarity:
    - `sample_weight` → `total_sample_weight`
    - `catch_weight` → `total_catch_weight`
    - `price_kg` → `total_price_kg`
    - `catch_price` → `total_catch_price`
  - Enhanced enumerator name standardization integration
- **Survey Data Reshaping**:
  - Updated
    [`reshape_priority_species()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/reshape_priority_species.md)
    to use more descriptive column name: `weight_priority` instead of
    `weight_kg`
  - Updated
    [`reshape_overall_sample()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/reshape_overall_sample.md)
    to use standardized column names: `sample_weight` and `sample_price`
- **Configuration Updates**:
  - Added `KEFS_KOBO_TOKEN` environment variable for KoboToolbox API
    authentication
  - Added validation flags file prefixes for both KEFS v1 and v2 in
    config.yml
  - Configured cloud storage paths for validation output files

### Documentation

- **Enhanced Function Documentation**:
  - Added comprehensive documentation for
    [`validate_kefs_surveys_v2()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/validate_kefs_surveys_v2.md)
    including detailed workflow description, validation limits, and
    requirements
  - Added documentation for KEFS validation helper functions:
    - [`get_trips_flags()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_trips_flags.md):
      Documents trip-level validation for horse power, fishers,
      duration, and revenue with 6 alert codes
    - [`get_catch_flags()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_catch_flags.md):
      Documents catch-level validation for sample weight inconsistencies
      with 2 alert codes
    - [`get_indicators_flags()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_indicators_flags.md):
      Documents composite indicator validation (CPUE, RPUE, price/kg)
      with 3 alert codes
  - Added documentation for KoboToolbox integration functions:
    - [`get_validation_status()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_validation_status.md):
      Retrieves validation status from KoboToolbox for submissions
    - [`update_validation_status()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/update_validation_status.md):
      Updates validation status in KoboToolbox
  - Updated
    [`get_indicators_flags()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_indicators_flags.md)
    documentation to correctly reflect the `clean_ids` parameter
  - All new documentation follows roxygen2 style with comprehensive
    examples, parameter descriptions, and return value specifications
- **New man pages added**:
  - `get_catch_flags.Rd`, `get_indicators_flags.Rd`,
    `get_trips_flags.Rd`
  - `get_validation_status.Rd`, `update_validation_status.Rd`
  - `validate_kefs_surveys_v2.Rd`

### NAMESPACE

- Exported new validation functions: `get_trips_flags`,
  `get_catch_flags`, `get_indicators_flags`
- Exported KoboToolbox integration functions: `get_validation_status`,
  `update_validation_status`
- Exported `validate_kefs_surveys_v2` for KEFS V2 validation workflow

## peskas.kenya.data.pipeline 4.3.0

### Configuration

- **Environment-based Configuration**:
  - Migrated authentication from file-based (`auth/`) to environment
    variable approach using `.env` files
  - Improved security and deployment flexibility by using environment
    variables for credentials
  - Added `.env.example` for reference configuration

### Fixes

- **Package Quality and R CMD Check**:
  - Fixed function naming conflict: renamed
    [`get_fishery_metrics()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_fishery_metrics.md)
    in preprocessing.R to
    [`get_fishery_metrics_long()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_fishery_metrics_long.md)
    to avoid duplicate function definitions
  - Corrected parameter names in helper functions
    ([`get_fishery_metrics()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_fishery_metrics.md),
    [`get_individual_metrics()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_individual_metrics.md),
    [`get_individual_gear_metrics()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_individual_gear_metrics.md))
    to match documentation
  - Resolved “unused arguments” error in
    [`export_summaries()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/export_summaries.md)
    function calls
  - Updated function documentation to align with actual parameter names

## peskas.kenya.data.pipeline 4.2.0

### New features

- **GPS Track Processing and Analysis**:
  - Added `process_fishing_tracks()` for comprehensive GPS track
    processing and fishing activity classification
  - Implemented `prepare_gps_data()` to convert raw tracking data into
    GPSMonitoring format
  - Added `classify_fishing_activity()` using speed thresholds and
    spatial clustering to identify fishing vs transit activities
  - Created `process_trajectories_with_speed()` for calculating vessel
    speeds from GPS positions
  - Implemented `calculate_fishing_summaries()` to generate effort
    metrics by trip and spatial grid
  - Added `visualize_fishing_track()` for mapping fishing activities
  - Created `create_exclusion_zones()` and `create_extent_polygon()` for
    spatial analysis
- **Airtable Integration**:
  - Implemented bidirectional Airtable API integration with
    [`airtable_to_df()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/airtable_to_df.md)
    for reading records
  - Added
    [`df_to_airtable()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/df_to_airtable.md)
    for bulk create operations
  - Created
    [`update_airtable_record()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/update_airtable_record.md)
    for updating individual records
  - Implemented
    [`bulk_update_airtable()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/bulk_update_airtable.md)
    for batch update operations
  - Added
    [`get_writable_fields()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_writable_fields.md)
    to retrieve field schemas and validate writable fields
- **Fishery Metrics Expansion**:
  - Extended
    [`get_fishery_metrics()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_fishery_metrics.md)
    with normalized long format output for maximum interoperability
  - Added metrics for CPUE and RPUE by gear type
  - Implemented species composition analysis with top 2 species ranking
  - Created fully normalized dataset structure for flexible aggregation
    and filtering
- **PDS Tracker Report**:
  - Added comprehensive GPS tracking visualization report
    (inst/reports/pds/trackers.qmd)
  - Integrated interactive maps, time series, and network analysis
    visualizations
  - Implemented spatial heatmaps and trip trajectory analysis

### Enhancements

- **Package Quality and Compliance**:
  - Fixed R CMD check warnings by replacing
    [`library()`](https://rdrr.io/r/base/library.html)/[`require()`](https://rdrr.io/r/base/library.html)
    calls with `::` notation and
    [`requireNamespace()`](https://rdrr.io/r/base/ns-load.html)
  - Added missing global variable bindings for track and fishery metrics
    variables
  - Updated NAMESPACE with new imports:
    [`dplyr::n`](https://dplyr.tidyverse.org/reference/context.html),
    [`dplyr::arrange`](https://dplyr.tidyverse.org/reference/arrange.html),
    [`dplyr::row_number`](https://dplyr.tidyverse.org/reference/row_number.html),
    [`stats::lag`](https://rdrr.io/r/stats/lag.html),
    [`stats::time`](https://rdrr.io/r/stats/time.html),
    [`rlang::sym`](https://rlang.r-lib.org/reference/sym.html)
  - Added GPSMonitoring and ggplot2 to Suggests dependencies
  - Excluded `.claude` directory and `.parquet` files from package build
    via .Rbuildignore
  - Reduced R CMD check from 3 warnings/6 notes to 2 warnings/2 notes
- **KEFS Data Integration**:
  - Updated configuration (inst/config.yml) for KEFS survey data
    integration
  - Enhanced inst/kefs_integration.R with improved data processing
    workflows

### Documentation

- Added 14 new man pages for GPS tracking functions:
  - `process_fishing_tracks.Rd`, `prepare_gps_data.Rd`,
    `classify_fishing_activity.Rd`
  - `process_trajectories_with_speed.Rd`,
    `calculate_fishing_summaries.Rd`
  - `create_exclusion_zones.Rd`, `create_extent_polygon.Rd`,
    `visualize_fishing_track.Rd`
- Added 6 new man pages for Airtable integration:
  - `airtable_to_df.Rd`, `df_to_airtable.Rd`,
    `update_airtable_record.Rd`
  - `bulk_update_airtable.Rd`, `get_writable_fields.Rd`
- Updated `get_fishery_metrics.Rd` to document dual function signatures
  for different use cases
- Enhanced documentation with comprehensive examples and parameter
  descriptions

## peskas.kenya.data.pipeline 4.1.0

### Enhancements

- **Improved MongoDB Data Handling**:
  - Enhanced
    [`mdb_collection_pull()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/mdb_collection_pull.md)
    to properly handle MongoDB ObjectId fields by explicitly including
    `_id` in queries and maintaining correct column ordering
  - Updated column reordering logic to prioritize `_id` field placement
    in retrieved datasets
  - Improved data consistency and integrity when pulling data from
    MongoDB collections
- **Code Style and Formatting**:
  - Standardized function parameter formatting across storage functions
    for improved readability
  - Enhanced code consistency in
    [`upload_parquet_to_cloud()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/upload_parquet_to_cloud.md),
    [`cloud_object_name()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/cloud_object_name.md),
    and related functions
  - Updated
    [`get_metadata()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_metadata.md)
    function formatting to follow consistent style guidelines
- **User Management Infrastructure**:
  - Added comprehensive user management system for MongoDB dashboard
    with treatment group support
  - Implemented secure password generation and user creation
    functionality
  - Added support for BMU-based user access control and role assignment

## peskas.kenya.data.pipeline 3.2.1

### New features

- **Individual Fisher Metrics Data Extraction**:
  - Added
    [`get_individual_gear_metrics()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_individual_gear_metrics.md)
    to calculates gear key fishery performance metrics at the individual
    fisher level from validated catch data, including CPUE (Catch Per
    Unit Effort), RPUE (Revenue Per Unit Effort), price per kg, trip
    costs, and profit margins for each fisher.
  - Integrated the fish composition distribution at fishers level

## peskas.kenya.data.pipeline 3.2.0

### New features

- **Individual Fisher Metrics Data Extraction**:
  - Added
    [`get_individual_metrics()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_individual_metrics.md)
    to calculates key fishery performance metrics at the individual
    fisher level from validated catch data, including CPUE (Catch Per
    Unit Effort), RPUE (Revenue Per Unit Effort), price per kg, trip
    costs, and profit margins for each fisher.

## peskas.kenya.data.pipeline 3.1.0

### New features

- **Individual Fisher Data Extraction**:
  - Added
    [`get_individual_data()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_individual_data.md)
    to extract fisher IDs and trip costs from raw survey data, enabling
    more granular analysis of fishing effort and expenses.
  - Integrated individual fisher and trip cost data into the version 2
    preprocessing pipeline
    ([`preprocess_landings_v2()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/preprocess_landings_v2.md)),
    with new columns for `fisher_id` and `trip_cost` in preprocessed
    outputs.

### Enhancements

- **Expanded Data Merging**:
  - Updated
    [`merge_landings()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/merge_landings.md)
    to support merging of legacy, v1, and v2 preprocessed landings,
    including new fields for individual fisher and trip cost data.
  - Improved column selection and ordering in merged datasets for
    consistency and downstream compatibility.
- **Export and Summarization Improvements**:
  - Enhanced
    [`export_summaries()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/export_summaries.md)
    to include new individual-level statistics, such as mean trip
    expenses per fisher, aggregated by BMU and month (for data collected
    after June 25, 2025).
  - Improved monthly and distribution summaries to reflect the expanded
    data model.

### Configuration

- Updated `config.yml` to support new data paths and fields required for
  individual fisher and trip cost data in v2 surveys.

### Documentation

- Added new man page for
  [`get_individual_data()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_individual_data.md).
- Updated documentation for all affected functions to reflect new
  parameters and outputs.

## peskas.kenya.data.pipeline 3.0.0

### New features

- **Versioned Survey Processing**:
  - Added version-specific ingestion functions for catch and price
    surveys
  - Implemented
    [`ingest_catch_survey_version()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/ingest_catch_survey_version.md)
    and
    [`ingest_price_survey_version()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/ingest_price_survey_version.md)
    for versioned data handling
  - Created versioned preprocessing functions
    ([`preprocess_landings_v1()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/preprocess_landings_v1.md)
    and
    [`preprocess_landings_v2()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/preprocess_landings_v2.md))
  - Added core preprocessing functions for modular data transformation

### Enhancements

- **Improved Data Processing Pipeline**:
  - Refactored ingestion logic to handle multiple survey versions
  - Enhanced preprocessing workflow with version-specific
    transformations
  - Updated configuration structure to support versioned data paths
  - Improved documentation for all versioned functions

### Documentation

- Added comprehensive documentation for new versioned functions
- Updated existing function documentation to reflect versioning changes
- Added new man pages for version-specific functions
- Enhanced function descriptions and examples

## peskas.kenya.data.pipeline 2.1.0

### New features

- **Geospatial Export Capabilities**:
  - Added
    [`create_geos()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/create_geos.md)
    function to generate GeoJSON files with regional-level fishery
    metrics
  - Implemented spatial analysis to assign BMUs to nearest coastal
    regions
  - Exported time series of aggregated metrics (CPUE, CPUA, effort,
    RPUE, RPUA) at regional level
  - Added regional polygon geometries for spatial visualization

### Enhancements

- **Temporal Data Processing**:
  - Revised fishery metrics calculation for more accurate temporal
    representation
  - Implemented daily-average approach instead of aggregate-then-divide
    method
  - Maintained per-day units (kg/fisher/day, KES/km²/day) consistent
    with scientific standards
  - Fixed CPUE and other daily metrics by calculating at daily level
    first
  - Filtered export data to include only records from 2023 onwards

### Fixes

- **Data Quality Improvements**:
  - Corrected handling of missing information with NA values instead of
    zeros
  - Fixed dependency logic of the pipeline
  - Improved price table prefix path handling
  - Updated configuration paths for downloading preprocessed price and
    legacy catch data

## peskas.kenya.data.pipeline 2.0.0

### New features

- **Alternative Validation Method**:
  - Added new validation functions using Interquartile Range (IQR)
    method:
    - [`alert_outlier_iqr()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/alert_outlier_iqr.md)
      for IQR-based outlier detection
    - [`check_outliers_iqr()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/check_outliers_iqr.md)
      for basic outlier checking
    - [`validate_nfishers_iqr()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/validate_nfishers_iqr.md)
      and
      [`validate_nboats_iqr()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/validate_nboats_iqr.md)
      for validating fisher and boat counts
    - [`validate_catch_iqr()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/validate_catch_iqr.md)
      and
      [`validate_total_catch_iqr()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/validate_total_catch_iqr.md)
      for catch validation
    - [`get_catch_bounds_iqr()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_catch_bounds_iqr.md)
      and
      [`get_total_catch_bounds_iqr()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_total_catch_bounds_iqr.md)
      for calculating bounds
  - Modified
    [`validate_landings()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/validate_landings.md)
    to use IQR validation by default

### Enhancements

- **Improved Error Handling**:
  - Added comprehensive input validation in all IQR functions
  - Added consistent NULL/empty data checks
  - Improved NA value handling using dplyr::if_else
  - Added warnings for cases where bounds cannot be calculated

### Documentation

- Added full documentation for all new IQR functions with examples
- Updated NAMESPACE to export new IQR functions
- Added proper Roxygen documentation for all new parameters

### Other Changes

- Switched default validation method from MAD to IQR in main pipeline
- Maintained backward compatibility with existing MAD functions

### New features

- **Migration to Google Cloud Storage**:
  - Replaced MongoDB storage with Google Cloud Storage (GCS) using
    Parquet files
  - Added new cloud storage functions for GCS operations:
    - Authentication and connection management
    - Upload and download capabilities
    - Versioned file handling
  - Integrated Apache Arrow for efficient Parquet file processing
  - Updated configuration to support GCS file prefixes and paths

### Enhancements

- **Storage Operations**:
  - Refactored storage operations to use cloud-native approaches
  - Improved data access performance with Parquet file format
  - Added support for versioned file management

### Dependencies

- Added cloud storage related libraries
- Added Arrow for Parquet file handling

## peskas.kenya.data.pipeline 1.0.0

### New features

- **Catch Validation Enhancements**:
  - Added
    [`validate_fishers_catch()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/validate_fishers_catch.md)
    to flag cases where a single fisher reports an excessively high
    catch.
  - Introduced
    [`impute_price()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/impute_price.md)
    to fill missing fish prices using median values across landing sites
    and sizes.
  - Integrated price validation in
    [`validate_landings()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/validate_landings.md)
    to ensure missing or zero prices are flagged.
- **Revenue Metrics in Summaries**:
  - [`export_summaries()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/export_summaries.md)
    now calculates revenue-based effort metrics:
    - `rpue` (Revenue Per Unit Effort) as aggregated price per fisher
    - `rpua` (Revenue Per Unit Area) as aggregated price per square km
- **Extended Workflow Automation**:
  - Updated GitHub Actions workflow (`data-pipeline.yaml`) to include a
    job for processing and merging price data.
  - Added price-related MongoDB collections (`raw_price`,
    `preprocessed_price`, `price_table`) to `config.yml`.

### Enhancements

- **Improved Validation and Data Processing**:
  - [`validate_total_catch()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/validate_total_catch.md)
    and
    [`validate_catch()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/validate_catch.md)
    now allow customizable `flag_value` parameters.
  - Enhanced
    [`merge_prices()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/merge_prices.md)
    with fixes for site name inconsistencies (e.g., “Rigati” →
    “Rigata”).
  - [`preprocess_legacy_landings()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/preprocess_legacy_landings.md)
    now ensures valid dates by correcting invalid timestamps.
- **Refined Data Summarization**:
  - [`export_summaries()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/export_summaries.md)
    now includes revenue metrics while maintaining prior CPUE and CPUA
    calculations.
  - Updated `.qmd` reports to reflect new price fields in the summary
    tables.

### Fixes

- **Bug Fixes and Consistency Improvements**:
  - Fixed potential issues in
    [`merge_prices()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/merge_prices.md)
    that could lead to duplicate price records.
  - Ensured
    [`impute_price()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/impute_price.md)
    correctly applies median imputation across all relevant data points.
  - Updated `.Rd` documentation for all modified functions to reflect
    new parameters and changes.

## peskas.kenya.data.pipeline 0.9.0

### New features

- **Price Data Integration**: Added support for processing fish price
  data alongside catch data.
  - Introduced
    [`ingest_landings_price()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/ingest_landings_price.md)
    to download price surveys from KoboToolbox.
  - Implemented
    [`preprocess_price_landings()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/preprocess_price_landings.md)
    to clean and standardize fish price data.
  - Added
    [`merge_prices()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/merge_prices.md)
    to combine and aggregate legacy and ongoing price data.
  - Created
    [`summarise_catch_price()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/summarise_catch_price.md)
    to compute median price per kilogram by landing site and fish
    category.
- **Extended Workflow Automation**:
  - Added a new GitHub Actions job `merge-price-data` to automate the
    processing and merging of price data.
- **Expanded MongoDB Collections**:
  - Introduced new collections in `config.yml` for handling raw,
    preprocessed, and aggregated price data (`raw_price`,
    `preprocessed_price`, and `price_table`).

### Enhancements

- **Validation and Alert Flagging**:
  - Improved handling of validation alert flags by introducing dynamic
    `flag_value` parameters.
  - Adjusted anomaly detection in
    [`validate_landings()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/validate_landings.md)
    to use more descriptive alert codes.
- **Configuration Updates**:
  - Updated `.github/workflows/data-pipeline.yaml` to include price
    survey ingestion and processing.
  - Extended `config.yml` to support price data processing in both local
    and production environments.
- **Namespace Expansion**:
  - Exported new functions (`ingest_landings_price`, `merge_prices`,
    `preprocess_price_landings`, `summarise_catch_price`) to make them
    available for package use.

### Fixes

- **Documentation Corrections**:
  - Updated `.Rd` documentation for `ingest_landings()` to clarify that
    it processes **catch surveys** (not all WCS surveys).
  - Added descriptions for new parameters (`flag_value`) in validation
    function documentation.
- **Bug Fixes**:
  - Resolved inconsistencies in price data processing by ensuring column
    renaming and format standardization.
  - Fixed logical errors in merging price data to prevent duplicate
    records.
  - Improved MongoDB push operations to correctly handle newly
    introduced price data collections.

## peskas.kenya.data.pipeline 0.8.0

### New features

- Introduced `flag_value` parameter to validation functions
  (`validate_dates`, `validate_nfishers`, `validate_nboats`, and
  `validate_catch`) for customizable alert thresholds.
- Added `logical_check` and `anomalous_submissions` workflows to improve
  anomaly detection in `validate_landings`.
- Enhanced data upload pipeline with
  [`purrr::walk2`](https://purrr.tidyverse.org/reference/map2.html) for
  streamlined MongoDB uploads of validated and flagged data.

### Enhancements

- Updated `export_summaries` to calculate `mean_trip_catch` using the
  median for improved robustness against skewed data.
- Improved documentation for validation functions to include detailed
  descriptions and examples for the new `flag_value` parameter.
- Refined the logic in `validate_landings` for clearer alert flagging
  and better handling of edge cases.
- Added the `validation_flags` collection to the configuration file
  (`config.yml`) for centralized management of flagging outputs.
- Updated anomaly alert flags to use descriptive string values instead
  of numeric codes for better interpretability.

### Fixes

- Fixed inconsistencies in the handling of alert flags during data
  validation.
- Resolved potential mismatches in merged datasets by improving join
  logic in `validate_landings`.
- Corrected documentation typos and improved consistency across `.Rd`
  files.

## peskas.kenya.data.pipeline 0.7.0

### New features

- Introduced `get_total_catch_bounds` for calculating upper bounds on
  total catch data grouped by landing site and gear type.
- Added `validate_total_catch` function for validating total catch data
  with improved outlier handling.
- Updated `validate_landings` to identify and filter out inconsistent
  submissions (e.g., mismatched fishers and boats).
- Enhanced `validate_catch` to set outlier values in `catch_kg` to NA
  and flag them for review.

### Enhancements

- Improved `get_catch_bounds` to exclude invalid categories and
  clarified grouping logic for gear and fish categories.
- Updated documentation across validation and catch bounds functions to
  include better descriptions, keywords, and return values.
- Refined data preprocessing in `preprocess_landings` to ensure unique
  rows, handle NA values, and streamline workflow.
- Adjusted configuration parameters in `config.yml` to enhance
  validation sensitivity (e.g., increasing `k_catch` for outlier
  detection).
- Updated export_summaries to improve monthly summaries by recalculating
  effort as fishers per km² per day and refining CPUE and CPUA metrics
  to include temporal normalization. Simplified mean trip catch
  calculations for consistency across datasets.

### Fixes

- Corrected plot code formatting in `data_report.qmd` for CPUE, effort,
  and CPUA visualizations.
- Fixed documentation typos and inconsistencies in `.Rd` files for
  validation functions.

## peskas.kenya.data.pipeline 0.6.0

### New features

- Add merge_landings function for combining data sources
- Add validation catch function to improve data quality checks
- Include form consent and total catch fields in merged data
- Calculate total catch in legacy landings

### Enhancements

- Homogenise gear and fish groups names
- Update validation code structure
- Fix mismatch between fish groups sum and total catch
- Fix landing sites names
- Improve validation catch function
- Update export data based on validation
- Fix legacy submission IDs

## peskas.kenya.data.pipeline 0.5.1

### New features

- Add functions to extract effort and CPUE from validated legacy data
- Implement MongoDB pushing functionality
- Add storage and metadata functions

### Enhancements

- Index functions by topic and package functional position
- Update package documentation and website
- Improve MongoDB storage calls
- Add configuration file for MongoDB database and collection references

### Fixes

- Drop stale files
- Update storage calls for better efficiency
- Fix various typos and syntax errors

## peskas.kenya.data.pipeline 0.5.0

### Enhancements

- Improve mongoDB storage functions by adding indexes to improve query
  performance. Before this fix column order in data was not preserved.

## peskas.kenya.data.pipeline 0.4.0

### New features

- Now `ingest_surveys()` uses Kobotoolbox API directly to download
  surveys instead of rely on R package

### Fixes

- Fix ingestion functions as it was limited to download max 30,000
  submissions. The approach now uses pagination to retrieve large
  datasets, with a limit of 30,000 records per request

## peskas.kenya.data.pipeline 0.3.0

### New features

- Add ingestion function to ingest ongoing data `ingest_surveys()`
- Add preprocessing function to preprocess ongoing data
  `preprocess_landings()`

## peskas.kenya.data.pipeline 0.2.0

- Add package website with documentation functions
- Integrate mongodb storage with package functions
- Optimize configuration file

## peskas.kenya.data.pipeline 0.1.0

- Initial CRAN submission.
