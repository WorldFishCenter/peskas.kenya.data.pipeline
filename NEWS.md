# peskas.kenya.data.pipeline 4.8.0

## Major Changes

- **Adopted `coasts` as the shared multicountry analytics engine**: Aggregated
  data summarization and dashboard export are now delegated to
  [`WorldFishCenter/peskas.coasts`](https://github.com/WorldFishCenter/peskas.coasts)
  (dev branch). This centralizes the logic for producing monthly, taxa, district,
  and gear summaries — as well as fishery metrics — across all Peskas country
  deployments (Zanzibar, Kenya, Mozambique), ensuring consistent outputs and a
  single place to maintain and improve the shared pipeline logic.
  - Added `coasts` to `Imports` and `Remotes` in `DESCRIPTION`
  - Added `remotes::install_github("WorldFishCenter/peskas.coasts", ref = "dev")`
    to both `Dockerfile` and `Dockerfile.prod` so the image ships the package
  - Pipeline steps that previously used local `summarize_data()` and
    `generate_fleet_analysis()` now call the equivalent `coasts::` functions,
    passing `package = "peskas.kenya.data.pipeline"` so they read the
    country-specific `inst/conf.yml`

# peskas.kenya.data.pipeline 4.7.0

## Improvements

- **Standardized configuration structure**: Replaced `inst/conf.yml` with a unified
  multi-country template harmonized across all Peskas deployments (Zanzibar, Kenya,
  Mozambique). Key structural changes:
  - Survey credentials moved from `surveys.*` into a new top-level `ingestion.*` section
  - Stage keys shortened (`raw_surveys` → `raw`, `preprocessed_surveys` → `preprocessed`, etc.)
  - Source names shortened (`wcs_surveys` → `wcs`, `wf_surveys_v1` → `wf_v1`, etc.)
  - MongoDB structure reorganized: connection strings under `connection_strings.*`,
    databases under `databases.*`, collections key pluralized, `portal` renamed to `dashboard`
  - Airtable config moved from top-level `airtable.*` to `metadata.airtable.*`
  - All R code updated to use the new config paths

# peskas.kenya.data.pipeline 4.6.0

## New Features

- **GPS Trip Matching System**: Added comprehensive fuzzy matching infrastructure to link catch surveys with GPS trip data
  - Implemented `match_surveys_to_gps_trips()` for universal two-step matching workflow (surveys -> registry -> trips)
  - Added `merge_trips()` for end-to-end Kenya KEFS-PDS data integration pipeline
  - Universal matching approach supports both explicit device registries (Kenya) and implicit registry construction (to test with Zanzibar)
  - Supports customizable Levenshtein distance thresholds for registration numbers (default 0.15) and names (default 0.25)
  - Implements conservative one-trip-per-day constraint to prevent ambiguous matches

- **Fuzzy Matching Infrastructure**: Created robust text matching system for cross-dataset linking
  - Text normalization functions for cleaning boat identifiers (`clean_text()`, `clean_registration()`)
  - Field-level Levenshtein distance matching with normalized thresholds (0-1 scale)
  - Multi-field matching strategy (registration number, boat name, fisher name) with per-field thresholds
  - Match quality metrics: `n_fields_used`, `n_fields_ok`, `match_ok` for transparency
  - Handles variant column names across datasets (vessel_reg_number, boat_reg_no, captain_name, etc.)

## Workflow Integration

- **GitHub Actions Pipeline**:
  - Added `merge-trips-kefs-v2` job to automate survey-trip matching
  - Integrated matching step after KEFS v2 validation in production workflow
  - Exports merged dataset (matched + unmatched records) to cloud storage

- **Configuration Support**:
  - Added merged trips output path configuration (`surveys.kefs.v2.merged`)
  - Integrated with existing PDS API credentials and cloud storage settings

## Documentation

- Added comprehensive documentation for 9 new functions with detailed examples:
  - `match_surveys_to_gps_trips.Rd`: Main workflow function with two-step matching process
  - `merge_trips.Rd`: Kenya-specific end-to-end pipeline
  - `match_surveys_to_registry.Rd`: Registry fuzzy matching algorithm
  - `match_imei_to_trip.Rd`: IMEI-date trip joining with uniqueness constraint
  - `build_registry_from_trips.Rd`: Implicit registry construction from historical trips
  - `standardize_column_names.Rd`, `clean_matching_fields.Rd`, `clean_registration.Rd`, `clean_text.Rd`: Helper functions


# peskas.kenya.data.pipeline 4.5.0

## New Features

- **API KEFS Data Export Pipeline**: Added new `export_api_raw()` function to export raw preprocessed survey data in API-friendly format
  - Exports raw/preprocessed trip data (before validation) to cloud storage
  - Part of a two-stage API export pipeline (raw and validated exports)
  - Transforms nested survey data into flat structure with standardized trip-level records
  - Generates unique trip IDs using xxhash64 algorithm
  - Exports versioned parquet files to `kenya/raw/` path for external API consumption
  - Includes comprehensive output schema with 14 standardized fields (trip_id, landing_date, gear, catch metrics, etc.)

## Improvements

- **Configuration Enhancements**:
  - Added `api` configuration section for trip data exports with separate raw/validated paths
  - Configured cloud storage paths for API exports (kenya/raw, kenya/validated)
  - Added Airtable base ID and token configuration for metadata management
  - Enhanced `options_api` storage configuration for peskas-coasts bucket

- **GitHub Actions Workflow**:
  - Added new `export-api-data` job to automated pipeline workflow
  - Configured API export job to run after survey preprocessing step
  - Added production environment configuration for API data exports

# peskas.kenya.data.pipeline 4.4.0

## New features

-   **KEFS V2 Validation Pipeline**:
    -   Added `validate_kefs_surveys_v2()` function for comprehensive validation of KEFS catch assessment surveys
    -   Integrated KoboToolbox API validation status querying with `get_validation_status()` and `update_validation_status()` functions
    -   Implemented multi-dimensional validation system with information, trip, catch, and indicator flags
    -   Added support for manual validation override to preserve human-reviewed approvals while applying automated checks
    -   Exported new validation helper functions: `get_trips_flags()`, `get_catch_flags()`, and `get_indicators_flags()`
    -   Added GitHub Actions workflow job `validate-kefs-catch-v2` for automated validation processing

## Enhancements

-   **KEFS Data Preprocessing Improvements**:
    -   Enhanced `preprocess_kefs_surveys_v2()` to include `submission_date` field for tracking when data was submitted
    -   Added `fishing_per_week` field extraction and parsing from survey data
    -   Improved catch outcome handling: automatically sets catch_outcome to "yes" when total_catch_weight > 0 but catch_outcome is NA
    -   Renamed weight and price columns for clarity:
        -   `sample_weight` → `total_sample_weight`
        -   `catch_weight` → `total_catch_weight`
        -   `price_kg` → `total_price_kg`
        -   `catch_price` → `total_catch_price`
    -   Enhanced enumerator name standardization integration
-   **Survey Data Reshaping**:
    -   Updated `reshape_priority_species()` to use more descriptive column name: `weight_priority` instead of `weight_kg`
    -   Updated `reshape_overall_sample()` to use standardized column names: `sample_weight` and `sample_price`
-   **Configuration Updates**:
    -   Added `KEFS_KOBO_TOKEN` environment variable for KoboToolbox API authentication
    -   Added validation flags file prefixes for both KEFS v1 and v2 in config.yml
    -   Configured cloud storage paths for validation output files

## Documentation

-   **Enhanced Function Documentation**:
    -   Added comprehensive documentation for `validate_kefs_surveys_v2()` including detailed workflow description, validation limits, and requirements
    -   Added documentation for KEFS validation helper functions:
        -   `get_trips_flags()`: Documents trip-level validation for horse power, fishers, duration, and revenue with 6 alert codes
        -   `get_catch_flags()`: Documents catch-level validation for sample weight inconsistencies with 2 alert codes
        -   `get_indicators_flags()`: Documents composite indicator validation (CPUE, RPUE, price/kg) with 3 alert codes
    -   Added documentation for KoboToolbox integration functions:
        -   `get_validation_status()`: Retrieves validation status from KoboToolbox for submissions
        -   `update_validation_status()`: Updates validation status in KoboToolbox
    -   Updated `get_indicators_flags()` documentation to correctly reflect the `clean_ids` parameter
    -   All new documentation follows roxygen2 style with comprehensive examples, parameter descriptions, and return value specifications
-   **New man pages added**:
    -   `get_catch_flags.Rd`, `get_indicators_flags.Rd`, `get_trips_flags.Rd`
    -   `get_validation_status.Rd`, `update_validation_status.Rd`
    -   `validate_kefs_surveys_v2.Rd`

## NAMESPACE

-   Exported new validation functions: `get_trips_flags`, `get_catch_flags`, `get_indicators_flags`
-   Exported KoboToolbox integration functions: `get_validation_status`, `update_validation_status`
-   Exported `validate_kefs_surveys_v2` for KEFS V2 validation workflow

# peskas.kenya.data.pipeline 4.3.0

## Configuration

-   **Environment-based Configuration**:
    -   Migrated authentication from file-based (`auth/`) to environment variable approach using `.env` files
    -   Improved security and deployment flexibility by using environment variables for credentials
    -   Added `.env.example` for reference configuration

## Fixes

-   **Package Quality and R CMD Check**:
    -   Fixed function naming conflict: renamed `get_fishery_metrics()` in preprocessing.R to `get_fishery_metrics_long()` to avoid duplicate function definitions
    -   Corrected parameter names in helper functions (`get_fishery_metrics()`, `get_individual_metrics()`, `get_individual_gear_metrics()`) to match documentation
    -   Resolved "unused arguments" error in `export_summaries()` function calls
    -   Updated function documentation to align with actual parameter names

# peskas.kenya.data.pipeline 4.2.0

## New features

-   **GPS Track Processing and Analysis**:
    -   Added `process_fishing_tracks()` for comprehensive GPS track processing and fishing activity classification
    -   Implemented `prepare_gps_data()` to convert raw tracking data into GPSMonitoring format
    -   Added `classify_fishing_activity()` using speed thresholds and spatial clustering to identify fishing vs transit activities
    -   Created `process_trajectories_with_speed()` for calculating vessel speeds from GPS positions
    -   Implemented `calculate_fishing_summaries()` to generate effort metrics by trip and spatial grid
    -   Added `visualize_fishing_track()` for mapping fishing activities
    -   Created `create_exclusion_zones()` and `create_extent_polygon()` for spatial analysis

-   **Airtable Integration**:
    -   Implemented bidirectional Airtable API integration with `airtable_to_df()` for reading records
    -   Added `df_to_airtable()` for bulk create operations
    -   Created `update_airtable_record()` for updating individual records
    -   Implemented `bulk_update_airtable()` for batch update operations
    -   Added `get_writable_fields()` to retrieve field schemas and validate writable fields

-   **Fishery Metrics Expansion**:
    -   Extended `get_fishery_metrics()` with normalized long format output for maximum interoperability
    -   Added metrics for CPUE and RPUE by gear type
    -   Implemented species composition analysis with top 2 species ranking
    -   Created fully normalized dataset structure for flexible aggregation and filtering

-   **PDS Tracker Report**:
    -   Added comprehensive GPS tracking visualization report (inst/reports/pds/trackers.qmd)
    -   Integrated interactive maps, time series, and network analysis visualizations
    -   Implemented spatial heatmaps and trip trajectory analysis

## Enhancements

-   **Package Quality and Compliance**:
    -   Fixed R CMD check warnings by replacing `library()`/`require()` calls with `::` notation and `requireNamespace()`
    -   Added missing global variable bindings for track and fishery metrics variables
    -   Updated NAMESPACE with new imports: `dplyr::n`, `dplyr::arrange`, `dplyr::row_number`, `stats::lag`, `stats::time`, `rlang::sym`
    -   Added GPSMonitoring and ggplot2 to Suggests dependencies
    -   Excluded `.claude` directory and `.parquet` files from package build via .Rbuildignore
    -   Reduced R CMD check from 3 warnings/6 notes to 2 warnings/2 notes

-   **KEFS Data Integration**:
    -   Updated configuration (inst/config.yml) for KEFS survey data integration
    -   Enhanced inst/kefs_integration.R with improved data processing workflows

## Documentation

-   Added 14 new man pages for GPS tracking functions:
    -   `process_fishing_tracks.Rd`, `prepare_gps_data.Rd`, `classify_fishing_activity.Rd`
    -   `process_trajectories_with_speed.Rd`, `calculate_fishing_summaries.Rd`
    -   `create_exclusion_zones.Rd`, `create_extent_polygon.Rd`, `visualize_fishing_track.Rd`
-   Added 6 new man pages for Airtable integration:
    -   `airtable_to_df.Rd`, `df_to_airtable.Rd`, `update_airtable_record.Rd`
    -   `bulk_update_airtable.Rd`, `get_writable_fields.Rd`
-   Updated `get_fishery_metrics.Rd` to document dual function signatures for different use cases
-   Enhanced documentation with comprehensive examples and parameter descriptions

# peskas.kenya.data.pipeline 4.1.0

## Enhancements

-   **Improved MongoDB Data Handling**:
    -   Enhanced `mdb_collection_pull()` to properly handle MongoDB ObjectId fields by explicitly including `_id` in queries and maintaining correct column ordering
    -   Updated column reordering logic to prioritize `_id` field placement in retrieved datasets
    -   Improved data consistency and integrity when pulling data from MongoDB collections

-   **Code Style and Formatting**:
    -   Standardized function parameter formatting across storage functions for improved readability
    -   Enhanced code consistency in `upload_parquet_to_cloud()`, `cloud_object_name()`, and related functions
    -   Updated `get_metadata()` function formatting to follow consistent style guidelines

-   **User Management Infrastructure**:
    -   Added comprehensive user management system for MongoDB dashboard with treatment group support
    -   Implemented secure password generation and user creation functionality
    -   Added support for BMU-based user access control and role assignment

# peskas.kenya.data.pipeline 3.2.1

## New features

-   **Individual Fisher Metrics Data Extraction**:
    -   Added `get_individual_gear_metrics()` to calculates gear key fishery performance metrics at the individual fisher level from validated catch data, including CPUE (Catch Per Unit Effort), RPUE (Revenue Per Unit Effort), price per kg, trip costs, and profit margins for each fisher.
    -   Integrated the fish composition distribution at fishers level

# peskas.kenya.data.pipeline 3.2.0

## New features

-   **Individual Fisher Metrics Data Extraction**:
    -   Added `get_individual_metrics()` to calculates key fishery performance metrics at the individual fisher level from validated catch data, including CPUE (Catch Per Unit Effort), RPUE (Revenue Per Unit Effort), price per kg, trip costs, and profit margins for each fisher.

# peskas.kenya.data.pipeline 3.1.0

## New features

-   **Individual Fisher Data Extraction**:
    -   Added `get_individual_data()` to extract fisher IDs and trip costs from raw survey data, enabling more granular analysis of fishing effort and expenses.
    -   Integrated individual fisher and trip cost data into the version 2 preprocessing pipeline (`preprocess_landings_v2()`), with new columns for `fisher_id` and `trip_cost` in preprocessed outputs.

## Enhancements

-   **Expanded Data Merging**:
    -   Updated `merge_landings()` to support merging of legacy, v1, and v2 preprocessed landings, including new fields for individual fisher and trip cost data.
    -   Improved column selection and ordering in merged datasets for consistency and downstream compatibility.
-   **Export and Summarization Improvements**:
    -   Enhanced `export_summaries()` to include new individual-level statistics, such as mean trip expenses per fisher, aggregated by BMU and month (for data collected after June 25, 2025).
    -   Improved monthly and distribution summaries to reflect the expanded data model.

## Configuration

-   Updated `config.yml` to support new data paths and fields required for individual fisher and trip cost data in v2 surveys.

## Documentation

-   Added new man page for `get_individual_data()`.
-   Updated documentation for all affected functions to reflect new parameters and outputs.

# peskas.kenya.data.pipeline 3.0.0

## New features

-   **Versioned Survey Processing**:
    -   Added version-specific ingestion functions for catch and price surveys
    -   Implemented `ingest_catch_survey_version()` and `ingest_price_survey_version()` for versioned data handling
    -   Created versioned preprocessing functions (`preprocess_landings_v1()` and `preprocess_landings_v2()`)
    -   Added core preprocessing functions for modular data transformation

## Enhancements

-   **Improved Data Processing Pipeline**:
    -   Refactored ingestion logic to handle multiple survey versions
    -   Enhanced preprocessing workflow with version-specific transformations
    -   Updated configuration structure to support versioned data paths
    -   Improved documentation for all versioned functions

## Documentation

-   Added comprehensive documentation for new versioned functions
-   Updated existing function documentation to reflect versioning changes
-   Added new man pages for version-specific functions
-   Enhanced function descriptions and examples

# peskas.kenya.data.pipeline 2.1.0

## New features

-   **Geospatial Export Capabilities**:
    -   Added `create_geos()` function to generate GeoJSON files with regional-level fishery metrics
    -   Implemented spatial analysis to assign BMUs to nearest coastal regions
    -   Exported time series of aggregated metrics (CPUE, CPUA, effort, RPUE, RPUA) at regional level
    -   Added regional polygon geometries for spatial visualization

## Enhancements

-   **Temporal Data Processing**:
    -   Revised fishery metrics calculation for more accurate temporal representation
    -   Implemented daily-average approach instead of aggregate-then-divide method
    -   Maintained per-day units (kg/fisher/day, KES/km²/day) consistent with scientific standards
    -   Fixed CPUE and other daily metrics by calculating at daily level first
    -   Filtered export data to include only records from 2023 onwards

## Fixes

-   **Data Quality Improvements**:
    -   Corrected handling of missing information with NA values instead of zeros
    -   Fixed dependency logic of the pipeline
    -   Improved price table prefix path handling
    -   Updated configuration paths for downloading preprocessed price and legacy catch data

# peskas.kenya.data.pipeline 2.0.0

## New features

-   **Alternative Validation Method**:
    -   Added new validation functions using Interquartile Range (IQR) method:
        -   `alert_outlier_iqr()` for IQR-based outlier detection
        -   `check_outliers_iqr()` for basic outlier checking
        -   `validate_nfishers_iqr()` and `validate_nboats_iqr()` for validating fisher and boat counts
        -   `validate_catch_iqr()` and `validate_total_catch_iqr()` for catch validation
        -   `get_catch_bounds_iqr()` and `get_total_catch_bounds_iqr()` for calculating bounds
    -   Modified `validate_landings()` to use IQR validation by default

## Enhancements

-   **Improved Error Handling**:
    -   Added comprehensive input validation in all IQR functions
    -   Added consistent NULL/empty data checks
    -   Improved NA value handling using dplyr::if_else
    -   Added warnings for cases where bounds cannot be calculated

## Documentation

-   Added full documentation for all new IQR functions with examples
-   Updated NAMESPACE to export new IQR functions
-   Added proper Roxygen documentation for all new parameters

## Other Changes

-   Switched default validation method from MAD to IQR in main pipeline
-   Maintained backward compatibility with existing MAD functions

## New features

-   **Migration to Google Cloud Storage**:
    -   Replaced MongoDB storage with Google Cloud Storage (GCS) using Parquet files
    -   Added new cloud storage functions for GCS operations:
        -   Authentication and connection management
        -   Upload and download capabilities
        -   Versioned file handling
    -   Integrated Apache Arrow for efficient Parquet file processing
    -   Updated configuration to support GCS file prefixes and paths

## Enhancements

-   **Storage Operations**:
    -   Refactored storage operations to use cloud-native approaches
    -   Improved data access performance with Parquet file format
    -   Added support for versioned file management

## Dependencies

-   Added cloud storage related libraries
-   Added Arrow for Parquet file handling

# peskas.kenya.data.pipeline 1.0.0

## New features

-   **Catch Validation Enhancements**:
    -   Added `validate_fishers_catch()` to flag cases where a single fisher reports an excessively high catch.
    -   Introduced `impute_price()` to fill missing fish prices using median values across landing sites and sizes.
    -   Integrated price validation in `validate_landings()` to ensure missing or zero prices are flagged.
-   **Revenue Metrics in Summaries**:
    -   `export_summaries()` now calculates revenue-based effort metrics:
        -   `rpue` (Revenue Per Unit Effort) as aggregated price per fisher
        -   `rpua` (Revenue Per Unit Area) as aggregated price per square km
-   **Extended Workflow Automation**:
    -   Updated GitHub Actions workflow (`data-pipeline.yaml`) to include a job for processing and merging price data.
    -   Added price-related MongoDB collections (`raw_price`, `preprocessed_price`, `price_table`) to `config.yml`.

## Enhancements

-   **Improved Validation and Data Processing**:
    -   `validate_total_catch()` and `validate_catch()` now allow customizable `flag_value` parameters.
    -   Enhanced `merge_prices()` with fixes for site name inconsistencies (e.g., "Rigati" → "Rigata").
    -   `preprocess_legacy_landings()` now ensures valid dates by correcting invalid timestamps.
-   **Refined Data Summarization**:
    -   `export_summaries()` now includes revenue metrics while maintaining prior CPUE and CPUA calculations.
    -   Updated `.qmd` reports to reflect new price fields in the summary tables.

## Fixes

-   **Bug Fixes and Consistency Improvements**:
    -   Fixed potential issues in `merge_prices()` that could lead to duplicate price records.
    -   Ensured `impute_price()` correctly applies median imputation across all relevant data points.
    -   Updated `.Rd` documentation for all modified functions to reflect new parameters and changes.

# peskas.kenya.data.pipeline 0.9.0

## New features

-   **Price Data Integration**: Added support for processing fish price data alongside catch data.
    -   Introduced `ingest_landings_price()` to download price surveys from KoboToolbox.
    -   Implemented `preprocess_price_landings()` to clean and standardize fish price data.
    -   Added `merge_prices()` to combine and aggregate legacy and ongoing price data.
    -   Created `summarise_catch_price()` to compute median price per kilogram by landing site and fish category.
-   **Extended Workflow Automation**:
    -   Added a new GitHub Actions job `merge-price-data` to automate the processing and merging of price data.
-   **Expanded MongoDB Collections**:
    -   Introduced new collections in `config.yml` for handling raw, preprocessed, and aggregated price data (`raw_price`, `preprocessed_price`, and `price_table`).

## Enhancements

-   **Validation and Alert Flagging**:
    -   Improved handling of validation alert flags by introducing dynamic `flag_value` parameters.
    -   Adjusted anomaly detection in `validate_landings()` to use more descriptive alert codes.
-   **Configuration Updates**:
    -   Updated `.github/workflows/data-pipeline.yaml` to include price survey ingestion and processing.
    -   Extended `config.yml` to support price data processing in both local and production environments.
-   **Namespace Expansion**:
    -   Exported new functions (`ingest_landings_price`, `merge_prices`, `preprocess_price_landings`, `summarise_catch_price`) to make them available for package use.

## Fixes

-   **Documentation Corrections**:
    -   Updated `.Rd` documentation for `ingest_landings()` to clarify that it processes **catch surveys** (not all WCS surveys).
    -   Added descriptions for new parameters (`flag_value`) in validation function documentation.
-   **Bug Fixes**:
    -   Resolved inconsistencies in price data processing by ensuring column renaming and format standardization.
    -   Fixed logical errors in merging price data to prevent duplicate records.
    -   Improved MongoDB push operations to correctly handle newly introduced price data collections.

# peskas.kenya.data.pipeline 0.8.0

## New features

-   Introduced `flag_value` parameter to validation functions (`validate_dates`, `validate_nfishers`, `validate_nboats`, and `validate_catch`) for customizable alert thresholds.
-   Added `logical_check` and `anomalous_submissions` workflows to improve anomaly detection in `validate_landings`.
-   Enhanced data upload pipeline with `purrr::walk2` for streamlined MongoDB uploads of validated and flagged data.

## Enhancements

-   Updated `export_summaries` to calculate `mean_trip_catch` using the median for improved robustness against skewed data.
-   Improved documentation for validation functions to include detailed descriptions and examples for the new `flag_value` parameter.
-   Refined the logic in `validate_landings` for clearer alert flagging and better handling of edge cases.
-   Added the `validation_flags` collection to the configuration file (`config.yml`) for centralized management of flagging outputs.
-   Updated anomaly alert flags to use descriptive string values instead of numeric codes for better interpretability.

## Fixes

-   Fixed inconsistencies in the handling of alert flags during data validation.
-   Resolved potential mismatches in merged datasets by improving join logic in `validate_landings`.
-   Corrected documentation typos and improved consistency across `.Rd` files.

# peskas.kenya.data.pipeline 0.7.0

## New features

-   Introduced `get_total_catch_bounds` for calculating upper bounds on total catch data grouped by landing site and gear type.
-   Added `validate_total_catch` function for validating total catch data with improved outlier handling.
-   Updated `validate_landings` to identify and filter out inconsistent submissions (e.g., mismatched fishers and boats).
-   Enhanced `validate_catch` to set outlier values in `catch_kg` to NA and flag them for review.

## Enhancements

-   Improved `get_catch_bounds` to exclude invalid categories and clarified grouping logic for gear and fish categories.
-   Updated documentation across validation and catch bounds functions to include better descriptions, keywords, and return values.
-   Refined data preprocessing in `preprocess_landings` to ensure unique rows, handle NA values, and streamline workflow.
-   Adjusted configuration parameters in `config.yml` to enhance validation sensitivity (e.g., increasing `k_catch` for outlier detection).
-   Updated export_summaries to improve monthly summaries by recalculating effort as fishers per km² per day and refining CPUE and CPUA metrics to include temporal normalization. Simplified mean trip catch calculations for consistency across datasets.

## Fixes

-   Corrected plot code formatting in `data_report.qmd` for CPUE, effort, and CPUA visualizations.
-   Fixed documentation typos and inconsistencies in `.Rd` files for validation functions.

# peskas.kenya.data.pipeline 0.6.0

## New features

-   Add merge_landings function for combining data sources
-   Add validation catch function to improve data quality checks
-   Include form consent and total catch fields in merged data
-   Calculate total catch in legacy landings

## Enhancements

-   Homogenise gear and fish groups names
-   Update validation code structure
-   Fix mismatch between fish groups sum and total catch
-   Fix landing sites names
-   Improve validation catch function
-   Update export data based on validation
-   Fix legacy submission IDs

# peskas.kenya.data.pipeline 0.5.1

## New features

-   Add functions to extract effort and CPUE from validated legacy data
-   Implement MongoDB pushing functionality
-   Add storage and metadata functions

## Enhancements

-   Index functions by topic and package functional position
-   Update package documentation and website
-   Improve MongoDB storage calls
-   Add configuration file for MongoDB database and collection references

## Fixes

-   Drop stale files
-   Update storage calls for better efficiency
-   Fix various typos and syntax errors

# peskas.kenya.data.pipeline 0.5.0

## Enhancements

-   Improve mongoDB storage functions by adding indexes to improve query performance. Before this fix column order in data was not preserved.

# peskas.kenya.data.pipeline 0.4.0

## New features

-   Now `ingest_surveys()` uses Kobotoolbox API directly to download surveys instead of rely on R package

## Fixes

-   Fix ingestion functions as it was limited to download max 30,000 submissions. The approach now uses pagination to retrieve large datasets, with a limit of 30,000 records per request

# peskas.kenya.data.pipeline 0.3.0

## New features

-   Add ingestion function to ingest ongoing data `ingest_surveys()`
-   Add preprocessing function to preprocess ongoing data `preprocess_landings()`

# peskas.kenya.data.pipeline 0.2.0

-   Add package website with documentation functions
-   Integrate mongodb storage with package functions
-   Optimize configuration file

# peskas.kenya.data.pipeline 0.1.0

-   Initial CRAN submission.
