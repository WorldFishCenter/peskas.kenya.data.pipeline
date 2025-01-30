# peskas.kenya.data.pipeline 1.0.0

## New features
- **Catch Validation Enhancements**:
  - Added `validate_fishers_catch()` to flag cases where a single fisher reports an excessively high catch.
  - Introduced `impute_price()` to fill missing fish prices using median values across landing sites and sizes.
  - Integrated price validation in `validate_landings()` to ensure missing or zero prices are flagged.
  
- **Revenue Metrics in Summaries**:
  - `export_summaries()` now calculates revenue-based effort metrics:
    - `rpue` (Revenue Per Unit Effort) as aggregated price per fisher
    - `rpua` (Revenue Per Unit Area) as aggregated price per square km

- **Extended Workflow Automation**:
  - Updated GitHub Actions workflow (`data-pipeline.yaml`) to include a job for processing and merging price data.
  - Added price-related MongoDB collections (`raw_price`, `preprocessed_price`, `price_table`) to `config.yml`.

## Enhancements
- **Improved Validation and Data Processing**:
  - `validate_total_catch()` and `validate_catch()` now allow customizable `flag_value` parameters.
  - Enhanced `merge_prices()` with fixes for site name inconsistencies (e.g., "Rigati" → "Rigata").
  - `preprocess_legacy_landings()` now ensures valid dates by correcting invalid timestamps.

- **Refined Data Summarization**:
  - `export_summaries()` now includes revenue metrics while maintaining prior CPUE and CPUA calculations.
  - Updated `.qmd` reports to reflect new price fields in the summary tables.

## Fixes
- **Bug Fixes and Consistency Improvements**:
  - Fixed potential issues in `merge_prices()` that could lead to duplicate price records.
  - Ensured `impute_price()` correctly applies median imputation across all relevant data points.
  - Updated `.Rd` documentation for all modified functions to reflect new parameters and changes.

# peskas.kenya.data.pipeline 0.9.0

## New features
- **Price Data Integration**: Added support for processing fish price data alongside catch data.
  - Introduced `ingest_landings_price()` to download price surveys from KoboToolbox.
  - Implemented `preprocess_price_landings()` to clean and standardize fish price data.
  - Added `merge_prices()` to combine and aggregate legacy and ongoing price data.
  - Created `summarise_catch_price()` to compute median price per kilogram by landing site and fish category.
- **Extended Workflow Automation**:
  - Added a new GitHub Actions job `merge-price-data` to automate the processing and merging of price data.
- **Expanded MongoDB Collections**:
  - Introduced new collections in `config.yml` for handling raw, preprocessed, and aggregated price data (`raw_price`, `preprocessed_price`, and `price_table`).

## Enhancements
- **Validation and Alert Flagging**:
  - Improved handling of validation alert flags by introducing dynamic `flag_value` parameters.
  - Adjusted anomaly detection in `validate_landings()` to use more descriptive alert codes.
- **Configuration Updates**:
  - Updated `.github/workflows/data-pipeline.yaml` to include price survey ingestion and processing.
  - Extended `config.yml` to support price data processing in both local and production environments.
- **Namespace Expansion**:
  - Exported new functions (`ingest_landings_price`, `merge_prices`, `preprocess_price_landings`, `summarise_catch_price`) to make them available for package use.

## Fixes
- **Documentation Corrections**:
  - Updated `.Rd` documentation for `ingest_landings()` to clarify that it processes **catch surveys** (not all WCS surveys).
  - Added descriptions for new parameters (`flag_value`) in validation function documentation.
- **Bug Fixes**:
  - Resolved inconsistencies in price data processing by ensuring column renaming and format standardization.
  - Fixed logical errors in merging price data to prevent duplicate records.
  - Improved MongoDB push operations to correctly handle newly introduced price data collections.

# peskas.kenya.data.pipeline 0.8.0

## New features
- Introduced `flag_value` parameter to validation functions (`validate_dates`, `validate_nfishers`, `validate_nboats`, and `validate_catch`) for customizable alert thresholds.
- Added `logical_check` and `anomalous_submissions` workflows to improve anomaly detection in `validate_landings`.
- Enhanced data upload pipeline with `purrr::walk2` for streamlined MongoDB uploads of validated and flagged data.

## Enhancements
- Updated `export_summaries` to calculate `mean_trip_catch` using the median for improved robustness against skewed data.
- Improved documentation for validation functions to include detailed descriptions and examples for the new `flag_value` parameter.
- Refined the logic in `validate_landings` for clearer alert flagging and better handling of edge cases.
- Added the `validation_flags` collection to the configuration file (`config.yml`) for centralized management of flagging outputs.
- Updated anomaly alert flags to use descriptive string values instead of numeric codes for better interpretability.

## Fixes
- Fixed inconsistencies in the handling of alert flags during data validation.
- Resolved potential mismatches in merged datasets by improving join logic in `validate_landings`.
- Corrected documentation typos and improved consistency across `.Rd` files.

# peskas.kenya.data.pipeline 0.7.0

## New features
- Introduced `get_total_catch_bounds` for calculating upper bounds on total catch data grouped by landing site and gear type.
- Added `validate_total_catch` function for validating total catch data with improved outlier handling.
- Updated `validate_landings` to identify and filter out inconsistent submissions (e.g., mismatched fishers and boats).
- Enhanced `validate_catch` to set outlier values in `catch_kg` to NA and flag them for review.

## Enhancements
- Improved `get_catch_bounds` to exclude invalid categories and clarified grouping logic for gear and fish categories.
- Updated documentation across validation and catch bounds functions to include better descriptions, keywords, and return values.
- Refined data preprocessing in `preprocess_landings` to ensure unique rows, handle NA values, and streamline workflow.
- Adjusted configuration parameters in `config.yml` to enhance validation sensitivity (e.g., increasing `k_catch` for outlier detection).
- Updated export_summaries to improve monthly summaries by recalculating effort as fishers per km² per day and refining CPUE and CPUA metrics to include temporal normalization. Simplified mean trip catch calculations for consistency across datasets.

## Fixes
- Corrected plot code formatting in `data_report.qmd` for CPUE, effort, and CPUA visualizations.
- Fixed documentation typos and inconsistencies in `.Rd` files for validation functions.

# peskas.kenya.data.pipeline 0.6.0
## New features
- Add merge_landings function for combining data sources
- Add validation catch function to improve data quality checks
- Include form consent and total catch fields in merged data
- Calculate total catch in legacy landings

## Enhancements
- Homogenise gear and fish groups names
- Update validation code structure
- Fix mismatch between fish groups sum and total catch
- Fix landing sites names
- Improve validation catch function
- Update export data based on validation
- Fix legacy submission IDs

# peskas.kenya.data.pipeline 0.5.1
## New features
- Add functions to extract effort and CPUE from validated legacy data
- Implement MongoDB pushing functionality
- Add storage and metadata functions

## Enhancements
- Index functions by topic and package functional position
- Update package documentation and website
- Improve MongoDB storage calls
- Add configuration file for MongoDB database and collection references

## Fixes
- Drop stale files
- Update storage calls for better efficiency
- Fix various typos and syntax errors

# peskas.kenya.data.pipeline 0.5.0
## Enhancements
- Improve mongoDB storage functions by adding indexes to improve query performance. Before this fix column order in data was not preserved.

# peskas.kenya.data.pipeline 0.4.0
## New features
- Now `ingest_surveys()` uses Kobotoolbox API directly to download surveys instead of rely on R package

## Fixes
- Fix ingestion functions as it was limited to download max 30,000 submissions. The approach now uses pagination to retrieve large datasets, with a limit of 30,000 records per request

# peskas.kenya.data.pipeline 0.3.0
## New features
- Add ingestion function to ingest ongoing data `ingest_surveys()`
- Add preprocessing function to preprocess ongoing data `preprocess_landings()`

# peskas.kenya.data.pipeline 0.2.0
- Add package website with documentation functions
- Integrate mongodb storage with package functions
- Optimize configuration file 

# peskas.kenya.data.pipeline 0.1.0
* Initial CRAN submission.
