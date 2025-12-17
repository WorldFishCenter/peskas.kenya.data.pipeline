# Package index

## Workflow

These are arguably the most important functions in the package. Each of
these functions executes a step in the data pipeline.

- [`export_summaries()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/export_summaries.md)
  : Export Summarized Fishery Data for Dashboard Integration
- [`export_validation_flags()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/export_validation_flags.md)
  : Export Validation Flags to MongoDB
- [`get_validation_status()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_validation_status.md)
  : Get Validation Status from KoboToolbox
- [`ingest_kefs_surveys_v1()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/ingest_kefs_surveys_v1.md)
  : Download and Process KEFS (BMU DAILY ARTISANAL 2025) Catch Surveys
  from Kobotoolbox
- [`ingest_kefs_surveys_v2()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/ingest_kefs_surveys_v2.md)
  : Download and Process KEFS (CATCH ASSESSMENT QUESTIONNAIRE) Catch
  Surveys from Kobotoolbox
- [`ingest_landings_price()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/ingest_landings_price.md)
  : Download and Process WCS Price Surveys from Kobotoolbox
- [`ingest_pds_tracks()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/ingest_pds_tracks.md)
  : Ingest Pelagic Data Systems (PDS) Track Data
- [`ingest_pds_trips()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/ingest_pds_trips.md)
  : Ingest Pelagic Data Systems (PDS) Trip Data
- [`ingest_wcs_surveys()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/ingest_wcs_surveys.md)
  : Download and Process WCS Catch Surveys from Kobotoolbox
- [`merge_landings()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/merge_landings.md)
  : Merge Legacy and Ongoing Landings Data
- [`merge_prices()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/merge_prices.md)
  : Merge Price Data
- [`preprocess_kefs_surveys_v1()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/preprocess_kefs_surveys_v1.md)
  : Preprocess KEFS Survey Data
- [`preprocess_kefs_surveys_v2()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/preprocess_kefs_surveys_v2.md)
  : Preprocess KEFS (CATCH ASSESSMENT QUESTIONNAIRE) Survey Data
- [`preprocess_landings_v1()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/preprocess_landings_v1.md)
  : Preprocess Landings Data (Version 1)
- [`preprocess_landings_v2()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/preprocess_landings_v2.md)
  : Preprocess Landings Data (Version 2)
- [`preprocess_legacy_landings()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/preprocess_legacy_landings.md)
  : Preprocess Legacy Landings Data
- [`preprocess_pds_tracks()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/preprocess_pds_tracks.md)
  : Preprocess Pelagic Data Systems (PDS) Track Data
- [`preprocess_price_landings()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/preprocess_price_landings.md)
  : Preprocess Price Data
- [`sync_validation_submissions()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/sync_validation_submissions.md)
  : Synchronize Validation Statuses with KoboToolbox
- [`update_validation_status()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/update_validation_status.md)
  : Update Validation Status in KoboToolbox
- [`validate_kefs_surveys_v2()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/validate_kefs_surveys_v2.md)
  : Validate KEFS Surveys Data (Version 2)
- [`validate_landings()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/validate_landings.md)
  : Validate Fisheries Data

## Cloud Storage

Functions that interact with cloud storage providers.

- [`cloud_object_name()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/cloud_object_name.md)
  : Retrieve Full Name of Versioned Cloud Object
- [`cloud_storage_authenticate()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/cloud_storage_authenticate.md)
  : Authenticate to a Cloud Storage Provider
- [`download_cloud_file()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/download_cloud_file.md)
  : Download Object from Cloud Storage
- [`download_parquet_from_cloud()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/download_parquet_from_cloud.md)
  : Download Parquet File from Cloud Storage
- [`get_metadata()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_metadata.md)
  : Get metadata tables
- [`mdb_collection_pull()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/mdb_collection_pull.md)
  : Retrieve Data from MongoDB
- [`mdb_collection_push()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/mdb_collection_push.md)
  : Upload Data to MongoDB and Overwrite Existing Content
- [`upload_cloud_file()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/upload_cloud_file.md)
  : Upload File to Cloud Storage
- [`upload_parquet_to_cloud()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/upload_parquet_to_cloud.md)
  : Upload Processed Data to Cloud Storage

## Ingestion

Functions dedicated to the ingestion module

- [`airtable_to_df()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/airtable_to_df.md)
  : Get All Records from Airtable with Pagination
- [`get_kobo_data()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_kobo_data.md)
  : Retrieve Data from Kobotoolbox API
- [`get_trip_points()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_trip_points.md)
  : Get Trip Points from Pelagic Data Systems API
- [`get_trips()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_trips.md)
  : Retrieve Trip Details from Pelagic Data API
- [`ingest_kefs_surveys_v1()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/ingest_kefs_surveys_v1.md)
  : Download and Process KEFS (BMU DAILY ARTISANAL 2025) Catch Surveys
  from Kobotoolbox
- [`ingest_kefs_surveys_v2()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/ingest_kefs_surveys_v2.md)
  : Download and Process KEFS (CATCH ASSESSMENT QUESTIONNAIRE) Catch
  Surveys from Kobotoolbox
- [`ingest_landings_price()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/ingest_landings_price.md)
  : Download and Process WCS Price Surveys from Kobotoolbox
- [`ingest_pds_tracks()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/ingest_pds_tracks.md)
  : Ingest Pelagic Data Systems (PDS) Track Data
- [`ingest_pds_trips()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/ingest_pds_trips.md)
  : Ingest Pelagic Data Systems (PDS) Trip Data
- [`ingest_wcs_surveys()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/ingest_wcs_surveys.md)
  : Download and Process WCS Catch Surveys from Kobotoolbox

## Preprocessing

Functions dedicated to the preprocessing module

- [`clean_catch_names()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/clean_catch_names.md)
  : Clean Catch Names
- [`fetch_asset()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/fetch_asset.md)
  : Fetch and Filter Asset Data from Airtable
- [`fetch_assets()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/fetch_assets.md)
  : Fetch Multiple Asset Tables from Airtable
- [`generate_track_summaries()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/generate_track_summaries.md)
  : Generate Grid Summaries for Track Data
- [`get_airtable_form_id()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_airtable_form_id.md)
  : Get Airtable Form ID from KoBoToolbox Asset ID
- [`map_surveys()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/map_surveys.md)
  : Map Survey Labels to Standardized Taxa, Gear, and Vessel Names
- [`preprocess_kefs_surveys_v1()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/preprocess_kefs_surveys_v1.md)
  : Preprocess KEFS Survey Data
- [`preprocess_kefs_surveys_v2()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/preprocess_kefs_surveys_v2.md)
  : Preprocess KEFS (CATCH ASSESSMENT QUESTIONNAIRE) Survey Data
- [`preprocess_landings_v1()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/preprocess_landings_v1.md)
  : Preprocess Landings Data (Version 1)
- [`preprocess_landings_v2()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/preprocess_landings_v2.md)
  : Preprocess Landings Data (Version 2)
- [`preprocess_legacy_landings()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/preprocess_legacy_landings.md)
  : Preprocess Legacy Landings Data
- [`preprocess_pds_tracks()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/preprocess_pds_tracks.md)
  : Preprocess Pelagic Data Systems (PDS) Track Data
- [`preprocess_price_landings()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/preprocess_price_landings.md)
  : Preprocess Price Data
- [`preprocess_track_data()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/preprocess_track_data.md)
  : Preprocess Track Data into Spatial Grid Summary
- [`reshape_catch_data_v1()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/reshape_catch_data_v1.md)
  : Reshape catch details from wide to long format
- [`reshape_overall_sample()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/reshape_overall_sample.md)
  : Reshape Overall Sample Weight Data from Wide to Long Format
- [`reshape_priority_species()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/reshape_priority_species.md)
  : Reshape Priority Species Catch Data from Wide to Long Format
- [`standardize_enumerator_names()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/standardize_enumerator_names.md)
  : Standardize Enumerator Names

## Validation

Functions dedicated to the validation module

- [`alert_outlier()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/alert_outlier.md)
  :

  Generate an alert vector based on the
  [`univOutl::LocScaleB()`](https://rdrr.io/pkg/univOutl/man/LocScaleB.html)
  function

- [`alert_outlier_iqr()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/alert_outlier_iqr.md)
  : Generate an alert vector based on IQR method

- [`check_outliers_iqr()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/check_outliers_iqr.md)
  : Check for outliers using IQR method

- [`export_validation_flags()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/export_validation_flags.md)
  : Export Validation Flags to MongoDB

- [`get_catch_bounds()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_catch_bounds.md)
  : Get fish groups Catch Bounds

- [`get_catch_bounds_iqr()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_catch_bounds_iqr.md)
  : Get fish groups Catch Bounds using IQR method

- [`get_catch_flags()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_catch_flags.md)
  : Generate Catch-Level Validation Flags

- [`get_indicators_flags()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_indicators_flags.md)
  : Generate Composite Indicator Validation Flags

- [`get_total_catch_bounds()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_total_catch_bounds.md)
  : Get Total Catch Bounds

- [`get_total_catch_bounds_iqr()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_total_catch_bounds_iqr.md)
  : Get Total Catch Bounds using IQR method

- [`get_trips_flags()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_trips_flags.md)
  : Generate Trip-Level Validation Flags

- [`get_validation_status()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_validation_status.md)
  : Get Validation Status from KoboToolbox

- [`sync_validation_submissions()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/sync_validation_submissions.md)
  : Synchronize Validation Statuses with KoboToolbox

- [`update_validation_status()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/update_validation_status.md)
  : Update Validation Status in KoboToolbox

- [`validate_catch()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/validate_catch.md)
  : Validate Individual Catch Data

- [`validate_catch_iqr()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/validate_catch_iqr.md)
  : Validate Individual Catch Data using IQR method

- [`validate_dates()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/validate_dates.md)
  : Validate Landing Dates

- [`validate_fishers_catch()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/validate_fishers_catch.md)
  : Validate Catch per Fisher

- [`validate_kefs_surveys_v2()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/validate_kefs_surveys_v2.md)
  : Validate KEFS Surveys Data (Version 2)

- [`validate_landings()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/validate_landings.md)
  : Validate Fisheries Data

- [`validate_nboats()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/validate_nboats.md)
  : Validate Number of Boats

- [`validate_nboats_iqr()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/validate_nboats_iqr.md)
  : Validate Number of Boats using IQR method

- [`validate_nfishers()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/validate_nfishers.md)
  : Validate Number of Fishers

- [`validate_nfishers_iqr()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/validate_nfishers_iqr.md)
  : Validate Number of Fishers using IQR method

- [`validate_total_catch()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/validate_total_catch.md)
  : Validate Total Catch Data

- [`validate_total_catch_iqr()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/validate_total_catch_iqr.md)
  : Validate Total Catch Data using IQR method

## Export

Functions dedicated dissemination of processed and analysed fisheries
data

- [`bulk_update_airtable()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/bulk_update_airtable.md)
  : Bulk Update Multiple Airtable Records
- [`create_geos()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/create_geos.md)
  : Generate Geographic Regional Summaries of Fishery Data
- [`df_to_airtable()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/df_to_airtable.md)
  : Create New Airtable Records
- [`export_summaries()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/export_summaries.md)
  : Export Summarized Fishery Data for Dashboard Integration
- [`update_airtable_record()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/update_airtable_record.md)
  : Update Single Airtable Record

## Helper functions

Functions dedicated to data analytics and general statistics

- [`add_version()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/add_version.md)
  : Add timestamp and sha string to a file name
- [`fetch_asset()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/fetch_asset.md)
  : Fetch and Filter Asset Data from Airtable
- [`fetch_assets()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/fetch_assets.md)
  : Fetch Multiple Asset Tables from Airtable
- [`get_airtable_form_id()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_airtable_form_id.md)
  : Get Airtable Form ID from KoBoToolbox Asset ID
- [`get_fishery_metrics()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_fishery_metrics.md)
  : Calculate Fishery Performance Metrics
- [`get_fishery_metrics_long()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_fishery_metrics_long.md)
  : Calculate key fishery metrics by landing site and month in
  normalized long format
- [`get_individual_gear_metrics()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_individual_gear_metrics.md)
  : Calculate Individual Fisher Performance Metrics by Gear Type
- [`get_individual_metrics()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_individual_metrics.md)
  : Calculate Individual Fisher Performance Metrics
- [`get_writable_fields()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/get_writable_fields.md)
  : Get Writable Fields from Airtable Table
- [`impute_price()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/impute_price.md)
  : Impute Missing Fish Prices Using Median Values
- [`map_surveys()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/map_surveys.md)
  : Map Survey Labels to Standardized Taxa, Gear, and Vessel Names
- [`read_config()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/read_config.md)
  : Read configuration file
- [`reshape_overall_sample()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/reshape_overall_sample.md)
  : Reshape Overall Sample Weight Data from Wide to Long Format
- [`reshape_priority_species()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/reshape_priority_species.md)
  : Reshape Priority Species Catch Data from Wide to Long Format
- [`standardize_enumerator_names()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/standardize_enumerator_names.md)
  : Standardize Enumerator Names
- [`summarise_catch_price()`](https://worldfishcenter.github.io/peskas.kenya.data.pipeline/reference/summarise_catch_price.md)
  : Summarize Catch Price Data
