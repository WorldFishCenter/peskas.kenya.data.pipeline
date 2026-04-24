# ── Internal format helpers ────────────────────────────────────────────────────

#' Transform WCS survey data into the canonical API schema
#'
#' @param surveys_df Data frame of WF preprocessed or validated survey records.
#' @param conf Configuration list from [read_config()].
#' @return A tibble in the canonical API schema.
#' @noRd
format_api_wcs <- function(surveys_df, conf) {
  surveys_df |>
    dplyr::rowwise() |>
    dplyr::mutate(
      trip_id = paste0("TRIP_", .data$submission_id),
      survey_id = dplyr::case_when(
        .data$version == "1" ~ "legacy",
        .data$version == "2" ~ conf$ingestion$wcs$koboform$asset_id,
        .data$version == "3" ~ conf$ingestion$wcs$koboform_kf$asset_id_kf
      )
    ) |>
    dplyr::group_by(.data$survey_id, .data$submission_id) |>
    dplyr::mutate(
      n_catch = as.integer(seq_along(.data$submission_id)),
    ) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      trip_duration_hrs = 24,
      vessel_type = NA_character_,
      catch_habitat = NA_character_,
      catch_outcome = NA_character_,
      length_cm = NA_real_
    ) |>
    dplyr::select(
      "survey_id",
      "trip_id",
      "landing_date",
      "gaul_1_code",
      "gaul_1_name",
      "gaul_2_code",
      "gaul_2_name",
      n_fishers = "no_of_fishers",
      "trip_duration_hrs",
      "gear",
      "vessel_type",
      "catch_habitat",
      "catch_outcome",
      "n_catch",
      catch_taxon = "alpha3_code",
      "scientific_name",
      "length_cm",
      "catch_kg",
      "catch_price",
      tot_catch_kg = "total_catch_kg",
      tot_catch_price = "total_catch_price"
    ) |>
    dplyr::relocate(
      c("catch_price", "tot_catch_kg", "tot_catch_price"),
      .after = "catch_kg"
    ) |>
    dplyr::distinct()
}


#' Transform KEFS survey data into the canonical API schema
#'
#' @param surveys_df Data frame of KEFS preprocessed or validated survey records.
#' @param conf Configuration list from [read_config()].
#' @return A tibble in the canonical API schema.
#' @noRd
format_api_kefs <- function(surveys_df, conf) {
  surveys_df |>
    dplyr::rowwise() |>
    dplyr::mutate(
      trip_id = paste0(
        "TRIP_",
        .data$submission_id
        #substr(digest::digest(.data$submission_id, algo = "xxhash64"), 1, 12)
      ),
      survey_id = conf$ingestion$kefs$koboform$asset_id_v2
    ) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      n_catch = as.integer(.data$n_sample),
      catch_outcome = dplyr::if_else(.data$catch_outcome == "yes", "1", "0")
    ) |>
    dplyr::select(
      "survey_id",
      "trip_id",
      "landing_date",
      "gaul_1_code",
      "gaul_1_name",
      "gaul_2_code",
      "gaul_2_name",
      n_fishers = "no_of_fishers",
      trip_duration_hrs = "trip_duration",
      "gear",
      "vessel_type",
      catch_habitat = "habitat",
      "catch_outcome",
      n_catch = "n_sample",
      catch_taxon = "sample_alpha3_code",
      scientific_name = "sample_scientific_name",
      "length_cm",
      catch_kg = "sample_weight",
      catch_price = "sample_price",
      tot_catch_kg = "total_catch_weight",
      tot_catch_price = "total_catch_price"
    ) |>
    dplyr::distinct()
}

#' Write a parquet file locally and upload it to cloud storage
#'
#' @param data Data frame to export.
#' @param file_prefix File prefix string (versioned filename will be derived).
#' @param cloud_path Cloud directory path.
#' @param conf Configuration list from [read_config()].
#' @return NULL invisibly.
#' @noRd
upload_api_parquet <- function(data, file_prefix, cloud_path, conf) {
  filename <- add_version(file_prefix, extension = "parquet")
  logger::log_info("Writing parquet file locally: {filename}")
  arrow::write_parquet(
    data,
    sink = filename,
    compression = "lz4",
    compression_level = 12
  )
  full_cloud_path <- file.path(cloud_path, filename)
  logger::log_info("Uploading to cloud storage: {full_cloud_path}")
  coasts::upload_cloud_file(
    file = filename,
    provider = conf$storage$google$key,
    options = conf$storage$google$options_api,
    name = full_cloud_path
  )
  file.remove(filename)
  invisible(NULL)
}


#' Export Validated API-Ready Trip Data
#'
#' @description
#' Downloads validated KEFS and WCS survey data, transforms both into the
#' canonical API schema, merges them, and uploads a single parquet file to
#' cloud storage. This is the **validated** stage of the two-stage API export
#' pipeline.
#'
#' @details
#' See [export_api_raw()] for the full output schema. This function reads from
#' the validated cloud paths and writes to
#' `conf$api$trips$validated$cloud_path`.
#'
#' @param log_threshold Logging level (default `logger::DEBUG`).
#' @return NULL invisibly. Side effect: uploads merged parquet to cloud storage.
#'
#' @keywords workflow export
#' @export
export_api_validated <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  target_form_ids <- c(
    get_airtable_form_id(
      kobo_asset_id = conf$ingestion$wcs$koboform$asset_id,
      conf = conf
    ),
    get_airtable_form_id(
      kobo_asset_id = conf$ingestion$wcs$koboform_kf$asset_id_kf,
      conf = conf
    )
  )

  # Build a single regex that matches any of the IDs
  ids_pattern <- paste0(
    "(^|,\\s*)(",
    paste(target_form_ids, collapse = "|"),
    ")(\\s*,|$)"
  )

  assets <-
    coasts::cloud_object_name(
      prefix = conf$metadata$airtable$assets,
      provider = conf$storage$google$key,
      version = "latest",
      extension = "rds",
      options = conf$storage$google$options_coasts
    ) |>
    coasts::download_cloud_file(
      provider = conf$storage$google$key,
      options = conf$storage$google$options_coasts
    ) |>
    readr::read_rds() |>
    purrr::keep_at(c("taxa", "gear", "vessels", "sites", "geo")) |>
    purrr::map(
      ~ dplyr::filter(.x, stringr::str_detect(.data$form_id, ids_pattern)) |>
        dplyr::distinct()
    )

  logger::log_info("Downloading WCS validated survey data...")
  wcs_validated <- coasts::download_parquet_from_cloud(
    prefix = conf$surveys$wcs$catch$validated$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  ) |>
    map_wcs_surveys(
      taxa_mapping = assets$taxa,
      gear_mapping = assets$gear,
      sites_mapping = assets$sites,
      geo_mapping = assets$geo
    )

  logger::log_info("Downloading WF validated survey data...")
  kefs_validated <- coasts::download_parquet_from_cloud(
    prefix = conf$surveys$kefs$v2$validated$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  logger::log_info("Transforming surveys to API format...")
  api_data <- dplyr::bind_rows(
    format_api_kefs(kefs_validated, conf),
    format_api_wcs(wcs_validated, conf)
  )

  logger::log_info(
    "Processed {nrow(api_data)} records from {length(unique(api_data$trip_id))} unique trips"
  )

  upload_api_parquet(
    data = api_data,
    file_prefix = conf$api$trips$validated$file_prefix,
    cloud_path = conf$api$trips$validated$cloud_path,
    conf = conf
  )

  logger::log_success("Validated API trip data export completed successfully")
  invisible(NULL)
}

#' Export Raw API-Ready Trip Data
#'
#' @description
#' Downloads preprocessed KEFS survey data, transforms it into the canonical
#' API schema, and uploads a parquet file to cloud storage. This is the
#' **raw/preprocessed** stage of the two-stage API export pipeline (KEFS only;
#' WCS is included at the validated stage via [export_api_validated()]).
#'
#' @details
#' **Output Schema**:
#' - `survey_id`: Kobo asset ID identifying the source survey form
#' - `trip_id`: Unique identifier (`TRIP_<submission_id>` format)
#' - `landing_date`: Date of landing
#' - `gaul_1_code`, `gaul_1_name`: GAUL level 1 region
#' - `gaul_2_code`, `gaul_2_name`: GAUL level 2 district
#' - `n_fishers`: Total fishers (men + women + children)
#' - `trip_duration_hrs`: Trip duration in hours
#' - `gear`: Standardised gear type
#' - `vessel_type`: Standardised vessel type
#' - `catch_habitat`: Habitat where catch occurred
#' - `catch_outcome`: Outcome of catch
#' - `n_catch`: Number of catch items
#' - `catch_taxon`: Species alpha-3 code
#' - `scientific_name`: Scientific name
#' - `length_cm`: Length in cm (NA for WCS surveys)
#' - `catch_kg`: Catch weight in kg
#' - `catch_price`: Individual-level price (NA — not resolved at this stage)
#' - `tot_catch_kg`: Total catch weight per trip
#' - `tot_catch_price`: Total catch price per trip
#'
#' **Cloud Storage Location**:
#' `conf$api$trips$raw$cloud_path` /
#' `{file_prefix}__{timestamp}_{git_sha}__.parquet`
#'
#' @param log_threshold Logging level (default `logger::DEBUG`).
#' @return NULL invisibly. Side effect: uploads merged parquet to cloud storage.
#'
#' @keywords workflow export
#' @export
export_api_raw <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  logger::log_info("Downloading KEFS preprocessed survey data...")
  kefs_raw <- coasts::download_parquet_from_cloud(
    prefix = conf$surveys$kefs$v2$preprocessed$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  logger::log_info("Transforming surveys to API format...")
  api_data <- format_api_kefs(kefs_raw, conf)

  logger::log_info(
    "Processed {nrow(api_data)} records from {length(unique(api_data$trip_id))} unique trips"
  )

  upload_api_parquet(
    data = api_data,
    file_prefix = conf$api$trips$raw$file_prefix,
    cloud_path = conf$api$trips$raw$cloud_path,
    conf = conf
  )

  logger::log_success("Validated API trip data export completed successfully")
  invisible(NULL)
}
