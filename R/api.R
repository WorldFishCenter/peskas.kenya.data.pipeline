# ── Internal format helpers ────────────────────────────────────────────────────

#' Transform WCS survey data into the canonical API schema
#'
#' @param surveys_df Data frame of WF preprocessed or validated survey records.
#' @param conf Configuration list from [read_config()].
#' @return A tibble in the canonical API schema.
#'
#' @section Trip duration:
#' `trip_duration_hrs` is published as `NA_real_`. No WCS form -- legacy, v1 or
#' v2 -- collects trip duration; the only time fields on the Kobo forms are the
#' `start`/`end` stamps of the enumerator's form session, which are unrelated to
#' time at sea. This column previously carried a hardcoded `24`, which was
#' indistinguishable downstream from a measured value and made any WCS
#' catch-per-hour figure meaningless. KEFS, which does record
#' `fishing_trip_start`/`fishing_trip_end`, still publishes a measured duration.
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
      # The WCS tables carry landing_date as POSIXct, KEFS as Date, and
      # bind_rows() promotes the pair to POSIXct -- which is what made Kenya
      # the only country publishing a timestamp where the schema says date.
      # Every WCS row is midnight, so the cast drops no information.
      landing_date = lubridate::as_date(.data$landing_date),
      # Not collected by any WCS form; NA rather than a fabricated constant.
      trip_duration_hrs = NA_real_,
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
      "landing_site",
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
#'
#' @section Sampled composition:
#' KEFS is a sampled-composition survey, unlike WCS. The form weighs the whole
#' catch (`TotalCatchWeight`, valued as `TValue`) and separately identifies the
#' species composition of a *sample* (`OverallSampleWeight`, reshaped to
#' `sample_weight`/`sample_price` by [reshape_overall_sample()]). The catch rows
#' published here are that sample: they sum to `total_sample_weight`, not to
#' `tot_catch_kg`, so `tot_catch_kg == sum(catch_kg)` does not hold for KEFS and
#' must not be forced. Roughly half of KEFS trips sample the entire catch, in
#' which case the two do coincide. `tot_catch_price` is likewise a whole-catch
#' valuation, `total_catch_weight * total_price_kg`, not a sum over catch rows.
#'
#' @section Length:
#' The shared API schema carries one `length_cm` per catch row, but KEFS records
#' lengths per individual fish -- `PrioritySpeciesCatch` is a length-frequency
#' subsample nested inside the composition, and a trip can measure several
#' species. [summarise_priority_lengths()] collapses those individuals onto the
#' species they belong to during preprocessing, so `length_cm` arrives here as
#' the mean length of the fish measured for that catch row. Because Kenya
#' records one row per fish rather than per length bin, that plain mean is the
#' same individual-weighted mean the Timor pipeline publishes for this schema.
#' A catch row whose species was not measured carries `NA`.
#'
#' @section Catch price units:
#' `sample_price` is the KSH/kg rate for the species, not a value: it equals the
#' form's trip-level `PricePerKg` in 95% of single-species trips, is a multiple
#' of 50 KSH in 84% of rows, and is uncorrelated with `sample_weight`. The API
#' schema defines `catch_price` as a value in the same units as
#' `tot_catch_price`, so the rate is multiplied by the weight here, matching how
#' [format_api_wcs()] derives `catch_price` from a median KSH/kg.
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
      n_sample = as.integer(.data$n_sample),
      catch_outcome = dplyr::if_else(.data$catch_outcome == "yes", "1", "0"),
      # sample_price is a KSH/kg rate, catch_price is a value; multiply through.
      catch_price = .data$sample_weight * .data$sample_price
    ) |>
    dplyr::select(
      "survey_id",
      "trip_id",
      "landing_date",
      "gaul_1_code",
      "gaul_1_name",
      "gaul_2_code",
      "gaul_2_name",
      "landing_site",

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
      "catch_price",
      tot_catch_kg = "total_catch_weight",
      tot_catch_price = "total_catch_price"
    ) |>
    dplyr::distinct()
}

#' Download the Airtable asset mappings used to map WCS surveys
#'
#' Both API export stages map WCS surveys with the same taxa/gear/site/geo
#' lookups, filtered to the two WCS Kobo forms.
#'
#' @param conf Configuration list from [read_config()].
#' @return A named list of mapping tibbles.
#' @noRd
read_wcs_api_assets <- function(conf) {
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
      ~ dplyr::filter(.x, stringr::str_detect(.data$form_id, ids_pattern))
    ) |>
    purrr::map(
      ~ dplyr::select(.x, -dplyr::any_of(c("country", "latitude", "longitude")))
    )
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

  assets <- read_wcs_api_assets(conf)

  logger::log_info("Downloading WCS validated survey data...")
  # WCS artifacts live in the dedicated peskas-wcs bucket; KEFS stays in the
  # shared Kenya bucket. The combined output below is mixed, so it goes to
  # options_api and must never be written back to the WCS bucket.
  wcs_validated <- coasts::download_parquet_from_cloud(
    prefix = conf$surveys$wcs$catch$validated$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options_wcs
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
#' Downloads preprocessed KEFS and WCS survey data, transforms both into the
#' canonical API schema, and uploads a parquet file to cloud storage. This is
#' the **raw/preprocessed** stage of the two-stage API export pipeline.
#'
#' @details
#' Both stages cover the same two sources, so the validated export is a strict
#' subset of this one and the API's `status` parameter selects two processing
#' stages of one population rather than two different populations. The WCS
#' preprocessed stage is `conf$surveys$wcs$catch$merged$file_prefix`, the
#' merged landings written by [merge_landings()]: the legacy, v1 and v2 sources
#' bound together before [validate_landings()] drops alerting trips and joins
#' prices. Trips present here but absent from the validated export are those
#' validation rejected.
#'
#' **Output Schema**:
#' - `survey_id`: Kobo asset ID identifying the source survey form
#' - `trip_id`: Unique identifier (`TRIP_<submission_id>` format)
#' - `landing_date`: Date of landing
#' - `gaul_1_code`, `gaul_1_name`: GAUL level 1 region
#' - `gaul_2_code`, `gaul_2_name`: GAUL level 2 district
#' - `landing_site`, `landing_site`: Landing site name
#' - `n_fishers`: Total fishers (men + women + children)
#' - `trip_duration_hrs`: Trip duration in hours (NA for WCS — not collected by
#'   any WCS form)
#' - `gear`: Standardised gear type
#' - `vessel_type`: Standardised vessel type
#' - `catch_habitat`: Habitat where catch occurred
#' - `catch_outcome`: Outcome of catch
#' - `n_catch`: Number of catch items
#' - `catch_taxon`: Species alpha-3 code
#' - `scientific_name`: Scientific name
#' - `length_cm`: Length in cm (NA for WCS surveys)
#' - `catch_kg`: Catch weight in kg
#' - `catch_price`: Individual-level price (NA for WCS — prices are resolved
#'   only at the validated stage)
#' - `tot_catch_kg`: Total catch weight per trip
#' - `tot_catch_price`: Total catch price per trip (NA for WCS, as above)
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

  assets <- read_wcs_api_assets(conf)

  logger::log_info("Downloading KEFS preprocessed survey data...")
  kefs_raw <- coasts::download_parquet_from_cloud(
    prefix = conf$surveys$kefs$v2$preprocessed$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  logger::log_info("Downloading WCS merged landings...")
  # The merged landings are the WCS preprocessed stage: the three source
  # versions bound together, before validate_landings() drops alerting trips
  # and attaches prices. Prices are resolved only at the validated stage, so
  # both price columns are NA here, matching the KEFS raw catch_price.
  wcs_raw <- coasts::download_parquet_from_cloud(
    prefix = conf$surveys$wcs$catch$merged$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options_wcs
  ) |>
    dplyr::mutate(
      submission_id = paste0(.data$version, "-", .data$submission_id),
      catch_price = NA_real_,
      total_catch_price = NA_real_
    ) |>
    map_wcs_surveys(
      taxa_mapping = assets$taxa,
      gear_mapping = assets$gear,
      sites_mapping = assets$sites,
      geo_mapping = assets$geo
    )

  logger::log_info("Transforming surveys to API format...")
  kefs_api <- format_api_kefs(kefs_raw, conf)
  wcs_api <- format_api_wcs(wcs_raw, conf)
  api_data <- dplyr::bind_rows(kefs_api, wcs_api)

  logger::log_info(
    "KEFS {nrow(kefs_api)} records / {dplyr::n_distinct(kefs_api$trip_id)} trips; WCS {nrow(wcs_api)} records / {dplyr::n_distinct(wcs_api$trip_id)} trips"
  )
  logger::log_info(
    "Processed {nrow(api_data)} records from {length(unique(api_data$trip_id))} unique trips"
  )

  upload_api_parquet(
    data = api_data,
    file_prefix = conf$api$trips$raw$file_prefix,
    cloud_path = conf$api$trips$raw$cloud_path,
    conf = conf
  )

  logger::log_success("Raw API trip data export completed successfully")
  invisible(NULL)
}
