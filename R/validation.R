#' Validate Fisheries Data
#'
#' This function imports and validates preprocessed fisheries data from Google Cloud Storage.
#' It conducts a series of validation checks to ensure data integrity, including checks
#' on dates, fisher counts, boat numbers, and catch weights. The function then compiles
#' the validated data and corresponding alert flags, which are subsequently uploaded
#' back to Google Cloud Storage.
#'
#' @return No return value. Function processes the data and uploads the validated results
#' as Parquet files to Google Cloud Storage.
#'
#' @details
#' The function performs the following main operations:
#' 1. Downloads preprocessed landings data from Google Cloud Storage.
#' 2. Validates the data for consistency and accuracy, focusing on:
#'    - Date validation
#'    - Number of fishers
#'    - Number of boats
#'    - Catch weight
#' 3. Generates a validated dataset that integrates the results of the validation checks.
#' 4. Creates alert flags to identify and track any data issues discovered during validation.
#' 5. Merges the validated data with additional metadata.
#' 6. Uploads the validated dataset and alert flags as Parquet files to Google Cloud Storage.
#'
#' @note This function requires a configuration file with Google Cloud Storage credentials
#' and parameters for validation.
#'
#' @keywords workflow validation
#' @export
validate_landings <- function() {
  conf <- read_config()

  merged_landings <-
    download_parquet_from_cloud(
      prefix = conf$surveys$wcs$catch$merged$file_prefix,
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    ) |>
    dplyr::mutate(
      submission_id = paste0(.data$version, "-", .data$submission_id)
    ) |>
    dplyr::relocate("submission_id", .after = "version")

  price_tables <-
    download_parquet_from_cloud(
      prefix = conf$surveys$wcs$price$price_table$file_prefix,
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    ) |>
    dplyr::as_tibble() |>
    # price per kg cannot be zero
    dplyr::filter(!.data$median_ksh_kg == 0) |>
    impute_price() |>
    dplyr::mutate(year = lubridate::year(.data$date)) |>
    dplyr::select(-"date")

  # Spot weird observations
  gear_requires_boats <- c(
    "nets", # most net types require boats (seine nets, gillnets, etc.)
    "longline", # typically deployed from boats
    "trollingline" # requires boat movement for trolling
  )

  logical_check <-
    merged_landings |>
    dplyr::select(
      "version",
      "submission_id",
      "landing_date",
      "landing_site",
      "no_of_fishers",
      "n_boats",
      "gear",
      "total_catch_kg"
    ) |>
    dplyr::distinct() |>
    dplyr::mutate(
      alert_flag = dplyr::case_when(
        # Condition 1: No. of Fishers must be > No. of Boats, unless both are 1
        (.data$no_of_fishers < .data$n_boats |
          (.data$no_of_fishers == .data$n_boats & .data$no_of_fishers != 1)) ~
          "1",
        # Condition 2: No. of Boats must be > 0 for gear types that require boats
        .data$n_boats == 0 & .data$gear %in% gear_requires_boats ~ "2",
        # Condition 3: Total Catch cannot be negative
        .data$total_catch_kg < 0 ~ "3",
        # Condition 4: No. of Fishers or Boats must be positive integers
        .data$no_of_fishers <= 0 | .data$n_boats < 0 ~ "4",
        # Condition 5: Total Catch is zero but fishers/boats are non-zero
        .data$total_catch_kg == 0 &
          (.data$no_of_fishers > 0 | .data$n_boats > 0) ~
          "5",
        # If none of the conditions are met, it's not anomalous
        TRUE ~ NA_character_
      )
    )

  anomalous_submissions <-
    logical_check |>
    dplyr::filter(!is.na(.data$alert_flag)) |>
    dplyr::pull("submission_id") |>
    unique()

  # Remove weird submissions
  merged_landings <-
    merged_landings |>
    dplyr::filter(!.data$submission_id %in% anomalous_submissions)

  validation_output <-
    list(
      dates_alert = validate_dates(data = merged_landings, flag_value = 6),
      fishers_alert = validate_nfishers(
        data = merged_landings,
        k = conf$validation$k_nfishers,
        flag_value = 7
      ),
      nboats_alert = validate_nboats(
        data = merged_landings,
        k = conf$validation$k_nboats,
        flag_value = 8
      ),
      catch_alert = validate_catch(
        data = merged_landings,
        k = conf$validation$k_catch,
        flag_value = 9
      ),
      total_catch_alert = validate_total_catch(
        data = merged_landings,
        k = conf$validation$k_catch,
        flag_value = 10
      ),
      fishers_catch_alert = validate_fishers_catch(
        data = merged_landings,
        max_kg = conf$validation$max_kg,
        flag_value = 11
      )
    )
  # validation_output <- list(
  #  dates_alert = validate_dates(data = merged_landings, flag_value = 6),
  #  fishers_alert = validate_nfishers_iqr(data = merged_landings, flag_value = 7),
  #  nboats_alert = validate_nboats_iqr(data = merged_landings, flag_value = 8),
  #  catch_alert = validate_catch_iqr(data = merged_landings, flag_value = 9),
  #  total_catch_alert = validate_total_catch_iqr(data = merged_landings, flag_value = 10),
  #  fishers_catch_alert = validate_fishers_catch(data = merged_landings, max_kg = conf$validation$max_kg, flag_value = 11)
  # )

  validated_vars <-
    validation_output[c(
      "dates_alert",
      "fishers_alert",
      "nboats_alert",
      "catch_alert"
    )] %>%
    purrr::map(~ dplyr::select(.x, !dplyr::contains("alert"))) %>%
    purrr::reduce(dplyr::left_join, by = c("submission_id", "catch_id")) |>
    dplyr::left_join(
      validation_output$total_catch_alert,
      by = c("submission_id")
    ) |>
    dplyr::left_join(
      validation_output$fishers_catch_alert,
      by = c("submission_id")
    ) |>
    dplyr::mutate(
      total_catch_kg = dplyr::coalesce(
        .data$total_catch_kg.x,
        .data$total_catch_kg.y
      )
    ) |>
    dplyr::select(
      -c(
        "alert_catch",
        "alert_fishers_catch",
        "total_catch_kg.x",
        "total_catch_kg.y"
      )
    )

  # replace merged landings with validted variables and keep dataframe columns order
  validated_data <-
    merged_landings %>%
    dplyr::select(-c(names(validated_vars)[3:ncol(validated_vars)])) %>%
    dplyr::left_join(validated_vars, by = c("submission_id", "catch_id")) %>%
    dplyr::select(dplyr::all_of(colnames(merged_landings))) |>
    dplyr::distinct()

  # alerts data
  alert_flags <-
    validation_output[c(
      "dates_alert",
      "fishers_alert",
      "nboats_alert",
      "catch_alert"
    )] %>%
    purrr::map(
      ~ dplyr::select(.x, "submission_id", "catch_id", dplyr::contains("alert"))
    ) %>%
    purrr::reduce(dplyr::full_join, by = c("submission_id", "catch_id")) %>%
    dplyr::left_join(
      validation_output$total_catch_alert,
      by = c("submission_id")
    ) |>
    dplyr::left_join(
      validation_output$fishers_catch_alert,
      by = c("submission_id")
    ) |>
    tidyr::unite(
      col = "alert_number",
      dplyr::contains("alert"),
      sep = "-",
      na.rm = TRUE
    ) |>
    dplyr::select("submission_id", "alert_number") |>
    dplyr::distinct() |>
    dplyr::full_join(logical_check, by = c("submission_id")) |>
    dplyr::select("version", "submission_id", "alert_number", "alert_flag") |>
    tidyr::unite(
      col = "alert_number",
      dplyr::contains("alert"),
      sep = "-",
      na.rm = TRUE
    )

  clean_data <-
    validated_data |>
    dplyr::left_join(alert_flags, by = c("version", "submission_id")) |>
    dplyr::filter(.data$alert_number == "") |>
    dplyr::select(-"alert_number") |>
    # Add catch prices
    dplyr::mutate(year = lubridate::year(.data$landing_date)) |>
    dplyr::left_join(
      price_tables,
      by = c("year", "landing_site", "fish_category", "size")
    ) |>
    dplyr::mutate(catch_price = .data$median_ksh_kg_imputed * .data$catch_kg) |>
    dplyr::group_by(.data$submission_id) |>
    dplyr::mutate(
      total_catch_price = sum(.data$catch_price)
    ) |>
    dplyr::ungroup() |>
    dplyr::select(-c("year", "median_ksh_kg_imputed")) |>
    dplyr::distinct()

  # Define the data and their corresponding collection names
  upload_data <- list(
    list(
      data = clean_data,
      prefix = conf$surveys$wcs$catch$validated$file_prefix
    ),
    list(data = alert_flags, prefix = conf$surveys$wcs$flags$file_prefix)
  )
  # Log messages for each upload
  log_messages <- c(
    "Uploading validated data to google cloud storage",
    "Uploading validation flags data to google cloud storage"
  )
  # Walk through both the data and the log messages
  purrr::walk2(
    upload_data,
    log_messages,
    ~ {
      logger::log_info(.y) # Log the current message
      upload_parquet_to_cloud(
        data = .x$data,
        provider = conf$storage$google$key,
        prefix = .x$prefix,
        options = conf$storage$google$options
      )
    }
  )
}


#' Validate KEFS Surveys Data (Version 2)
#'
#' This function imports and validates preprocessed KEFS (Kenya Fisheries) survey data
#' from Google Cloud Storage. It performs validation checks on trip characteristics,
#' catch data, and derived indicators (CPUE, RPUE, price per kg). The function queries
#' KoboToolbox for manual validation status and respects human-reviewed approvals while
#' generating automated validation flags for data quality issues.
#'
#' @return No return value. Function processes the data and uploads the validated results
#' and alert flags as Parquet files to Google Cloud Storage.
#'
#' @details
#' The function performs the following main operations:
#' 1. Downloads preprocessed KEFS survey data from Google Cloud Storage.
#' 2. Sets up parallel processing with rate limiting (max 4 workers, 200ms delay) to avoid overwhelming the API server.
#' 3. Queries KoboToolbox API to retrieve existing validation statuses for all submissions.
#' 4. Identifies manually approved submissions (excluding system approvals) to preserve human review decisions.
#' 5. Validates the data across multiple dimensions:
#'    - Information flags: Missing catch outcome and weight data (flag 1.1)
#'    - Trip flags: Horse power, number of fishers, trip duration, and revenue anomalies (flags 1-4)
#'    - Catch flags: Sample weight inconsistencies (flags 5.1-5.2)
#'    - Indicator flags: CPUE, RPUE, and price per kg outliers (flags 6.1-6.3)
#' 6. Combines all validation flags into a comprehensive alert system.
#' 7. Clears validation flags for manually approved submissions (respecting human decisions).
#' 8. Filters valid data based on presence of alert flags.
#' 9. Uploads both the validation flags and validated dataset as Parquet files to Google Cloud Storage.
#'
#' @section Validation Limits:
#' The function uses the following default limits for trip validation:
#' \itemize{
#'   \item max_hp: 150 (maximum horse power)
#'   \item max_n_fishers: 100 (maximum number of fishers)
#'   \item max_trip_duration: 96 hours (maximum trip duration)
#'   \item max_revenue: 387,600 KSH (approximately 3,000 USD)
#' }
#'
#' And for indicator validation:
#' \itemize{
#'   \item max_cpue: 20 kg/fisher/hour (maximum catch per unit effort)
#'   \item max_rpue: 3,876 KSH/fisher/hour (approximately 30 USD/fisher/hour)
#'   \item max_price_kg: 3,876 KSH/kg (approximately 30 USD/kg)
#' }
#'
#' @note
#' This function requires:
#' - A configuration file with Google Cloud Storage credentials and KoboToolbox API credentials
#' - The `future` package configured for parallel processing
#' - The preprocessed KEFS surveys data to be available in Google Cloud Storage
#'
#' @keywords workflow validation
#' @export
validate_kefs_surveys_v2 <- function() {
  conf <- read_config()

  preprocessed_surveys <-
    download_parquet_from_cloud(
      prefix = conf$surveys$kefs$v2$preprocessed$file_prefix,
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    )

  # Set up limited parallel processing with rate limiting for kf.fimskenya.co.ke
  # Max 4 workers to avoid overwhelming the server
  future::plan(
    strategy = future::multisession,
  )

  # Get validation status from KoboToolbox for existing submissions
  submission_ids <- unique(preprocessed_surveys$submission_id)

  # Query validation status from kefs asset
  logger::log_info(
    "Querying validation status from kefs asset for {length(submission_ids)} submissions"
  )

  # Enable progress reporting
  progressr::handlers(progressr::handler_progress(
    format = "[:bar] :current/:total (:percent) eta: :eta"
  ))

  validation_results <- progressr::with_progress({
    p <- progressr::progressor(along = submission_ids)

    submission_ids %>%
      furrr::future_map_dfr(
        function(id) {
          result <- get_validation_status(
            submission_id = id,
            asset_id = conf$ingestion$kefs$koboform$asset_id_v2,
            token = conf$ingestion$kefs$koboform$token
          )
          p()
          result
        },
        .options = furrr::furrr_options(seed = TRUE)
      )
  })

  # Extract manually approved IDs (exclude system-approved)
  # Only human-reviewed approvals should override automatic validation flags
  manual_approved_ids <- validation_results %>%
    dplyr::filter(
      .data$validation_status == "validation_status_approved" &
        !is.na(.data$validated_by) &
        .data$validated_by != "" &
        .data$validated_by != conf$ingestion$kefs$koboform$username # Exclude system approvals
    ) %>%
    dplyr::pull(.data$submission_id) %>%
    unique()

  logger::log_info(
    "Found {length(manual_approved_ids)} manually approved submissions in KoboToolbox"
  )

  info_flags <-
    preprocessed_surveys |>
    dplyr::mutate(
      alert_info = dplyr::case_when(
        is.na(.data$catch_outcome) & is.na(.data$total_catch_weight) ~ "1.1",
        TRUE ~ NA_character_
      )
    ) |>
    dplyr::select(
      "submission_id",
      dplyr::starts_with("alert_")
    ) |>
    dplyr::distinct()

  preprocessed_surveys <-
    preprocessed_surveys |>
    dplyr::filter(
      !.data$submission_id %in%
        info_flags$submission_id[!is.na(info_flags$alert_info)]
    )

  trip_limits <-
    list(
      max_hp = 150,
      max_n_fishers = 100,
      max_trip_duration = 96,
      max_revenue = 387600
    )

  trip_flags <- get_trips_flags(
    dat = preprocessed_surveys,
    limits = trip_limits
  )
  catch_flags <- get_catch_flags(dat = preprocessed_surveys)

  no_flags_ids <-
    dplyr::full_join(trip_flags, catch_flags, by = "submission_id") |>
    dplyr::filter(
      is.na(.data$alert_flag_trip) & is.na(.data$alert_flag_catch)
    ) |>
    dplyr::select("submission_id") |>
    dplyr::pull(.data$submission_id) |>
    unique()

  indicator_limits <-
    list(
      max_cpue = 20, # max catch per unit effort (fisher/hours) 20 kg/hour
      max_rpue = 3876, # max revenue per unit effort (fisher/hours) 30 USD/hour
      max_price_kg = 3876 # max price per kg 30 USD/kg
    )

  composite_flags <- get_indicators_flags(
    dat = preprocessed_surveys,
    limits = indicator_limits,
    clean_ids = no_flags_ids
  )

  flags_combined <-
    dplyr::full_join(trip_flags, catch_flags, by = "submission_id") |>
    dplyr::full_join(composite_flags, by = "submission_id") |>
    dplyr::full_join(info_flags, by = "submission_id") |>
    tidyr::unite(
      col = "alert_flag",
      "alert_flag_trip",
      "alert_flag_catch",
      "alert_flag_indicators",
      "alert_info",
      sep = ",",
      na.rm = TRUE
    ) |>
    dplyr::mutate(alert_flag = dplyr::na_if(.data$alert_flag, "")) |>
    # Respect manual approvals: clear flags for manually approved submissions
    dplyr::mutate(
      alert_flag = dplyr::if_else(
        .data$submission_id %in% manual_approved_ids,
        NA_character_,
        .data$alert_flag
      )
    ) |>
    dplyr::left_join(
      preprocessed_surveys |>
        dplyr::select(
          "submission_id",
          "submission_date",
          submitted_by = "enumerator_name_clean"
        )
    ) |>
    dplyr::distinct()

  # Log how many manually approved submissions had flags cleared
  cleared_flags <- flags_combined %>%
    dplyr::filter(.data$submission_id %in% manual_approved_ids) %>%
    nrow()

  if (cleared_flags > 0) {
    logger::log_info(
      "Cleared validation flags for {cleared_flags} manually approved submissions"
    )
  }

  valid_data <-
    preprocessed_surveys |>
    dplyr::semi_join(
      flags_combined |> dplyr::filter(is.na(.data$alert_flag)),
      by = "submission_id"
    ) |>
    dplyr::distinct()

  upload_parquet_to_cloud(
    data = flags_combined,
    prefix = conf$surveys$kefs$v2$validation$flags$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  upload_parquet_to_cloud(
    data = valid_data,
    prefix = conf$surveys$kefs$v2$validated$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  invisible(NULL)
}

#' Synchronize Validation Statuses with KoboToolbox
#'
#' @description
#' Synchronizes validation statuses between the local system and KoboToolbox by processing
#' validation flags and updating submission statuses accordingly. This function respects
#' manual human approvals and handles both flagged and clean submissions in parallel with rate limiting.
#'
#' @details
#' The function follows these steps:
#' 1. Downloads the current validation flags from cloud storage
#' 2. Sets up parallel processing using the future package (max 4 workers with rate limiting)
#' 3. Fetches current validation status from KoboToolbox to identify manual approvals
#' 4. Identifies manually approved submissions and preserves them (excludes system approvals)
#' 5. Processes flagged submissions (marking as not approved), EXCLUDING manually approved ones
#' 6. Processes clean submissions (marking as approved)
#' 7. Combines validation statuses from updates and preserved manual approvals
#' 8. Adds KoboToolbox validation status to validation flags
#' 9. Pushes all validation flags with KoboToolbox status to MongoDB for record-keeping
#'
#' Progress reporting is enabled to track the status of submissions being processed.
#'
#' @param log_threshold The logging level threshold for the logger package (e.g., DEBUG, INFO).
#'        Default is logger::DEBUG.
#'
#' @return None. The function performs status updates and database operations as side effects.
#'
#' @section Manual Approval Preservation:
#' The function identifies submissions that have been manually approved by humans (validated_by
#' is not empty and is not the system username) and preserves these approvals even if automated
#' validation would flag them. Flagged submissions that were manually approved will NOT be
#' marked as "not approved" by this function. This ensures human review decisions are always
#' respected and never overwritten by the automated system.
#'
#' @section Rate Limiting:
#' To avoid overwhelming the KoboToolbox API server (kf.fimskenya.co.ke), the function limits
#' parallel workers to 4 and adds a 200ms delay between requests. This provides approximately
#' 20 requests per second across all workers while maintaining server stability. With this
#' configuration, processing 13,000 submissions takes approximately 2-3 hours (including the
#' initial status fetch to identify manual approvals).
#'
#' @note
#' This function requires proper configuration in the config file, including:
#' - MongoDB connection parameters
#' - KoboToolbox asset ID and token (configured under ingestion$kefs$koboform)
#' - Google cloud storage parameters
#'
#' @examples
#' \dontrun{
#' # Run with default DEBUG logging
#' sync_validation_submissions()
#'
#' # Run with INFO level logging
#' sync_validation_submissions(log_threshold = logger::INFO)
#' }
#'
#' @keywords workflow validation
#' @export
sync_validation_submissions <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  conf <- read_config()

  # Download validation flags
  validation_flags <-
    download_parquet_from_cloud(
      prefix = conf$surveys$kefs$v2$validation$flags$file_prefix,
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    )

  # Set up limited parallel processing with rate limiting for kf.fimskenya.co.ke
  # Max 4 workers to avoid overwhelming the server
  future::plan(
    strategy = future::multisession,
    workers = future::availableCores() - 2
  )

  # Enable progress reporting globally
  progressr::handlers(progressr::handler_progress(
    format = "[:bar] :current/:total (:percent) eta: :eta"
  ))

  # 1. Fetch current validation status to identify manual approvals
  all_submission_ids <- unique(validation_flags$submission_id)

  logger::log_info(
    "Fetching current validation status for {length(all_submission_ids)} submissions to identify manual approvals"
  )

  current_kobo_status <- process_submissions_parallel(
    submission_ids = all_submission_ids,
    process_fn = function(id) {
      get_validation_status(
        submission_id = id,
        asset_id = conf$ingestion$kefs$koboform$asset_id_v2,
        token = conf$ingestion$kefs$koboform$token
      )
    },
    description = "current validation statuses",
    rate_limit = 0.1
  )

  # 2. Identify manually approved submissions (preserve human decisions)
  manual_approved_ids <- current_kobo_status %>%
    dplyr::filter(
      .data$validation_status == "validation_status_approved" &
        !is.na(.data$validated_by) &
        .data$validated_by != "" &
        .data$validated_by != conf$ingestion$kefs$koboform$username
    ) %>%
    dplyr::pull(.data$submission_id)

  if (length(manual_approved_ids) > 0) {
    logger::log_info(
      "Found {length(manual_approved_ids)} manually approved submissions - these will be preserved"
    )
  }

  # 3. Process submissions with alert flags (mark as not approved)
  # EXCLUDE manually approved submissions
  flagged_submissions <- validation_flags %>%
    dplyr::filter(!is.na(.data$alert_flag)) %>%
    dplyr::pull(.data$submission_id) %>%
    unique() %>%
    setdiff(manual_approved_ids)

  flagged_results <- if (length(flagged_submissions) > 0) {
    process_submissions_parallel(
      submission_ids = flagged_submissions,
      process_fn = function(id) {
        update_validation_status(
          submission_id = id,
          status = "validation_status_not_approved",
          asset_id = conf$ingestion$kefs$koboform$asset_id_v2,
          token = conf$ingestion$kefs$koboform$token
        )
      },
      description = "flagged submissions",
      rate_limit = 0.1
    )
  } else {
    logger::log_info(
      "No flagged submissions to update (excluding manual approvals)"
    )
    dplyr::tibble()
  }

  # 4. Process submissions without alert flags (mark as approved)
  # Skip submissions that are already approved to avoid redundant API calls
  clean_submissions <- validation_flags %>%
    dplyr::filter(is.na(.data$alert_flag)) %>%
    dplyr::pull(.data$submission_id) %>%
    unique()

  # Filter out submissions that are already approved
  clean_to_update <- clean_submissions %>%
    setdiff(
      current_kobo_status %>%
        dplyr::filter(
          .data$validation_status == "validation_status_approved"
        ) %>%
        dplyr::pull(.data$submission_id)
    )

  clean_results <- if (length(clean_to_update) > 0) {
    process_submissions_parallel(
      submission_ids = clean_to_update,
      process_fn = function(id) {
        update_validation_status(
          submission_id = id,
          status = "validation_status_approved",
          asset_id = conf$ingestion$kefs$koboform$asset_id_v2,
          token = conf$ingestion$kefs$koboform$token
        )
      },
      description = "clean submissions",
      rate_limit = 0.2
    )
  } else {
    logger::log_info(
      "No clean submissions need updating (all already approved)"
    )
    dplyr::tibble()
  }

  # For submissions we didn't update (already approved), get their current status
  already_approved_clean <- current_kobo_status %>%
    dplyr::filter(
      .data$submission_id %in%
        clean_submissions &
        .data$validation_status == "validation_status_approved" &
        !.data$submission_id %in% manual_approved_ids # Don't duplicate manual approvals
    ) %>%
    dplyr::select(
      "submission_id",
      "validation_status",
      "validated_at",
      "validated_by"
    )

  # 5. Combine validation statuses from all sources
  logger::log_info("Combining validation status results")

  # Combine all status sources:
  # - Updated flagged submissions (marked as not approved)
  # - Updated clean submissions (marked as approved)
  # - Manually approved submissions (preserved)
  # - Already approved clean submissions (skipped updates)
  current_kobo_status <- dplyr::bind_rows(
    flagged_results %>%
      dplyr::select(
        "submission_id",
        "validation_status",
        "validated_at",
        "validated_by"
      ),
    clean_results %>%
      dplyr::select(
        "submission_id",
        "validation_status",
        "validated_at",
        "validated_by"
      ),
    current_kobo_status %>%
      dplyr::filter(.data$submission_id %in% manual_approved_ids) %>%
      dplyr::select(
        "submission_id",
        "validation_status",
        "validated_at",
        "validated_by"
      ),
    already_approved_clean
  )

  # Add current KoboToolbox validation status to validation_flags
  validation_flags_with_kobo_status <-
    validation_flags %>%
    dplyr::left_join(
      current_kobo_status,
      by = "submission_id",
      suffix = c("", "_kobo")
    )

  # Create long format for enumerators statistics
  validation_flags_long <-
    validation_flags_with_kobo_status |>
    dplyr::mutate(alert_flag = as.character(.data$alert_flag)) %>%
    tidyr::separate_rows("alert_flag", sep = ",\\s*") |>
    dplyr::select(-c(dplyr::starts_with("valid")))

  asset_id <- conf$ingestion$kefs$koboform$asset_id_v2
  # Push the validation flags with KoboToolbox status to MongoDB
  mdb_collection_push(
    data = validation_flags_with_kobo_status,
    connection_string = conf$storage$mongodb$cluster$validation$connection_string,
    db_name = conf$storage$mongodb$cluster$validation$database,
    collection_name = paste(
      conf$storage$mongodb$cluster$validation$collection$flags,
      asset_id,
      sep = "-"
    )
  )
  # Push enumerators statistics to MongoDB
  mdb_collection_push(
    data = validation_flags_long,
    connection_string = conf$storage$mongodb$cluster$validation$connection_string,
    db_name = conf$storage$mongodb$cluster$validation$database,
    collection_name = paste(
      conf$storage$mongodb$cluster$validation$collection$enumerators_stats,
      asset_id,
      sep = "-"
    )
  )

  logger::log_info("Validation synchronization completed successfully")
}


#' Process Submissions with Rate-Limited Parallel API Calls
#'
#' @description
#' Helper function to process multiple submissions in parallel with rate limiting,
#' progress tracking, and error logging.
#'
#' @param submission_ids Character vector of submission IDs to process
#' @param process_fn Function to apply to each submission ID
#' @param description Character string describing what's being processed (for logging)
#' @param rate_limit Numeric delay in seconds between requests (default: 0.2)
#'
#' @return Data frame with results from process_fn for all submissions
#'
#' @keywords internal
process_submissions_parallel <- function(
  submission_ids,
  process_fn,
  description = "submissions",
  rate_limit = 0.2
) {
  logger::log_info("Processing {length(submission_ids)} {description}")

  progressr::with_progress({
    p <- progressr::progressor(along = submission_ids)

    results <- furrr::future_map_dfr(
      submission_ids,
      function(id) {
        # Add delay to respect rate limits
        Sys.sleep(rate_limit)

        result <- process_fn(id)
        p()
        result
      },
      .options = furrr::furrr_options(seed = TRUE)
    )

    # Log failures if update_success column exists
    if ("update_success" %in% names(results)) {
      failures <- results %>% dplyr::filter(!.data$update_success)
      if (nrow(failures) > 0) {
        logger::log_warn(
          "Failed to update {nrow(failures)} {description}: {paste(failures$submission_id, collapse = ', ')}"
        )
      } else {
        logger::log_info(
          "Successfully processed all {length(submission_ids)} {description}"
        )
      }
    }

    results
  })
}
