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
#' @keywords workflow validation wcs
#' @export
validate_landings <- function() {
  conf <- read_config()

  merged_landings <-
    coasts::download_parquet_from_cloud(
      prefix = conf$surveys$wcs$catch$merged$file_prefix,
      provider = conf$storage$google$key,
      options = conf$storage$google$options_wcs
    ) |>
    dplyr::mutate(
      submission_id = paste0(.data$version, "-", .data$submission_id)
    ) |>
    dplyr::relocate("submission_id", .after = "version")

  price_tables <-
    coasts::download_parquet_from_cloud(
      prefix = conf$surveys$wcs$price$price_table$file_prefix,
      provider = conf$storage$google$key,
      options = conf$storage$google$options_wcs
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
        k = 3.5,
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

  priced_landings <-
    validated_data |>
    dplyr::left_join(alert_flags, by = c("version", "submission_id")) |>
    dplyr::filter(.data$alert_number == "") |>
    dplyr::select(-"alert_number") |>
    # Add catch prices
    dplyr::mutate(year = lubridate::year(.data$landing_date))

  clean_data <-
    priced_landings |>
    # One price row per key, enforced here so a duplicated price table fails
    # loudly instead of silently duplicating every catch row it matches.
    # total_catch_price below is summed over these rows, so a fan-out would
    # over-state trip revenue by the same factor.
    dplyr::left_join(
      price_tables,
      by = c("year", "landing_site", "fish_category", "size"),
      relationship = "many-to-one"
    ) |>
    dplyr::mutate(catch_price = .data$median_ksh_kg_imputed * .data$catch_kg) |>
    dplyr::group_by(.data$submission_id) |>
    dplyr::mutate(
      total_catch_price = sum(.data$catch_price)
    ) |>
    dplyr::ungroup() |>
    dplyr::select(-c("year", "median_ksh_kg_imputed")) |>
    dplyr::distinct()

  logger::log_info(
    "Price join: {nrow(priced_landings)} catch rows -> {nrow(clean_data)} rows across {dplyr::n_distinct(clean_data$submission_id)} trips"
  )

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
      coasts::upload_parquet_to_cloud(
        data = .x$data,
        provider = conf$storage$google$key,
        prefix = .x$prefix,
        options = conf$storage$google$options_wcs
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
#' 2. Reads reviewers' decisions with [coasts::review_decisions()]: the flags
#'    collection the Peskas Management Platform writes to, and every
#'    submission's status on KEFS's KoboToolbox server.
#' 3. Validates the data across multiple dimensions:
#'    - Information flags: Missing catch outcome and weight data (flag 1.1)
#'    - Trip flags: Horse power, number of fishers, trip duration, and revenue anomalies (flags 1-4)
#'    - Catch flags: Sample weight inconsistencies (flags 5.1-5.2)
#'    - Indicator flags: CPUE, RPUE, and price per kg outliers (flags 6.1-6.3)
#' 4. Combines all validation flags into a comprehensive alert system.
#' 5. Keeps the submissions with no flag, and those a reviewer approved.
#' 6. Uploads the validated dataset, and pushes the flags with the reviewers'
#'    decisions to MongoDB with [export_validation_flags()].
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
#' - The preprocessed KEFS surveys data to be available in Google Cloud Storage
#'
#' @keywords workflow validation
#' @export
validate_kefs_surveys_v2 <- function() {
  conf <- read_config()

  preprocessed_surveys <-
    coasts::download_parquet_from_cloud(
      prefix = conf$surveys$kefs$v2$preprocessed$file_prefix,
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    )

  # Reviewers' decisions, read before this run rewrites the flags collection.
  # KEFS surveys live on KEFS's own KoBoToolbox server, with the credentials
  # ingestion uses there.
  kefs <- conf$ingestion$kefs$koboform
  validation_statuses <-
    coasts::review_decisions(
      flags = coasts::mdb_collection_pull(
        connection_string = conf$storage$mongodb$connection_strings$validation,
        db_name = conf$storage$mongodb$databases$validation$database_name,
        collection_name = paste(
          conf$storage$mongodb$databases$validation$collections$flags,
          kefs$asset_id_v2,
          sep = "-"
        )
      ),
      pipeline_users = kefs$username,
      asset_id = kefs$asset_id_v2,
      username = kefs$username,
      password = kefs$password,
      url = "kf.fims.kefs.go.ke"
    ) |>
    dplyr::mutate(submission_id = as.character(.data$submission_id))
  approved_ids <- validation_statuses$submission_id[
    validation_statuses$validation_status == "validation_status_approved"
  ]
  rejected_ids <- validation_statuses$submission_id[
    validation_statuses$validation_status == "validation_status_not_approved"
  ]

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
    dplyr::left_join(
      preprocessed_surveys |>
        dplyr::select(
          "submission_id",
          "submission_date",
          submitted_by = "enumerator_name_clean"
        )
    ) |>
    dplyr::distinct()

  # A reviewer's decision outranks the automatic flags, either way.
  valid_data <-
    preprocessed_surveys |>
    dplyr::semi_join(
      flags_combined |>
        dplyr::filter(
          is.na(.data$alert_flag) | .data$submission_id %in% approved_ids,
          !.data$submission_id %in% rejected_ids
        ),
      by = "submission_id"
    ) |>
    dplyr::distinct()

  coasts::upload_parquet_to_cloud(
    data = valid_data,
    prefix = conf$surveys$kefs$v2$validated$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )
  export_validation_flags(
    conf = conf,
    asset_id = "v2",
    all_flags = flags_combined,
    validation_statuses = validation_statuses
  )

  invisible(NULL)
}


#' Export Validation Flags to MongoDB
#'
#' @description
#' Exports validation flags to MongoDB, keeping the decisions reviewers made in
#' the Peskas Management Platform or in KoboToolbox (read beforehand with
#' `coasts::review_decisions()`).
#'
#' @details
#' The function performs the following steps:
#' \enumerate{
#'   \item Joins validation flags with KoboToolbox validation statuses
#'   \item Identifies manual human approvals (excluding system username)
#'   \item Preserves manual human decisions while updating system-generated statuses
#'   \item Creates both wide and long format datasets for different reporting needs
#'   \item Pushes results directly to MongoDB collections
#' }
#'
#' \strong{Validation Status Logic:}
#' \itemize{
#'   \item If submission has flags AND validated_by is system username: set to "not_approved"
#'   \item If submission has no flags AND validated_by is system username: set to "approved"
#'   \item If validated_by is NOT system username: preserve existing status (a reviewer's approval or rejection)
#' }
#'
#' @param conf Configuration object from `read_config()` containing MongoDB connection
#'   parameters and survey-specific settings
#' @param asset_id Character string specifying which survey to process. Must be one of
#'   "v1" or "v2". Determines which configuration to use from
#'   `conf$ingestion$kobo-{asset_id}`. Default is "adnap".
#' @param all_flags Data frame containing all validation flags with columns:
#'   `submission_id`, `submitted_by`, `submission_date`, `alert_flag`
#' @param validation_statuses Reviewers' decisions from `coasts::review_decisions()`,
#'   with columns `submission_id`, `validation_status`, `validated_at`, `validated_by`
#'
#' @return Invisible NULL. The function pushes data to MongoDB as a side effect.
#'
#' @section MongoDB Collections:
#' The function pushes to two MongoDB collections:
#' \describe{
#'   \item{flags-{asset_id}}{Wide format with one row per submission including
#'     validation status and flags}
#'   \item{enumerators_stats-{asset_id}}{Long format with one row per flag per
#'     submission for enumerator statistics}
#' }
#'
#' @note
#' This function is called internally by `validate_kefs_surveys_v2()` and should not
#' typically be called directly. It requires:
#' \itemize{
#'   \item Valid configuration with MongoDB connection string
#'   \item Survey-specific configuration under `conf$ingestion$kobo-{asset_id}`
#'   \item System username configured to identify automated vs. manual validations
#' }
#'
#' @examples
#' \dontrun{
#' # Called internally by validate_kefs_surveys_v2()
#' export_validation_flags(
#'   conf = conf,
#'   asset_id = "v1",
#'   all_flags = flags_combined,
#'   validation_statuses = validation_statuses
#' )
#' }
#'
#' @seealso
#' \itemize{
#'   \item \code{\link[=validate_kefs_surveys_v2]{validate_kefs_surveys_v2()}} for the main validation workflow
#'   \item \code{\link[=mdb_collection_push]{coasts::mdb_collection_push()}} for MongoDB operations
#' }
#'
#' @keywords validation workflow
#' @export
export_validation_flags <- function(
  conf = NULL,
  asset_id = c("v1", "v2"),
  all_flags = NULL,
  validation_statuses = NULL
) {
  asset_id <- match.arg(asset_id)
  config_key <- paste0("asset_id_", asset_id)

  # Get the survey-specific config
  survey_conf <- conf$ingestion$kefs$koboform

  validation_flags_with_kobo_status <-
    all_flags |>
    dplyr::full_join(validation_statuses, by = "submission_id") |>
    dplyr::mutate(
      # The pipeline signs an unflagged submission, unless a reviewer decided it.
      validated_by = dplyr::if_else(
        is.na(.data$alert_flag) & is.na(.data$validated_by),
        survey_conf$username,
        .data$validated_by
      ),
      validation_status = dplyr::case_when(
        # Preserve existing status if validated by someone else (not pipeline account user and not NA)
        !is.na(.data$validated_by) &
          .data$validated_by != survey_conf$username ~ .data$validation_status,
        # Apply new status only if validated_by is NA or matches kobo user
        !is.na(.data$alert_flag) ~ "validation_status_not_approved",
        is.na(.data$alert_flag) ~ "validation_status_approved",
        TRUE ~ .data$validation_status
      )
    ) |>
    dplyr::filter(!is.na(.data$submitted_by))

  validation_flags_long <- validation_flags_with_kobo_status |>
    dplyr::mutate(alert_flag = as.character(.data$alert_flag)) %>%
    tidyr::separate_rows("alert_flag", sep = ",\\s*") |>
    dplyr::select(-c(dplyr::starts_with("valid")))

  asset_id <- survey_conf[[config_key]]

  coasts::mdb_collection_push(
    data = validation_flags_with_kobo_status,
    connection_string = conf$storage$mongodb$connection_strings$validation,
    db_name = conf$storage$mongodb$databases$validation$database_name,
    collection_name = paste(
      conf$storage$mongodb$databases$validation$collections$flags,
      asset_id,
      sep = "-"
    )
  )

  coasts::mdb_collection_push(
    data = validation_flags_long,
    connection_string = conf$storage$mongodb$connection_strings$validation,
    db_name = conf$storage$mongodb$databases$validation$database_name,
    collection_name = paste(
      conf$storage$mongodb$databases$validation$collections$enumerators_stats,
      asset_id,
      sep = "-"
    )
  )

  logger::log_info("Validation synchronization completed successfully")
  invisible(NULL)
}
