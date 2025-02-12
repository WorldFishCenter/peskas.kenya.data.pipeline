#' Validate Fisheries Data
#'
#' This function imports and validates preprocessed fisheries data from a MongoDB collection. It conducts a series of validation checks to ensure data integrity, including checks on dates, fisher counts, boat numbers, and catch weights. The function then compiles the validated data and corresponding alert flags, which are subsequently uploaded back to MongoDB.
#'
#' @return This function does not return a value. Instead, it processes the data and uploads the validated results to a MongoDB collection in the pipeline databse.
#'
#' @details
#' The function performs the following main operations:
#' 1. Pulls preprocessed landings data from the preprocessed MongoDB collection.
#' 2. Validates the data for consistency and accuracy, focusing on:
#'    - Date validation
#'    - Number of fishers
#'    - Number of boats
#'    - Catch weight
#' 3. Generates a validated dataset that integrates the results of the validation checks.
#' 4. Creates alert flags to identify and track any data issues discovered during validation.
#' 5. Merges the validated data with additional metadata, such as survey details and landing site information.
#' 6. Uploads the validated dataset to the validated MongoDB collection.
#'
#' @note This function requires a configuration file to be present and readable by the 'read_config' function, which should provide MongoDB connection details and parameters for validation.
#'
#' @examples
#' \dontrun{
#' validate_landings()
#' }
#'
#' @keywords workflow validation
#' @export
validate_landings <- function() {
  conf <- read_config()

  merged_landings <-
    mdb_collection_pull(
      collection_name = conf$storage$mongodb$database$pipeline$collection_name$ongoing$merged_landings,
      db_name = conf$storage$mongodb$database$pipeline$name,
      connection_string = conf$storage$mongodb$connection_string
    ) |>
    dplyr::as_tibble()

  price_tables <-
    mdb_collection_pull(
      collection_name = conf$storage$mongodb$database$pipeline$collection_name$ongoing$price_table,
      db_name = conf$storage$mongodb$database$pipeline$name,
      connection_string = conf$storage$mongodb$connection_string
    ) |>
    dplyr::as_tibble() |>
    # price per kg cannot be zero
    dplyr::filter(!.data$median_ksh_kg == 0) |>
    impute_price() |>
    dplyr::mutate(year = lubridate::year(.data$date))

  # Spot weird observations
  gear_requires_boats <- c("reefseine", "beachseine", "ringnet", "long_line", "trollingline")


  logical_check <-
    merged_landings |>
    dplyr::select("version", "submission_id", "landing_date", "landing_site", "no_of_fishers", "n_boats", "gear", "total_catch_kg") |>
    dplyr::distinct() |>
    dplyr::mutate(
      alert_flag = dplyr::case_when(
        # Condition 1: No. of Fishers must be > No. of Boats, unless both are 1
        (.data$no_of_fishers < .data$n_boats | (.data$no_of_fishers == .data$n_boats & .data$no_of_fishers != 1)) ~ "1",
        # Condition 2: No. of Boats must be > 0 for gear types that require boats
        .data$n_boats == 0 & .data$gear %in% gear_requires_boats ~ "2",
        # Condition 3: Total Catch cannot be negative
        .data$total_catch_kg < 0 ~ "3",
        # Condition 4: No. of Fishers or Boats must be positive integers
        .data$no_of_fishers <= 0 | .data$n_boats < 0 ~ "4",
        # Condition 5: Total Catch is zero but fishers/boats are non-zero
        .data$total_catch_kg == 0 & (.data$no_of_fishers > 0 | .data$n_boats > 0) ~ "5",
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
      fishers_alert = validate_nfishers(data = merged_landings, k = conf$validation$k_nfishers, flag_value = 7),
      nboats_alert = validate_nboats(data = merged_landings, k = conf$validation$k_nboats, flag_value = 8),
      catch_alert = validate_catch(data = merged_landings, k = conf$validation$k_catch, flag_value = 9),
      total_catch_alert = validate_total_catch(data = merged_landings, k = conf$validation$k_catch, flag_value = 10),
      fishers_catch_alert = validate_fishers_catch(data = merged_landings, max_kg = conf$validation$max_kg, flag_value = 11)
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
    validation_output[c("dates_alert", "fishers_alert", "nboats_alert", "catch_alert")] %>%
    purrr::map(~ dplyr::select(.x, !dplyr::contains("alert"))) %>%
    purrr::reduce(dplyr::left_join, by = c("submission_id", "catch_id")) |>
    dplyr::left_join(validation_output$total_catch_alert, by = c("submission_id")) |>
    dplyr::left_join(validation_output$fishers_catch_alert, by = c("submission_id")) |>
    dplyr::mutate(total_catch_kg = dplyr::coalesce(.data$total_catch_kg.x, .data$total_catch_kg.y)) |>
    dplyr::select(-c("alert_catch", "alert_fishers_catch", "total_catch_kg.x", "total_catch_kg.y"))

  # replace merged landings with validted variables and keep dataframe columns order
  validated_data <-
    merged_landings %>%
    dplyr::select(-c(names(validated_vars)[3:ncol(validated_vars)])) %>%
    dplyr::left_join(validated_vars, by = c("submission_id", "catch_id")) %>%
    dplyr::select(dplyr::all_of(colnames(merged_landings)))

  # alerts data
  alert_flags <-
    validation_output[c("dates_alert", "fishers_alert", "nboats_alert", "catch_alert")] %>%
    purrr::map(~ dplyr::select(.x, "submission_id", "catch_id", dplyr::contains("alert"))) %>%
    purrr::reduce(dplyr::full_join, by = c("submission_id", "catch_id")) %>%
    dplyr::left_join(validation_output$total_catch_alert, by = c("submission_id")) |>
    dplyr::left_join(validation_output$fishers_catch_alert, by = c("submission_id")) |>
    tidyr::unite(col = "alert_number", dplyr::contains("alert"), sep = "-", na.rm = TRUE) |>
    dplyr::select("submission_id", "alert_number") |>
    dplyr::distinct() |>
    dplyr::full_join(logical_check, by = c("submission_id")) |>
    dplyr::select("version", "submission_id", "alert_number", "alert_flag") |>
    tidyr::unite(col = "alert_number", dplyr::contains("alert"), sep = "-", na.rm = TRUE)

  clean_data <-
    validated_data |>
    dplyr::left_join(alert_flags, by = c("version", "submission_id")) |>
    dplyr::filter(.data$alert_number == "") |>
    dplyr::select(-"alert_number") |>
    # Add catch prices
    dplyr::mutate(year = lubridate::year(.data$landing_date)) |>
    dplyr::left_join(price_tables, by = c("year", "landing_site", "fish_category", "size")) |>
    dplyr::mutate(catch_price = .data$median_ksh_kg_imputed * .data$catch_kg) |>
    dplyr::group_by(.data$submission_id) |>
    dplyr::mutate(
      total_catch_price = sum(.data$catch_price)
    ) |>
    dplyr::ungroup() |>
    dplyr::select(-c("date", "median_ksh_kg_imputed"))

  # Define the data and their corresponding collection names
  upload_data <- list(
    list(data = clean_data, collection_name = conf$storage$mongodb$database$pipeline$collection_name$ongoing$validated),
    list(data = alert_flags, collection_name = conf$storage$mongodb$database$pipeline$collection_name$validation_flags)
  )
  # Log messages for each upload
  log_messages <- c(
    "Uploading validated data to mongodb",
    "Uploading validation flags data to mongodb"
  )
  # Walk through both the data and the log messages
  purrr::walk2(
    upload_data, log_messages,
    ~ {
      logger::log_info(.y) # Log the current message
      mdb_collection_push(
        data = .x$data,
        connection_string = conf$storage$mongodb$connection_string,
        collection_name = .x$collection_name,
        db_name = conf$storage$mongodb$database$pipeline$name
      )
    }
  )
}
