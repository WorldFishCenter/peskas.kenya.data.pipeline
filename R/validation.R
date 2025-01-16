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

  # Spot weird observations

  gear_requires_boats <- c("gillnet", "handline", "traps", "monofilament", "reefseine", "ringnet", "setnet")

  weird_submissions <-
    merged_landings |>
    dplyr::select("submission_id", "landing_date", "landing_site", "no_of_fishers", "n_boats", "gear", "total_catch_kg") |>
    dplyr::distinct() |>
    dplyr::mutate(
      alert_flag = dplyr::case_when(
        # Condition 1: No. of Fishers must be >= No. of Boats
        .data$no_of_fishers < .data$n_boats ~ 1,
        # Condition 2: No. of Boats must be > 0 for gear types that require boats
        .data$n_boats == 0 & .data$gear %in% gear_requires_boats ~ 2,
        # Condition 3: Total Catch cannot be negative
        .data$total_catch_kg < 0 ~ 3,
        # Condition 4: No. of Fishers or Boats must be positive integers
        .data$no_of_fishers <= 0 | .data$n_boats < 0 ~ 4,
        # Condition 5: Total Catch is zero but fishers/boats are non-zero
        .data$total_catch_kg == 0 & (.data$no_of_fishers > 0 | .data$n_boats > 0) ~ 5,
        # If none of the conditions are met, it's not weird
        TRUE ~ NA_real_
      )
    ) |>
    dplyr::filter(!is.na(.data$alert_flag)) |>
    dplyr::pull("submission_id") |>
    unique()

  # Remove weird submissions
  merged_landings <-
    merged_landings |>
    dplyr::filter(!.data$submission_id %in% weird_submissions)


  validation_output <-
    list(
      dates_alert = validate_dates(data = merged_landings),
      fishers_alert = validate_nfishers(data = merged_landings, k = conf$validation$k_nfishers),
      nboats_alert = validate_nboats(data = merged_landings, k = conf$validation$k_nboats),
      catch_alert = validate_catch(data = merged_landings, k = conf$validation$k_catch, flag_value = 4),
      total_catch_alert = validate_total_catch(data = merged_landings, k = conf$validation$k_catch, flag_value = 5)
    )

  validated_vars <-
    validation_output %>%
    purrr::discard(names(validation_output) == "total_catch_alert") %>%
    purrr::map(~ dplyr::select(.x, !dplyr::contains("alert"))) %>%
    purrr::reduce(dplyr::left_join, by = c("submission_id", "catch_id")) |>
    dplyr::left_join(validation_output$total_catch_alert, by = c("submission_id")) |>
    dplyr::select(-"alert_catch")

  # replace merged landings with validted variables and keep dataframe columns order
  validated_data <-
    merged_landings %>%
    dplyr::select(-c(names(validated_vars)[3:ncol(validated_vars)])) %>%
    dplyr::left_join(validated_vars, by = c("submission_id", "catch_id")) %>%
    dplyr::select(dplyr::all_of(colnames(merged_landings)))

  # alerts data
  alert_flags <-
    validation_output %>%
    purrr::discard(names(validation_output) == "total_catch_alert") %>%
    purrr::map(~ dplyr::select(.x, "submission_id", "catch_id", dplyr::contains("alert"))) %>%
    purrr::reduce(dplyr::full_join, by = c("submission_id", "catch_id")) %>%
    tidyr::unite(col = "alert_number", dplyr::contains("alert"), sep = "-", na.rm = TRUE)


  # upload validated outputs
  logger::log_info("Uploading validated data to mongodb")
  mdb_collection_push(
    data = validated_data,
    connection_string = conf$storage$mongodb$connection_string,
    collection_name = conf$storage$mongodb$database$pipeline$collection_name$ongoing$validated,
    db_name = conf$storage$mongodb$database$pipeline$name
  )
}
