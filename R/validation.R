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


  validation_output <-
    list(
      dates_alert = validate_dates(data = merged_landings),
      fishers_alert = validate_nfishers(data = merged_landings, k = conf$validation$k_nfishers),
      nboats_alert = validate_nboats(data = merged_landings, k = conf$validation$k_nboats),
      catch_alert = validate_catch(data = merged_landings, k = conf$validation$k_catch),
      total_catch_alert = validate_total_catch(data = merged_landings, k = conf$validation$k_catch)
    )

  validated_vars <-
    validation_output %>%
    purrr::map(~ dplyr::select(.x, !dplyr::contains("alert"))) %>%
    purrr::reduce(dplyr::left_join, by = c("catch_id"))

  # replace merged landings with validted variables and keep dataframe columns order
  validated_data <-
    merged_landings %>%
    dplyr::select(-c(names(validated_vars)[2:ncol(validated_vars)])) %>%
    dplyr::left_join(validated_vars, by = c("catch_id")) %>%
    dplyr::select(dplyr::all_of(colnames(merged_landings)))

  # alerts data
  alert_flags <-
    validation_output %>%
    purrr::map(~ dplyr::select(.x, "catch_id", dplyr::contains("alert"))) %>%
    purrr::reduce(dplyr::full_join, by = "catch_id") %>%
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
