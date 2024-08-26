#' Validate Legacy Fisheries Data
#'
#' This function imports and validates preprocessed legacy fisheries data from a MongoDB collection. It conducts a series of validation checks to ensure data integrity, including checks on dates, fisher counts, boat numbers, and catch weights. The function then compiles the validated data and corresponding alert flags, which are subsequently uploaded back to MongoDB.
#'
#' @return This function does not return a value. Instead, it processes the data and uploads the validated results and alert flags to a MongoDB collection named "legacy_data-validated" in the "kenya" database.
#'
#' @details
#' The function performs the following main operations:
#' 1. Pulls preprocessed legacy landings data from the "legacy_data-preprocessed" MongoDB collection.
#' 2. Validates the data for consistency and accuracy, focusing on:
#'    - Date validation
#'    - Number of fishers
#'    - Number of boats
#'    - Catch weight
#' 3. Generates a validated dataset that integrates the results of the validation checks.
#' 4. Creates alert flags to identify and track any data issues discovered during validation.
#' 5. Merges the validated data with additional metadata, such as survey details and landing site information.
#' 6. Uploads the validated dataset to the "legacy_data-validated" MongoDB collection.
#'
#' @note This function requires a configuration file to be present and readable by the 'read_config' function, which should provide MongoDB connection details and parameters for validation.
#'
#' @examples
#' \dontrun{
#' validate_legacy_landings()
#' }
#'
#' @export
validate_legacy_landings <- function() {
  conf <- read_config()

  preprocessed_legacy_landings <-
    mdb_collection_pull(
      collection_name = "legacy-preprocessed",
      db_name = "pipeline",
      connection_string = conf$storage$mongodb$connection_string
    ) |>
    dplyr::as_tibble()


  validation_output <-
    list(
      dates_alert = validate_dates(data = preprocessed_legacy_landings),
      fishers_alert = validate_nfishers(data = preprocessed_legacy_landings, k = conf$validation$k_nfishers),
      nboats_alert = validate_nboats(data = preprocessed_legacy_landings, k = conf$validation$k_nboats),
      catch_alert = validate_catch(data = preprocessed_legacy_landings, k = conf$validation$k_catch)
    )

  # validated data
  base_data <-
    preprocessed_legacy_landings %>%
    dplyr::select(
      "survey_id", "catch_id", "landing_site",
      "gear", "gear_new", "fish_category",
      "catch_name", "ecology", "price"
    )

  validated_output <-
    validation_output %>%
    purrr::map(~ dplyr::select(.x, !dplyr::contains("alert"))) %>%
    purrr::reduce(dplyr::left_join, by = "catch_id") %>%
    dplyr::left_join(base_data, by = "catch_id") %>%
    dplyr::select(
      "survey_id", "catch_id", "landing_site", "landing_date",
      "no_of_fishers", "n_boats", "gear", "gear_new",
      "fish_category", "catch_name", "ecology", "price",
      "catch_kg"
    )

  # alerts data
  alert_flags <-
    validation_output %>%
    purrr::map(~ dplyr::select(.x, "catch_id", dplyr::contains("alert"))) %>%
    purrr::reduce(dplyr::left_join, by = "catch_id") %>%
    tidyr::unite(col = "alert_number", dplyr::contains("alert"), sep = "-", na.rm = TRUE)


  # upload validated outputs
  logger::log_info("Uploading validated data to mongodb")
  mdb_collection_push(
    data = validated_output,
    connection_string = conf$storage$mongodb$connection_string,
    collection_name = "legacy-validated",
    db_name = "pipeline"
  )
}

#' Generate an alert vector based on the `univOutl::LocScaleB()` function
#'
#' @param x numeric vector where outliers will be checked
#' @param no_alert_value value to put in the output when there is no alert (x is within bounds)
#' @param alert_if_larger alert for when x is above the bounds found by `univOutl::LocScaleB()`
#' @param alert_if_smaller alert for when x is below the bounds found by `univOutl::LocScaleB()`
#' @param ... arguments for `univOutl::LocScaleB()`
#'
#' @return a vector of the same lenght as x
#' @importFrom stats mad
alert_outlier <- function(x,
                          no_alert_value = NA_real_,
                          alert_if_larger = no_alert_value,
                          alert_if_smaller = no_alert_value,
                          ...) {
  algo_args <- list(...)

  # Helper function to check if everything is NA or zero
  all_na_or_zero <- function(x) {
    isTRUE(all(is.na(x) | x == 0))
  }

  # If everything is NA or zero there is nothing to compute
  if (all_na_or_zero(x)) {
    return(NA_real_)
  }
  # If the median absolute deviation is zero we shouldn't be using this algo
  if (mad(x, na.rm = T) <= 0) {
    return(NA_real_)
  }
  # If weights are specified and they are all NA or zero
  if (!is.null(algo_args$weights)) {
    if (all_na_or_zero(algo_args$weights)) {
      return(NA_real_)
    }
  }

  bounds <- univOutl::LocScaleB(x, ...) %>%
    magrittr::extract2("bounds")

  if (isTRUE(algo_args$logt)) bounds <- exp(bounds) - 1

  dplyr::case_when(
    x < bounds[1] ~ alert_if_smaller,
    x > bounds[2] ~ alert_if_larger,
    TRUE ~ no_alert_value
  )
}


#' Validate Landing Dates
#'
#' This function checks the validity of the `landing_date` in the provided dataset.
#' If the `landing_date` is before 1990-01-01, an alert (error label) with the number 1 is triggered.
#' The `landing_date` is then set to `NA` for those records.
#'
#' @param data A data frame containing the `landing_date` column.
#'
#' @return A data frame with two columns: `landing_date` and `alert_date`.
#'   - `landing_date`: The original date if valid, otherwise `NA`.
#'   - `alert_date`: A numeric value indicating the alert (error label) number, where 1 represents an invalid date.
#'
#' @importFrom dplyr transmute mutate case_when
#'
#' @examples
#' \dontrun{
#' validate_dates(data)
#' }
validate_dates <- function(data = NULL) {
  data %>%
    dplyr::transmute(
      catch_id = .data$catch_id,
      landing_date = .data$landing_date,
      alert_date = ifelse(.data$landing_date < "1990-01-01", 1, NA_real_)
    ) %>%
    dplyr::mutate(
      landing_date = dplyr::case_when(
        is.na(alert_date) ~ .data$landing_date,
        TRUE ~ as.Date(NA_real_)
      )
    )
}

#' Validate Number of Fishers
#'
#' This function validates the `no_of_fishers` column in the provided dataset.
#' An alert (error label) is triggered if the number of fishers is an outlier,
#' determined by the `alert_outlier` function with specified parameters. The alert number
#' is stored in `alert_n_fishers`. If an alert is triggered, the `no_of_fishers` value
#' is set to `NA`.
#'
#' @param data A data frame containing the `no_of_fishers` column.
#' @param k a numeric value used in the LocScaleB function for outlier detection.
#'
#' @return A data frame with two columns: `no_of_fishers` and `alert_n_fishers`.
#'   - `no_of_fishers`: The original number of fishers if valid, otherwise `NA`.
#'   - `alert_n_fishers`: A numeric value indicating the alert (error label) number.
#'
#' @importFrom dplyr transmute mutate
#'
#' @examples
#' \dontrun{
#' validate_nfishers(data, k = 3)
#' }
validate_nfishers <- function(data = NULL, k = NULL) {
  data %>%
    dplyr::transmute(
      .data$catch_id,
      .data$no_of_fishers,
      alert_n_fishers = alert_outlier(
        x = .data$no_of_fishers,
        alert_if_larger = 2, logt = TRUE, k = k
      )
    ) %>%
    dplyr::mutate(no_of_fishers = ifelse(is.na(.data$alert_n_fishers), .data$no_of_fishers, NA_real_))
}

#' Validate Number of Boats
#'
#' This function validates the `n_boats` column in the provided dataset.
#' An alert (error label) is triggered if the number of boats is an outlier,
#' determined by the `alert_outlier` function with specified parameters. The alert number
#' is stored in `alert_n_boats`. If an alert is triggered, the `n_boats` value
#' is set to `NA`.
#'
#' @param data A data frame containing the `n_boats` column.
#' @param k a numeric value used in the LocScaleB function for outlier detection.
#'
#' @return A data frame with two columns: `n_boats` and `alert_n_boats`.
#'   - `n_boats`: The original number of boats if valid, otherwise `NA`.
#'   - `alert_n_boats`: A numeric value indicating the alert (error label) number.
#'
#' @importFrom dplyr transmute mutate
#'
#' @examples
#' \dontrun{
#' validate_nboats(data, k = 2)
#' }
validate_nboats <- function(data = NULL, k = NULL) {
  data %>%
    dplyr::transmute(
      .data$catch_id,
      .data$n_boats,
      alert_n_boats = alert_outlier(
        x = .data$n_boats,
        alert_if_larger = 3, logt = TRUE, k = k
      )
    ) %>%
    dplyr::mutate(n_boats = ifelse(is.na(.data$alert_n_boats), .data$n_boats, NA_real_))
}

#' Get Catch Bounds
#'
#' This function calculates the upper bounds for catch data based on gear type and catch name.
#'
#' @param data A data frame containing columns: gear_new, catch_name, and catch_kg.
#' @param k A numeric value used in the LocScaleB function for outlier detection.
#'
#' @return A data frame with columns: gear_new, catch_name, and upper.up (upper bound).
#'
#' @importFrom dplyr select filter bind_rows mutate
#' @importFrom purrr discard map
#' @importFrom tidyr separate
#' @importFrom magrittr extract2
#'
#' @examples
#' \dontrun{
#' catch_bounds <- get_catch_bounds(your_data, k = 2)
#' }
get_catch_bounds <- function(data = NULL, k = NULL) {
  get_bounds <- function(x = NULL, k = NULL) {
    univOutl::LocScaleB(x$catch_kg, logt = TRUE, k = k) %>%
      magrittr::extract2("bounds")
  }

  data %>%
    dplyr::select("gear_new", "catch_name", "catch_kg") %>%
    dplyr::filter(!.data$catch_name == "0") %>%
    split(interaction(.$gear_new, .$catch_name)) %>%
    purrr::discard(~ nrow(.) == 0) %>%
    purrr::map(get_bounds, k = k) %>%
    dplyr::bind_rows(.id = "gear_catch") %>%
    dplyr::mutate(upper.up = exp(.data$upper.up)) %>%
    tidyr::separate(col = "gear_catch", into = c("gear_new", "catch_name")) %>%
    dplyr::select(-"lower.low")
}

#' Validate Catch Data
#'
#' This function validates the catch data by comparing it to calculated upper bounds.
#'
#' @param data A data frame containing catch data with columns: catch_id, gear_new, catch_name, and catch_kg.
#' @param k A numeric value used in the get_catch_bounds function for outlier detection.
#'
#' @return A data frame with columns: catch_id, catch_kg, and alert_catch.
#'
#' @importFrom dplyr select left_join mutate
#'
#' @examples
#' \dontrun{
#' validated_catch <- validate_catch(your_data, k = 3)
#' }
validate_catch <- function(data = NULL, k = NULL) {
  bounds <- get_catch_bounds(data = data, k = k)

  data %>%
    dplyr::select("catch_id", "gear_new", "catch_name", "catch_kg") %>%
    dplyr::left_join(bounds, by = c("gear_new", "catch_name")) %>%
    dplyr::rowwise() |>
    dplyr::mutate(
      alert_catch = ifelse(.data$catch_kg >= .data$upper.up, 4, NA_real_),
      catch_kg = ifelse(is.na(.data$alert_catch), .data$catch_kg, NA_real_)
    ) %>%
    dplyr::ungroup() |>
    dplyr::select(-c("upper.up", "gear_new", "catch_name"))
}
