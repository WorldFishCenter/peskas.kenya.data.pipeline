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
#'
#' @keywords validation
#' @export
alert_outlier <- function(
    x,
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
#' @keywords validation
#' @examples
#' \dontrun{
#' validate_dates(data)
#' }
#' @export
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
#' @keywords validation
#' @examples
#' \dontrun{
#' validate_nfishers(data, k = 3)
#' }
#' @export
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
#' @keywords validation
#' @examples
#' \dontrun{
#' validate_nboats(data, k = 2)
#' }
#' @export
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
#' This function calculates the upper bounds for catch data based on gear type and fish category.
#'
#' @param data A data frame containing columns: gear, fish_category (or total_catch_kg), and catch_kg (or total_catch_kg).
#' @param k A numeric value used in the LocScaleB function for outlier detection.
#' @param total_catch A logical value indicating whether to calculate bounds for total catch (TRUE) or individual catch (FALSE).
#'
#' @return A data frame with columns: gear, fish_category (if applicable), and upper.up (upper bound).
#'
#' @importFrom dplyr select filter bind_rows mutate
#' @importFrom purrr discard map
#' @importFrom tidyr separate
#' @importFrom magrittr extract2
#'
#' @keywords validation
#' @export
get_catch_bounds <- function(data = NULL, k = NULL, total_catch = FALSE) {
  get_bounds <- function(x = NULL, k = NULL) {
    catch_column <- if (total_catch) "total_catch_kg" else "catch_kg"
    univOutl::LocScaleB(x[[catch_column]], logt = TRUE, k = k) %>%
      magrittr::extract2("bounds")
  }

  data %>%
    dplyr::select(if (total_catch) c("gear", "total_catch_kg") else c("gear", "fish_category", "catch_kg")) %>%
    {
      if (!total_catch) dplyr::filter(., !.data$fish_category == "0") else .
    } %>%
    split(if (total_catch) interaction(.$gear) else interaction(.$gear, .$fish_category)) %>%
    purrr::discard(~ nrow(.) == 0) %>%
    purrr::map(get_bounds, k = k) %>%
    dplyr::bind_rows(.id = "gear_catch") %>%
    dplyr::mutate(upper.up = exp(.data$upper.up)) %>%
    tidyr::separate(col = "gear_catch", into = if (total_catch) "gear" else c("gear", "fish_category")) %>%
    dplyr::select(-"lower.low")
}

#' Validate Catch Data
#'
#' This function validates the catch data by comparing it to calculated upper bounds.
#'
#' @param data A data frame containing catch data with columns: catch_id, gear, fish_category (or total_catch_kg), and catch_kg (or total_catch_kg).
#' @param k A numeric value used in the get_catch_bounds function for outlier detection.
#' @param total_catch A logical value indicating whether to validate total catch (TRUE) or individual catch (FALSE).
#' @param flag_value A numeric value to use as the flag for catches exceeding the upper bound. Default is 4.
#'
#' @return A data frame with columns: catch_id, catch_kg (or total_catch_kg), and alert_catch.
#'
#' @keywords validation
#' @export
validate_catch <- function(data = NULL, k = NULL, total_catch = FALSE, flag_value = 4) {
  bounds <- get_catch_bounds(data = data, k = k, total_catch = total_catch)

  catch_column <- if (total_catch) "total_catch_kg" else "catch_kg"

  data %>%
    dplyr::select("catch_id", "gear", if (!total_catch) "fish_category", dplyr::all_of(catch_column)) %>%
    dplyr::left_join(bounds, by = if (total_catch) "gear" else c("gear", "fish_category")) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(
      alert_catch = ifelse(.data[[catch_column]] >= .data$upper.up, flag_value, NA_real_),
      !!catch_column := ifelse(is.na(.data$alert_catch), .data[[catch_column]], NA_real_)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select(-c("upper.up", "gear", if (!total_catch) "fish_category"))
}
