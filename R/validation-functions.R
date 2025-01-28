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
#' @param flag_value A numeric value to use as the flag for catches exceeding the upper bound.
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
#' validate_dates(data, flag_value = 1)
#' }
#' @export
validate_dates <- function(data = NULL, flag_value = NULL) {
  data %>%
    dplyr::transmute(
      submission_id = .data$submission_id,
      catch_id = .data$catch_id,
      landing_date = .data$landing_date,
      alert_date = ifelse(.data$landing_date < "1990-01-01", flag_value, NA_real_)
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
#' @param flag_value A numeric value to use as the flag for catches exceeding the upper bound.
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
#' validate_nfishers(data, k = 3, flag_value = 2)
#' }
#' @export
validate_nfishers <- function(data = NULL, k = NULL, flag_value = NULL) {
  data %>%
    dplyr::transmute(
      .data$submission_id,
      .data$catch_id,
      .data$no_of_fishers,
      alert_n_fishers = alert_outlier(
        x = .data$no_of_fishers,
        alert_if_larger = flag_value, logt = TRUE, k = k
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
#' @param flag_value A numeric value to use as the flag for catches exceeding the upper bound.
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
#' validate_nboats(data, k = 2, flag_value = 3)
#' }
#' @export
validate_nboats <- function(data = NULL, k = NULL, flag_value = NULL) {
  data %>%
    dplyr::transmute(
      .data$submission_id,
      .data$catch_id,
      .data$n_boats,
      alert_n_boats = alert_outlier(
        x = .data$n_boats,
        alert_if_larger = flag_value, logt = TRUE, k = k
      )
    ) %>%
    dplyr::mutate(n_boats = ifelse(is.na(.data$alert_n_boats), .data$n_boats, NA_real_))
}


#' Get fish groups Catch Bounds
#'
#' Calculates the upper bounds for *fish groups* catch data (using \code{catch_kg})
#' based on gear type and fish category. Data is grouped by the interaction of gear
#' and fish category, and category "0" is excluded from the analysis.
#'
#' @param data A data frame containing columns: \code{gear}, \code{fish_category}, \code{catch_kg}.
#' @param k A numeric value used in the \code{univOutl::LocScaleB} function for outlier detection.
#'
#' @return A data frame with columns: \code{gear}, \code{fish_category}, and \code{upper.up} (the upper bound).
#'
#' @importFrom dplyr select filter bind_rows mutate
#' @importFrom purrr discard map
#' @importFrom tidyr separate
#' @importFrom magrittr extract2
#' @importFrom univOutl LocScaleB
#'
#' @keywords validation
#' @export
get_catch_bounds <- function(data = NULL, k = NULL) {
  # 1) Filter out non-valid fish categories
  # 2) Split by gear + fish_category
  # 3) Calculate upper bounds (on log scale, then exponentiate)

  data %>%
    dplyr::select("gear", "fish_category", "catch_kg") %>%
    dplyr::filter(!.data$fish_category == "0") %>%
    split(interaction(.$gear, .$fish_category)) %>%
    purrr::discard(~ nrow(.) == 0) %>%
    purrr::map(~ {
      univOutl::LocScaleB(.x[["catch_kg"]], logt = TRUE, k = k) %>%
        magrittr::extract2("bounds")
    }) %>%
    dplyr::bind_rows(.id = "gear_catch") %>%
    dplyr::mutate(upper.up = exp(.data$upper.up)) %>%
    tidyr::separate(col = "gear_catch", into = c("gear", "fish_category")) %>%
    dplyr::select(-"lower.low")
}

#' Get Total Catch Bounds
#'
#' Calculates the upper bounds for *total* catch data (using \code{total_catch_kg})
#' based on landing site and gear type combinations. NA values in total_catch_kg are
#' filtered out before analysis. The function groups data by combined landing_site
#' and gear identifiers before calculating bounds.
#'
#' @param data A data frame containing columns: \code{gear}, \code{landing_site}, \code{submission_id}
#'             and \code{total_catch_kg}.
#' @param k A numeric value used in the \code{univOutl::LocScaleB} function for outlier detection.
#'
#' @return A data frame with columns: \code{landing_site}, \code{gear} and \code{upper.up} (the upper bound).
#'
#' @importFrom dplyr select bind_rows mutate
#' @importFrom purrr discard map
#' @importFrom magrittr extract2
#' @importFrom univOutl LocScaleB
#'
#' @keywords validation
#' @export
get_total_catch_bounds <- function(data = NULL, k = NULL) {
  data |>
    dplyr::filter(!is.na(.data$total_catch_kg)) |>
    dplyr::select("landing_site", "submission_id", "gear", "total_catch_kg") %>%
    dplyr::distinct() |>
    dplyr::select(-"submission_id") %>%
    # Create a grouping identifier combining landing_site and gear
    dplyr::mutate(group_id = paste(.data$landing_site, .data$gear, sep = ".")) %>%
    split(.$group_id) |>
    purrr::discard(~ nrow(.) == 0) |>
    purrr::map(~ {
      univOutl::LocScaleB(.x[["total_catch_kg"]], logt = TRUE, k = k) %>%
        magrittr::extract2("bounds")
    }) %>%
    dplyr::bind_rows(.id = "group_id") |>
    # Split the group_id back into landing_site and gear
    tidyr::separate(.data$group_id, into = c("landing_site", "gear"), sep = "\\.", remove = TRUE) |>
    dplyr::mutate(upper.up = exp(.data$upper.up)) |>
    dplyr::select(-"lower.low")
}


#' Validate Individual Catch Data
#'
#' Compares each fish group catch (in \code{catch_kg}) to the upper bounds and flags
#' values that exceed the bound. Values exceeding bounds are set to NA in the catch_kg column.
#'
#' @param data A data frame containing columns: \code{catch_id}, \code{gear}, \code{fish_category}, \code{catch_kg}.
#' @param k A numeric value passed to \code{\link{get_catch_bounds}} for outlier detection.
#' @param flag_value A numeric value to use as the flag for catches exceeding the upper bound. Default is 4.
#'
#' @return A data frame with columns: \code{submission_id}, \code{catch_id}, \code{catch_kg}, and \code{alert_catch}.
#'
#' @importFrom dplyr select left_join rowwise mutate ungroup
#'
#' @keywords validation
#' @export
validate_catch <- function(data = NULL, k = NULL, flag_value = 4) {
  # Calculate bounds
  bounds <- get_catch_bounds(data, k)

  # Join bounds and flag outliers
  data %>%
    dplyr::select("submission_id", "catch_id", "gear", "fish_category", "catch_kg") %>%
    dplyr::left_join(bounds, by = c("gear", "fish_category")) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(
      alert_catch = ifelse(.data$catch_kg >= .data$upper.up, flag_value, NA_real_),
      # Optionally remove outliers from the dataset by setting them to NA
      catch_kg = ifelse(is.na(.data$alert_catch), .data$catch_kg, NA_real_)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select("submission_id", "catch_id", "catch_kg", "alert_catch")
}

#' Validate Total Catch Data
#'
#' Compares the total catch (in \code{total_catch_kg}) to the upper bounds and flags
#' values that exceed the bound. Values exceeding bounds are set to NA in the total_catch_kg column.
#' Bounds are calculated based on landing site and gear type combinations.
#'
#' @param data A data frame containing columns: \code{submission_id}, \code{landing_site}, \code{gear},
#'             \code{total_catch_kg}.
#' @param k A numeric value passed to \code{\link{get_total_catch_bounds}} for outlier detection.
#' @param flag_value A numeric value to use as the flag for catches exceeding the upper bound. Default is 4.
#'
#' @return A data frame with columns: \code{submission_id}, \code{total_catch_kg}, and \code{alert_catch}.
#'
#' @importFrom dplyr select left_join rowwise mutate ungroup
#'
#' @keywords validation
#' @export
validate_total_catch <- function(data = NULL, k = NULL, flag_value = 4) {
  # Calculate bounds
  bounds <- get_total_catch_bounds(data, k)

  # Join bounds and flag outliers
  data %>%
    dplyr::select("submission_id", "landing_site", "gear", "total_catch_kg") %>%
    dplyr::distinct() %>%
    dplyr::left_join(bounds, by = c("landing_site", "gear")) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(
      alert_catch = ifelse(.data$total_catch_kg >= .data$upper.up, flag_value, NA_real_),
      total_catch_kg = ifelse(is.na(.data$alert_catch), .data$total_catch_kg, NA_real_)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select("submission_id", "total_catch_kg", "alert_catch")
}
