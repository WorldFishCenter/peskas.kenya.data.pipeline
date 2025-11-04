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
  ...
) {
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

  if (isTRUE(algo_args$logt)) {
    bounds <- exp(bounds) - 1
  }

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
      alert_date = ifelse(
        .data$landing_date < "1990-01-01",
        flag_value,
        NA_real_
      )
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
        alert_if_larger = flag_value,
        logt = TRUE,
        k = k
      )
    ) %>%
    dplyr::mutate(
      no_of_fishers = ifelse(
        is.na(.data$alert_n_fishers),
        .data$no_of_fishers,
        NA_real_
      )
    )
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
        alert_if_larger = flag_value,
        logt = TRUE,
        k = k
      )
    ) %>%
    dplyr::mutate(
      n_boats = ifelse(is.na(.data$alert_n_boats), .data$n_boats, NA_real_)
    )
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
    purrr::map(
      ~ {
        univOutl::LocScaleB(.x[["catch_kg"]], logt = TRUE, k = k) %>%
          magrittr::extract2("bounds")
      }
    ) %>%
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
    dplyr::mutate(
      group_id = paste(.data$landing_site, .data$gear, sep = ".")
    ) %>%
    split(.$group_id) |>
    purrr::discard(~ nrow(.) == 0) |>
    purrr::map(
      ~ {
        univOutl::LocScaleB(.x[["total_catch_kg"]], logt = TRUE, k = k) %>%
          magrittr::extract2("bounds")
      }
    ) %>%
    dplyr::bind_rows(.id = "group_id") |>
    # Split the group_id back into landing_site and gear
    tidyr::separate(
      .data$group_id,
      into = c("landing_site", "gear"),
      sep = "\\.",
      remove = TRUE
    ) |>
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
validate_catch <- function(data = NULL, k = NULL, flag_value = NULL) {
  # Calculate bounds
  bounds <- get_catch_bounds(data, k)

  # Join bounds and flag outliers
  data %>%
    dplyr::select(
      "submission_id",
      "catch_id",
      "gear",
      "fish_category",
      "catch_kg"
    ) %>%
    dplyr::left_join(bounds, by = c("gear", "fish_category")) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(
      alert_catch = ifelse(
        .data$catch_kg >= .data$upper.up,
        flag_value,
        NA_real_
      ),
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
validate_total_catch <- function(data = NULL, k = NULL, flag_value = NULL) {
  # Calculate bounds
  bounds <- get_total_catch_bounds(data, k)

  # Join bounds and flag outliers
  data %>%
    dplyr::select("submission_id", "landing_site", "gear", "total_catch_kg") %>%
    dplyr::distinct() %>%
    dplyr::left_join(bounds, by = c("landing_site", "gear")) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(
      alert_catch = ifelse(
        .data$total_catch_kg >= .data$upper.up,
        flag_value,
        NA_real_
      ),
      total_catch_kg = ifelse(
        is.na(.data$alert_catch),
        .data$total_catch_kg,
        NA_real_
      )
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select("submission_id", "total_catch_kg", "alert_catch")
}

#' Validate Catch per Fisher
#'
#' This function validates the relationship between total catch and number of fishers.
#' It flags cases where a single fisher reports a catch exceeding the specified maximum.
#' When flagged, the total catch value is set to NA.
#'
#' @param data A data frame containing the columns:
#'   \itemize{
#'     \item submission_id: Unique identifier for the submission
#'     \item no_of_fishers: Number of fishers
#'     \item total_catch_kg: Total catch in kilograms
#'   }
#' @param max_kg Numeric value specifying the maximum catch (in kg) allowed for a single fisher
#' @param flag_value A numeric value to use as the flag for catches exceeding the maximum per fisher
#'
#' @return A data frame with columns:
#'   \itemize{
#'     \item submission_id: The original submission identifier
#'     \item total_catch_kg: The original catch if valid, otherwise NA
#'     \item alert_catch: Flag value if catch per fisher exceeds maximum, otherwise NA
#'   }
#'
#' @importFrom dplyr select distinct mutate ungroup
#'
#' @keywords validation
#' @examples
#' \dontrun{
#' validate_fishers_catch(data, max_kg = 100, flag_value = 5)
#' }
#' @export
validate_fishers_catch <- function(
  data = NULL,
  max_kg = NULL,
  flag_value = NULL
) {
  data %>%
    dplyr::select("submission_id", "no_of_fishers", "total_catch_kg") %>%
    dplyr::distinct() %>%
    dplyr::mutate(
      alert_fishers_catch = dplyr::case_when(
        .data$no_of_fishers == 1 & .data$total_catch_kg >= max_kg ~ flag_value,
        TRUE ~ NA_real_
      ),
      total_catch_kg = ifelse(
        is.na(.data$alert_fishers_catch),
        .data$total_catch_kg,
        NA_real_
      )
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select("submission_id", "total_catch_kg", "alert_fishers_catch")
}

#' Impute Missing Fish Prices Using Median Values
#'
#' @description
#' This function imputes missing fish prices in two steps:
#' 1. For fish with size (small/large): uses median price from other landing sites
#' 2. For fish with NA size: uses median between small and large sizes
#'
#' @param price_table A tibble containing fish price data with columns:
#'   \itemize{
#'     \item date: Date of the record
#'     \item landing_site: Name of the landing site
#'     \item fish_category: Type of fish
#'     \item size: Size category of fish (large, small, or NA)
#'     \item median_ksh_kg: Original price in Kenyan Shillings per kg
#'   }
#'
#' @return A tibble with the same structure as input, but with:
#'   \itemize{
#'     \item All possible combinations of date, landing_site, fish_category and their valid sizes
#'     \item Original median_ksh_kg column removed
#'     \item New median_ksh_kg_imputed column containing original and imputed prices
#'   }
#'
#' @examples
#' \dontrun{
#' imputed_data <- impute_price(price_table = fish_prices)
#' }
#'
#' @importFrom dplyr distinct group_by mutate ungroup select
#' @importFrom tidyr complete nesting
#' @importFrom stats median
#'
#' @keywords helper
#'
#' @export
impute_price <- function(price_table = NULL) {
  valid_combinations <- price_table %>%
    dplyr::distinct(.data$fish_category, .data$size)

  # First imputation for all sizes
  imputed_prices <- price_table %>%
    tidyr::complete(
      tidyr::nesting(fish_category, size),
      .data$date,
      .data$landing_site
    ) %>%
    # First impute by size group (small and large)
    dplyr::group_by(.data$date, .data$fish_category, .data$size) %>%
    dplyr::mutate(
      median_ksh_kg_imputed = dplyr::case_when(
        !is.na(.data$median_ksh_kg) ~ .data$median_ksh_kg,
        TRUE ~ stats::median(.data$median_ksh_kg, na.rm = TRUE)
      )
    ) %>%
    dplyr::ungroup()

  # Then calculate median between small and large for NA sizes
  imputed_prices <- imputed_prices %>%
    dplyr::group_by(.data$date, .data$fish_category, .data$landing_site) %>%
    dplyr::mutate(
      median_ksh_kg_imputed = dplyr::case_when(
        !is.na(.data$median_ksh_kg_imputed) ~ .data$median_ksh_kg_imputed,
        is.na(.data$size) ~ stats::median(
          .data$median_ksh_kg_imputed[size %in% c("small", "large")],
          na.rm = TRUE
        ),
        TRUE ~ .data$median_ksh_kg_imputed
      )
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select(-"median_ksh_kg") %>%
    dplyr::distinct()

  return(imputed_prices)
}

#' Generate an alert vector based on IQR method
#'
#' @param x numeric vector where outliers will be checked
#' @param no_alert_value value to put in the output when there is no alert
#' @param alert_if_larger alert for when x is above the upper bound
#' @param alert_if_smaller alert for when x is below the lower bound
#' @param multiplier multiplier for IQR range (default is 1.5)
#' @return a vector of the same length as x with alert values
#'
#' @keywords validation
#' @export
alert_outlier_iqr <- function(
  x,
  no_alert_value = NA_real_,
  alert_if_larger = no_alert_value,
  alert_if_smaller = no_alert_value,
  multiplier = 1.5
) {
  # Helper function to check if everything is NA or zero
  all_na_or_zero <- function(x) {
    isTRUE(all(is.na(x) | x == 0))
  }

  # If everything is NA or zero there is nothing to compute
  if (all_na_or_zero(x)) {
    return(NA_real_) # Changed to match alert_outlier behavior
  }

  q <- stats::quantile(x, probs = c(0.25, 0.75), na.rm = TRUE)
  iqr <- q[2] - q[1]

  # If IQR is zero, we can't compute meaningful bounds
  if (iqr <= 0) {
    return(NA_real_)
  }

  lb <- q[1] - (multiplier * iqr)
  ub <- q[2] + (multiplier * iqr)

  dplyr::case_when(
    x < lb ~ alert_if_smaller,
    x > ub ~ alert_if_larger,
    TRUE ~ no_alert_value
  )
}

#' Get fish groups Catch Bounds using IQR method
#'
#' @param data A data frame containing columns: gear, fish_category, catch_kg
#' @param multiplier multiplier for IQR range (default is 1.5)
#' @return A data frame with columns: gear, fish_category, upper.up
#'
#' @keywords validation
#' @export
get_catch_bounds_iqr <- function(data = NULL, multiplier = 1.5) {
  # Check for NULL or empty data
  if (is.null(data) || nrow(data) == 0) {
    stop("Input data is NULL or empty")
  }

  # Check for required columns
  required_cols <- c("gear", "fish_category", "catch_kg")
  if (!all(required_cols %in% names(data))) {
    missing_cols <- setdiff(required_cols, names(data))
    stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
  }

  bounds_df <- data %>%
    # Select only needed columns
    dplyr::select(dplyr::all_of(required_cols)) %>%
    # Ensure unique combinations
    dplyr::distinct() %>%
    # Remove any invalid fish categories (matching MAD version)
    dplyr::filter(!is.na(.data$fish_category)) %>%
    dplyr::filter(!.data$fish_category == "0") %>%
    # Group by gear and fish category
    dplyr::group_by(.data$gear, .data$fish_category) %>%
    # Calculate quartiles and n for each group
    dplyr::summarise(
      q1 = stats::quantile(.data$catch_kg, 0.25, na.rm = TRUE),
      q3 = stats::quantile(.data$catch_kg, 0.75, na.rm = TRUE),
      n = dplyr::n(),
      .groups = "drop"
    ) %>%
    # Filter groups with enough observations
    dplyr::filter(.data$n > 1) %>% # Need at least 2 points for IQR
    # Calculate IQR and upper bound
    dplyr::mutate(
      iqr = .data$q3 - .data$q1,
      upper.up = .data$q3 + multiplier * .data$iqr
    ) %>%
    # Keep only needed columns
    dplyr::select("gear", "fish_category", "upper.up", "n")

  # Check if any bounds were calculated
  if (nrow(bounds_df) == 0) {
    warning("No bounds could be calculated - check your data")
  }

  return(bounds_df)
}

#' Get Total Catch Bounds using IQR method
#'
#' @param data A data frame containing required columns
#' @param multiplier multiplier for IQR range (default is 1.5)
#' @return A data frame with upper bounds for each landing site and gear combination
#'
#' @keywords validation
#' @export
get_total_catch_bounds_iqr <- function(data = NULL, multiplier = 1.5) {
  # Check for NULL or empty data
  if (is.null(data) || nrow(data) == 0) {
    stop("Input data is NULL or empty")
  }

  # Check for required columns
  required_cols <- c("landing_site", "gear", "total_catch_kg")
  if (!all(required_cols %in% names(data))) {
    missing_cols <- setdiff(required_cols, names(data))
    stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
  }

  bounds_df <- data %>%
    # Select and filter
    dplyr::select(dplyr::all_of(required_cols)) %>%
    dplyr::filter(!is.na(.data$total_catch_kg)) %>%
    dplyr::distinct() %>% # Added to match validate_total_catch behavior
    # Group and calculate
    dplyr::group_by(.data$landing_site, .data$gear) %>%
    dplyr::summarise(
      q1 = stats::quantile(.data$total_catch_kg, 0.25, na.rm = TRUE),
      q3 = stats::quantile(.data$total_catch_kg, 0.75, na.rm = TRUE),
      n = dplyr::n(),
      .groups = "drop"
    ) %>%
    # Filter groups with enough observations
    dplyr::filter(.data$n > 1) %>%
    # Calculate bounds
    dplyr::mutate(
      iqr = .data$q3 - .data$q1,
      upper.up = .data$q3 + multiplier * .data$iqr
    ) %>%
    dplyr::select("landing_site", "gear", "upper.up", "n")

  if (nrow(bounds_df) == 0) {
    warning("No bounds could be calculated - check your data")
  }

  return(bounds_df)
}

#' Validate Individual Catch Data using IQR method
#'
#' @param data A data frame containing required columns
#' @param multiplier multiplier for IQR range (default is 1.5)
#' @param flag_value A numeric value to use as the flag for catches exceeding bounds
#' @return A data frame with validated catch data and alert flags
#'
#' @keywords validation
#' @export
validate_catch_iqr <- function(
  data = NULL,
  multiplier = 1.5,
  flag_value = NULL
) {
  # Check for NULL or empty data
  if (is.null(data) || nrow(data) == 0) {
    stop("Input data is NULL or empty")
  }

  # Check for required columns
  required_cols <- c(
    "submission_id",
    "catch_id",
    "gear",
    "fish_category",
    "catch_kg"
  )
  if (!all(required_cols %in% names(data))) {
    missing_cols <- setdiff(required_cols, names(data))
    stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
  }

  # Calculate bounds
  bounds <- get_catch_bounds_iqr(data, multiplier)

  # Validate using bounds (removed rowwise as it's not needed)
  data %>%
    dplyr::select(dplyr::all_of(required_cols)) %>%
    dplyr::left_join(bounds, by = c("gear", "fish_category")) %>%
    dplyr::mutate(
      alert_catch = dplyr::case_when(
        is.na(.data$upper.up) ~ NA_real_,
        .data$catch_kg >= .data$upper.up ~ flag_value,
        TRUE ~ NA_real_
      ),
      catch_kg = dplyr::if_else(
        is.na(.data$alert_catch),
        .data$catch_kg,
        NA_real_,
        NA_real_
      )
    ) %>%
    dplyr::select("submission_id", "catch_id", "catch_kg", "alert_catch")
}

#' Validate Total Catch Data using IQR method
#'
#' @param data A data frame containing required columns
#' @param multiplier multiplier for IQR range (default is 1.5)
#' @param flag_value A numeric value to use as the flag for catches exceeding bounds
#' @return A data frame with validated total catch data and alert flags
#'
#' @keywords validation
#' @export
validate_total_catch_iqr <- function(
  data = NULL,
  multiplier = 1.5,
  flag_value = NULL
) {
  # Check for NULL or empty data
  if (is.null(data) || nrow(data) == 0) {
    stop("Input data is NULL or empty")
  }

  # Check for required columns
  required_cols <- c("submission_id", "landing_site", "gear", "total_catch_kg")
  if (!all(required_cols %in% names(data))) {
    missing_cols <- setdiff(required_cols, names(data))
    stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
  }

  # Get unique combinations and calculate bounds
  data_unique <- data %>%
    dplyr::select(dplyr::all_of(required_cols)) %>%
    dplyr::distinct()

  bounds <- get_total_catch_bounds_iqr(data_unique, multiplier)

  # Validate using bounds (removed rowwise as it's not needed)
  data_unique %>%
    dplyr::left_join(bounds, by = c("landing_site", "gear")) %>%
    dplyr::mutate(
      alert_catch = dplyr::case_when(
        is.na(.data$upper.up) ~ NA_real_,
        .data$total_catch_kg >= .data$upper.up ~ flag_value,
        TRUE ~ NA_real_
      ),
      total_catch_kg = dplyr::if_else(
        is.na(.data$alert_catch),
        .data$total_catch_kg,
        NA_real_,
        NA_real_
      )
    ) %>%
    dplyr::select("submission_id", "total_catch_kg", "alert_catch")
}

#' Check for outliers using IQR method
#'
#' @param x numeric vector where outliers will be checked
#' @param multiplier multiplier for IQR range (default is 1.5)
#' @return a logical vector indicating which values are within bounds (TRUE) or outliers (FALSE)
#'
#' @keywords validation
#' @examples
#' \dontrun{
#' x <- c(1, 2, 3, 100)
#' check_outliers_iqr(x, multiplier = 1.5)
#' }
#' @export
check_outliers_iqr <- function(x, multiplier = 1.5) {
  # Check for NULL
  if (is.null(x)) {
    stop("Input vector is NULL")
  }

  # Check if numeric
  if (!is.numeric(x)) {
    stop("Input must be numeric")
  }

  # If everything is NA or zero return NA
  if (all(is.na(x) | x == 0)) {
    return(rep(NA, length(x)))
  }

  # Calculate quartiles and IQR
  q <- stats::quantile(x, probs = c(0.25, 0.75), na.rm = TRUE)
  iqr <- q[2] - q[1]

  # If IQR is zero, we can't compute meaningful bounds
  if (iqr <= 0) {
    return(rep(NA, length(x)))
  }

  # Calculate bounds
  lb <- q[1] - (multiplier * iqr)
  ub <- q[2] + (multiplier * iqr)

  # Return logical vector
  x >= lb & x <= ub
}

#' Validate Number of Fishers using IQR method
#'
#' @param data A data frame containing the no_of_fishers column
#' @param multiplier multiplier for IQR range (default is 1.5)
#' @param flag_value A numeric value to use as the flag for values outside bounds
#' @return A data frame with validated no_of_fishers and alert flags
#'
#' @keywords validation
#' @examples
#' \dontrun{
#' validate_nfishers_iqr(data, multiplier = 1.5, flag_value = 7)
#' }
#' @export
validate_nfishers_iqr <- function(
  data = NULL,
  multiplier = 1.5,
  flag_value = NULL
) {
  # Check for NULL or empty data
  if (is.null(data) || nrow(data) == 0) {
    stop("Input data is NULL or empty")
  }

  # Check for required columns
  required_cols <- c("submission_id", "catch_id", "no_of_fishers")
  if (!all(required_cols %in% names(data))) {
    missing_cols <- setdiff(required_cols, names(data))
    stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
  }

  data %>%
    dplyr::transmute(
      .data$submission_id,
      .data$catch_id,
      .data$no_of_fishers,
      alert_n_fishers = alert_outlier_iqr(
        x = .data$no_of_fishers,
        alert_if_larger = flag_value,
        multiplier = multiplier
      )
    ) %>%
    dplyr::mutate(
      no_of_fishers = dplyr::if_else(
        is.na(.data$alert_n_fishers),
        .data$no_of_fishers,
        NA_real_,
        NA_real_
      )
    )
}

#' Validate Number of Boats using IQR method
#'
#' @param data A data frame containing the n_boats column
#' @param multiplier multiplier for IQR range (default is 1.5)
#' @param flag_value A numeric value to use as the flag for values outside bounds
#' @return A data frame with validated n_boats and alert flags
#'
#' @keywords validation
#' @examples
#' \dontrun{
#' validate_nboats_iqr(data, multiplier = 1.5, flag_value = 8)
#' }
#' @export
validate_nboats_iqr <- function(
  data = NULL,
  multiplier = 1.5,
  flag_value = NULL
) {
  # Check for NULL or empty data
  if (is.null(data) || nrow(data) == 0) {
    stop("Input data is NULL or empty")
  }

  # Check for required columns
  required_cols <- c("submission_id", "catch_id", "n_boats")
  if (!all(required_cols %in% names(data))) {
    missing_cols <- setdiff(required_cols, names(data))
    stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
  }

  data %>%
    dplyr::transmute(
      .data$submission_id,
      .data$catch_id,
      .data$n_boats,
      alert_n_boats = alert_outlier_iqr(
        x = .data$n_boats,
        alert_if_larger = flag_value,
        multiplier = multiplier
      )
    ) %>%
    dplyr::mutate(
      n_boats = dplyr::if_else(
        is.na(.data$alert_n_boats),
        .data$n_boats,
        NA_real_,
        NA_real_
      )
    )
}


#' Get Validation Status from KoboToolbox
#'
#' Retrieves the validation status for a specific submission in KoboToolbox.
#' The function handles NULL responses and returns a consistent tibble structure
#' regardless of the API response.
#'
#' @param submission_id Character string. The ID of the submission to check.
#' @param asset_id Character string. The asset ID from KoboToolbox.
#' @param token Character string. The authorization token for KoboToolbox API.
#' @param debug Logical. If TRUE, prints the request object. Default is FALSE.
#'
#' @return A tibble with one row containing:
#'   \item{submission_id}{The ID of the checked submission}
#'   \item{validation_status}{The validation status (e.g., "validation_status_approved" or "not_validated")}
#'   \item{validated_at}{Timestamp of validation as POSIXct}
#'   \item{validated_by}{Username of the validator}
#'
#' @keywords validation
#' @examples
#' \dontrun{
#' # Single submission
#' get_validation_status(
#'   submission_id = "1234567",
#'   asset_id = "your asset id",
#'   token = "Token YOUR_TOKEN_HERE"
#' )
#'
#' # Multiple submissions using purrr
#' submission_ids <- c("1234567", "154267")
#' submission_ids %>%
#'   purrr::map_dfr(get_validation_status,
#'     asset_id = "your asset id",
#'     token = "Token YOUR_TOKEN_HERE"
#'   )
#' }
#'
#' @keywords workflow validation
#' @export
get_validation_status <- function(
  submission_id = NULL,
  asset_id = NULL,
  token = NULL,
  debug = FALSE
) {
  base_url <- paste0(
    "https://eu.kobotoolbox.org/api/v2/assets/",
    asset_id,
    "/data/"
  )
  url <- paste0(base_url, submission_id, "/validation_status/")

  # Add "Token " prefix to token if it doesn't already have it
  if (!grepl("^Token ", token)) {
    token <- paste("Token", token)
  }

  req <- httr2::request(url) %>%
    httr2::req_headers(
      Authorization = token
    ) %>%
    httr2::req_method("GET")

  if (debug) {
    print(req)
  }

  tryCatch(
    {
      response <- httr2::req_perform(req)
      if (httr2::resp_status(response) == 200) {
        validation_data <- httr2::resp_body_json(response)

        # Handle NULL validation data
        timestamp <- if (
          !is.null(validation_data) && !is.null(validation_data$timestamp)
        ) {
          lubridate::as_datetime(validation_data$timestamp)
        } else {
          lubridate::as_datetime(NA)
        }

        status <- if (
          !is.null(validation_data) && !is.null(validation_data$uid)
        ) {
          validation_data$uid
        } else {
          "not_validated"
        }

        validator <- if (
          !is.null(validation_data) && !is.null(validation_data$by_whom)
        ) {
          validation_data$by_whom
        } else {
          NA_character_
        }

        dplyr::tibble(
          submission_id = submission_id,
          validation_status = status,
          validated_at = timestamp,
          validated_by = validator
        )
      } else {
        dplyr::tibble(
          submission_id = submission_id,
          validation_status = "not_validated",
          validated_at = lubridate::as_datetime(NA),
          validated_by = NA_character_
        )
      }
    },
    error = function(e) {
      if (debug) {
        cat("Error:", as.character(e), "\n")
      }

      dplyr::tibble(
        submission_id = submission_id,
        validation_status = "not_validated",
        validated_at = lubridate::as_datetime(NA),
        validated_by = NA_character_
      )
    }
  )
}
#' Update Validation Status in KoboToolbox
#'
#' Updates the validation status for a specific submission in KoboToolbox.
#' The function allows setting the status to approved, not approved, or on hold.
#'
#' @param submission_id Character string. The ID of the submission to update.
#' @param asset_id Character string. The asset ID from KoboToolbox.
#' @param token Character string. The authorization token for KoboToolbox API.
#' @param status Character string. The validation status to set. Must be one of:
#'        "validation_status_approved", "validation_status_not_approved", or
#'        "validation_status_on_hold".
#' @param debug Logical. If TRUE, prints the request object and response. Default is FALSE.
#'
#' @return A tibble with one row containing:
#'   \item{submission_id}{The ID of the updated submission}
#'   \item{validation_status}{The new validation status}
#'   \item{validated_at}{Timestamp of validation as POSIXct}
#'   \item{validated_by}{Username of the validator}
#'   \item{update_success}{Logical indicating if the update was successful}
#'
#' @keywords validation
#' @examples
#' \dontrun{
#' # Update a single submission
#' update_validation_status(
#'   submission_id = "1234567",
#'   asset_id = "your asset id",
#'   token = "Token YOUR_TOKEN_HERE",
#'   status = "validation_status_approved"
#' )
#'
#' # Update multiple submissions using purrr
#' submission_ids <- c("1234567", "154267")
#' submission_ids %>%
#'   purrr::map_dfr(update_validation_status,
#'     asset_id = "your asset id",
#'     token = "Token YOUR_TOKEN_HERE",
#'     status = "validation_status_approved"
#'   )
#' }
#'
#' @keywords workflow validation
#' @export
update_validation_status <- function(
  submission_id = NULL,
  asset_id = NULL,
  token = NULL,
  status = "validation_status_approved",
  debug = FALSE
) {
  # Validate status
  valid_statuses <- c(
    "validation_status_approved",
    "validation_status_not_approved",
    "validation_status_on_hold"
  )

  if (!status %in% valid_statuses) {
    stop("Status must be one of: ", paste(valid_statuses, collapse = ", "))
  }

  # Construct the URL
  base_url <- paste0(
    "https://eu.kobotoolbox.org/api/v2/assets/",
    asset_id,
    "/data/"
  )
  url <- paste0(base_url, submission_id, "/validation_status/")

  # Set up request body
  body <- list(
    "validation_status.uid" = status
  )

  # Add "Token " prefix to token if it doesn't already have it
  if (!grepl("^Token ", token)) {
    token <- paste("Token", token)
  }

  # Set up request
  req <- httr2::request(url) %>%
    httr2::req_headers(
      Authorization = token,
      "Content-Type" = "application/json"
    ) %>%
    httr2::req_method("PATCH") %>%
    httr2::req_body_json(body)

  if (debug) {
    print(req)
    print(body)
  }

  tryCatch(
    {
      response <- httr2::req_perform(req)

      if (debug) {
        cat("Response status:", httr2::resp_status(response), "\n")
        cat("Response body:", httr2::resp_body_string(response), "\n")
      }

      if (httr2::resp_status(response) %in% c(200, 201, 204)) {
        # If update was successful, get the current validation status
        updated_data <- get_validation_status(
          submission_id = submission_id,
          asset_id = asset_id,
          token = token,
          debug = debug
        )

        # Add success indicator
        updated_data %>%
          dplyr::mutate(update_success = TRUE)
      } else {
        dplyr::tibble(
          submission_id = submission_id,
          validation_status = NA_character_,
          validated_at = lubridate::as_datetime(NA),
          validated_by = NA_character_,
          update_success = FALSE
        )
      }
    },
    error = function(e) {
      if (debug) {
        cat("Error:", as.character(e), "\n")
      }

      dplyr::tibble(
        submission_id = submission_id,
        validation_status = NA_character_,
        validated_at = lubridate::as_datetime(NA),
        validated_by = NA_character_,
        update_success = FALSE
      )
    }
  )
}

#' Generate Trip-Level Validation Flags
#'
#' This function validates trip-level characteristics from KEFS survey data and generates
#' alert flags for anomalous values. It checks for unrealistic or inconsistent values in
#' horse power, number of fishers, trip duration, and total catch price.
#'
#' @param dat A data frame containing KEFS survey trip data with columns:
#'   \itemize{
#'     \item submission_id: Unique identifier for the submission
#'     \item hp: Horse power of the boat
#'     \item no_of_fishers: Number of fishers on the trip
#'     \item trip_duration: Duration of the trip in hours
#'     \item catch_outcome: Whether catch was recorded ("yes" or "no")
#'     \item total_catch_price: Total price of catch in Kenyan Shillings
#'     \item total_catch_weight: Total weight of catch in kilograms
#'     \item mesh_size: Mesh size used (if applicable)
#'   }
#' @param limits A list containing threshold values for validation:
#'   \itemize{
#'     \item max_hp: Maximum acceptable horse power
#'     \item max_n_fishers: Maximum acceptable number of fishers
#'     \item max_trip_duration: Maximum acceptable trip duration in hours
#'     \item max_revenue: Maximum acceptable revenue in Kenyan Shillings
#'   }
#'
#' @return A data frame with columns:
#'   \itemize{
#'     \item submission_id: The original submission identifier
#'     \item alert_flag_trip: Comma-separated string of alert codes (NA if no alerts):
#'       \itemize{
#'         \item "1": Horse power is <= 0 or exceeds maximum
#'         \item "2": Number of fishers is <= 0 or exceeds maximum
#'         \item "3": Trip duration is <= 0 or exceeds maximum
#'         \item "4.1": Catch outcome is "yes" but total catch price is <= 0
#'         \item "4.2": Catch outcome is "no" but total catch price is >= 0
#'         \item "4.3": Total catch price exceeds maximum revenue limit
#'       }
#'   }
#'
#' @keywords validation
#' @examples
#' \dontrun{
#' trip_limits <- list(
#'   max_hp = 150,
#'   max_n_fishers = 100,
#'   max_trip_duration = 96,
#'   max_revenue = 387600
#' )
#' trip_flags <- get_trips_flags(dat = survey_data, limits = trip_limits)
#' }
#' @export
get_trips_flags <- function(dat = NULL, limits = NULL) {
  dat |>
    dplyr::select(
      "submission_id":"mesh_size",
      "catch_outcome",
      "total_catch_price",
      "total_catch_weight"
    ) |>
    dplyr::distinct() |>
    dplyr::mutate(
      alert_hp_anomalous = dplyr::case_when(
        .data$hp <= 0 | .data$hp > limits$max_hp ~ "1",
        TRUE ~ NA_character_
      ),
      alert_n_fishers = dplyr::case_when(
        .data$no_of_fishers <= 0 |
          .data$no_of_fishers > limits$max_n_fishers ~ "2",
        TRUE ~ NA_character_
      ),
      alert_trip_duration = dplyr::case_when(
        .data$trip_duration <= 0 |
          .data$trip_duration > limits$max_trip_duration ~ "3",
        TRUE ~ NA_character_
      ),
      alert_tot_catch_price = dplyr::case_when(
        .data$catch_outcome == "yes" & .data$total_catch_price <= 0 ~ "4.1",
        .data$catch_outcome == "no" & .data$total_catch_price >= 0 ~ "4.2",
        .data$total_catch_price > limits$max_revenue ~ "4.3",
        TRUE ~ NA_character_
      )
    ) |>
    dplyr::select(
      "submission_id",
      dplyr::starts_with("alert_")
    ) |>
    dplyr::mutate(
      alert_flag = paste(
        .data$alert_hp_anomalous,
        .data$alert_n_fishers,
        .data$alert_trip_duration,
        .data$alert_tot_catch_price,
        sep = ","
      ) |>
        stringr::str_remove_all("NA,") |>
        stringr::str_remove_all(",NA") |>
        stringr::str_remove_all("^NA$")
    ) |>
    dplyr::mutate(
      alert_flag = ifelse(
        .data$alert_flag == "",
        NA_character_,
        .data$alert_flag
      )
    ) |>
    dplyr::group_by(.data$submission_id) %>%
    # Summarize to get values
    dplyr::summarise(
      alert_flag_trip = if (all(is.na(.data$alert_flag))) {
        NA_character_
      } else {
        paste(.data$alert_flag[!is.na(.data$alert_flag)], collapse = ", ")
      }
    )
}

#' Generate Catch-Level Validation Flags
#'
#' This function validates catch-level data from KEFS surveys and generates alert flags
#' for inconsistencies in sample weights relative to total catch weights. It focuses on
#' submissions where catch outcome was "yes" and checks for logical inconsistencies in
#' the relationship between sample weights and total weights.
#'
#' @param dat A data frame containing KEFS survey catch data with columns:
#'   \itemize{
#'     \item submission_id: Unique identifier for the submission
#'     \item catch_outcome: Whether catch was recorded ("yes" or "no")
#'     \item total_sample_weight: Total weight of all samples in kilograms
#'     \item total_catch_weight: Total weight of the entire catch in kilograms
#'     \item sample_weight: Weight of individual sample in kilograms
#'     \item sample_price: Price of the sample
#'   }
#'
#' @return A data frame with columns:
#'   \itemize{
#'     \item submission_id: The original submission identifier
#'     \item alert_flag_catch: Comma-separated string of alert codes (NA if no alerts):
#'       \itemize{
#'         \item "5.1": Total sample weight exceeds total catch weight
#'         \item "5.2": Individual sample weight exceeds either total sample weight
#'                      or total catch weight
#'       }
#'   }
#'
#' @details
#' The function only processes submissions where catch_outcome is "yes". It identifies
#' two types of weight inconsistencies:
#' \enumerate{
#'   \item The sum of all samples exceeds the total reported catch
#'   \item An individual sample weighs more than the total it's supposed to be part of
#' }
#'
#' @keywords validation
#' @examples
#' \dontrun{
#' catch_flags <- get_catch_flags(dat = survey_data)
#' }
#' @export
get_catch_flags <- function(dat = NULL) {
  dat |>
    dplyr::filter(
      .data$catch_outcome == "yes"
    ) |>
    dplyr::select(c(
      "submission_id",
      "catch_outcome":"sample_price"
    )) |>
    dplyr::distinct() |>
    dplyr::mutate(
      alert_total_sample = dplyr::case_when(
        .data$total_sample_weight > .data$total_catch_weight ~ "5.1",
        TRUE ~ NA_character_
      ),
      alert_sample = dplyr::case_when(
        .data$sample_weight > .data$total_sample_weight |
          .data$sample_weight > .data$total_catch_weight ~ "5.2",
        TRUE ~ NA_character_
      )
    ) |>
    dplyr::select(
      "submission_id",
      dplyr::starts_with("alert_")
    ) |>
    dplyr::mutate(
      alert_flag = paste(
        .data$alert_total_sample,
        .data$alert_sample,
        sep = ","
      ) |>
        stringr::str_remove_all("NA,") |>
        stringr::str_remove_all(",NA") |>
        stringr::str_remove_all("^NA$")
    ) |>
    dplyr::mutate(
      alert_flag = ifelse(
        .data$alert_flag == "",
        NA_character_,
        .data$alert_flag
      )
    ) |>
    dplyr::group_by(.data$submission_id) %>%
    # Summarize to get values
    dplyr::summarise(
      alert_flag_catch = if (all(is.na(.data$alert_flag))) {
        NA_character_
      } else {
        paste(.data$alert_flag[!is.na(.data$alert_flag)], collapse = ", ")
      }
    )
}

#' Generate Composite Indicator Validation Flags
#'
#' This function validates derived fisheries indicators (CPUE, RPUE, and price per kg)
#' from KEFS survey data. It calculates these performance metrics and flags values that
#' exceed specified maximum thresholds. This validation is applied only to submissions
#' that have passed previous validation checks.
#'
#' @param dat A data frame containing KEFS survey data with columns:
#'   \itemize{
#'     \item submission_id: Unique identifier for the submission
#'     \item no_of_fishers: Number of fishers on the trip
#'     \item trip_duration: Duration of the trip in hours
#'     \item total_catch_weight: Total weight of catch in kilograms
#'     \item total_catch_price: Total price of catch in Kenyan Shillings
#'   }
#' @param limits A list containing threshold values for validation:
#'   \itemize{
#'     \item max_cpue: Maximum acceptable catch per unit effort (kg/fisher/hour)
#'     \item max_rpue: Maximum acceptable revenue per unit effort (KSH/fisher/hour)
#'     \item max_price_kg: Maximum acceptable price per kilogram (KSH/kg)
#'   }
#' @param clean_ids A vector of submission IDs that have passed previous validation checks.
#'   Only these submissions will be evaluated for indicator-based anomalies.
#'
#' @return A data frame with columns:
#'   \itemize{
#'     \item submission_id: The original submission identifier
#'     \item alert_flag_indicators: Comma-separated string of alert codes (NA if no alerts):
#'       \itemize{
#'         \item "6.1": CPUE (catch per unit effort) exceeds maximum
#'         \item "6.2": RPUE (revenue per unit effort) exceeds maximum
#'         \item "6.3": Price per kilogram exceeds maximum
#'       }
#'   }
#'
#' @details
#' The function calculates three key fisheries performance indicators:
#' \enumerate{
#'   \item CPUE (Catch Per Unit Effort): total_catch_weight / no_of_fishers / trip_duration
#'   \item RPUE (Revenue Per Unit Effort): total_catch_price / no_of_fishers / trip_duration
#'   \item Price per kg: total_catch_price / total_catch_weight
#' }
#'
#' These indicators help identify submissions with unrealistically high productivity or
#' pricing that may indicate data entry errors or exceptional circumstances requiring review.
#'
#' This function is typically called after trip-level and catch-level validations have been
#' performed, applying additional scrutiny only to submissions that have already been
#' determined to be generally valid.
#'
#' @keywords validation
#' @examples
#' \dontrun{
#' # Get IDs that passed previous validation
#' clean_ids <- dplyr::full_join(trip_flags, catch_flags, by = "submission_id") |>
#'   dplyr::filter(is.na(alert_flag_trip) & is.na(alert_flag_catch)) |>
#'   dplyr::pull(submission_id) |>
#'   unique()
#'
#' indicator_limits <- list(
#'   max_cpue = 20,
#'   max_rpue = 3876,
#'   max_price_kg = 3876
#' )
#' indicator_flags <- get_indicators_flags(
#'   dat = survey_data,
#'   limits = indicator_limits,
#'   clean_ids = clean_ids
#' )
#' }
#' @export
get_indicators_flags <- function(dat = NULL, limits = NULL, clean_ids = NULL) {
  dat |>
    dplyr::filter(.data$submission_id %in% clean_ids) |>
    dplyr::select(
      "submission_id",
      "no_of_fishers",
      "trip_duration",
      "total_catch_weight",
      "total_catch_price"
    ) |>
    dplyr::distinct() |>
    dplyr::transmute(
      submission_id = .data$submission_id,
      no_of_fishers,
      .data$trip_duration,
      .data$total_catch_weight,
      price_kg = .data$total_catch_price / .data$total_catch_weight,
      cpue = .data$total_catch_weight /
        .data$no_of_fishers /
        .data$trip_duration,
      rpue = .data$total_catch_price /
        .data$no_of_fishers /
        .data$trip_duration,
    ) |>
    dplyr::mutate(
      alert_cpue = dplyr::case_when(
        .data$cpue > limits$max_cpue ~ "6.1",
        TRUE ~ NA_character_
      ),
      alert_rpue = dplyr::case_when(
        .data$rpue > limits$max_rpue ~ "6.2",
        TRUE ~ NA_character_
      ),
      alert_price_kg = dplyr::case_when(
        .data$price_kg > limits$max_price_kg ~ "6.3",
        TRUE ~ NA_character_
      )
    ) |>
    dplyr::select(
      "submission_id",
      dplyr::starts_with("alert_")
    ) |>
    dplyr::mutate(
      alert_flag = paste(
        .data$alert_cpue,
        .data$alert_rpue,
        .data$alert_price_kg,
        sep = ","
      ) |>
        stringr::str_remove_all("NA,") |>
        stringr::str_remove_all(",NA") |>
        stringr::str_remove_all("^NA$")
    ) |>
    dplyr::mutate(
      alert_flag = ifelse(
        .data$alert_flag == "",
        NA_character_,
        .data$alert_flag
      )
    ) |>
    dplyr::group_by(.data$submission_id) %>%
    # Summarize to get values
    dplyr::summarise(
      alert_flag_indicators = if (all(is.na(.data$alert_flag))) {
        NA_character_
      } else {
        paste(.data$alert_flag[!is.na(.data$alert_flag)], collapse = ", ")
      }
    )
}
