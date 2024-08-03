#' Preprocess Legacy Landings Data
#'
#' This function preprocesses legacy landings data by converting it into a tibble,
#' transforming the `landing_date` column to `Date` format, and converting
#' specified columns to lowercase.
#'
#' @param landings A data frame containing legacy WCS SSF landings data. The data frame
#' should include the columns: `landing_date`, `catch_name`, `gear`, `gear_new`,
#' `fish_category`, and `ecology`.
#'
#' @return A tibble with transformed data:
#'   \item{landing_date}{Converted to `Date` format.}
#'   \item{catch_name, gear, gear_new, fish_category, ecology}{Converted to lowercase.}
#'
#' @examples
#' \dontrun{
#' preprocessed_data <- preprocess_legacy_landings(landings_data)
#' }
#'
#' @export
preprocess_legacy_landings <- function(landings = NULL) {
  landings %>%
    dplyr::as_tibble() %>%
    dplyr::mutate(
      landing_date = as.Date(as.POSIXct("landing_date", origin = "1970-01-01")),
      dplyr::across(.cols = c(
        "catch_name", "gear", "gear_new",
        "fish_category", "ecology"
      ), tolower)
    )
}
