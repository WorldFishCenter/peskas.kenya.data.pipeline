#' Prepare and Clean Legacy Landings Data
#'
#' This function imports, preprocesses, and cleans legacy landings data from an Excel file.
#' The data is cleaned by renaming columns, removing unnecessary columns, generating a unique
#' landing ID, and filtering the data based on a specific date threshold.
#'
#' @param ... Additional arguments passed to the `rio::import` function, such as file path or import options.
#'
#' @return A tibble containing the cleaned and preprocessed legacy landings data, with the following transformations:
#'   \item{landing_date}{Converted to `Date` format and filtered to include dates from 1990 onwards.}
#'   \item{landing_id}{A unique identifier generated for each landing record.}
#'   \item{landing_site, sector, gear, n_boats}{Columns renamed for clarity.}
#'
#' @examples
#' \dontrun{
#' cleaned_legacy_data <- prepare_legacy_landings()
#' }
#'
#' @export
prepare_legacy_landings <- function(...) {
  conf <- read_config()

  raw_legacy_dat <- peskas.kenya.data.pipeline::mdb_collection(
    collection_name = "legacy_data-raw",
    db_name = "kenya",
    connection_string = conf$storage$mongodb$connection_string
  ) |>
    dplyr::as_tibble()

  raw_legacy_dat %>%
    dplyr::select(-c("Months", "Year", "Day", "Month", "Management", "New_mngt", "Mngt")) %>%
    janitor::clean_names() %>%
    dplyr::rename(
      landing_date = "date",
      landing_site = "site",
      n_boats = "no_boats"
    ) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(survey_id = digest::digest(
      paste(.data$landing_date, .data$landing_site, .data$gear, .data$n_boats,
        sep = "_"
      ),
      algo = "crc32"
    )) %>%
    dplyr::group_by("survey_id") %>%
    dplyr::mutate(
      n_catch = seq(1, dplyr::n(), 1),
      catch_id = paste(.data$survey_id, .data$n_catch, sep = "-")
    ) %>%
    dplyr::select(
      "survey_id",
      "catch_id",
      "landing_date",
      "landing_site",
      dplyr::everything()
    ) %>%
    dplyr::select(-c(
      "n_catch", "sector", "size_km", "new_areas",
      "seascape", "new_fishing_areas", "total_catch"
    )) %>%
    dplyr::mutate(
      landing_date = as.Date(.data$landing_date, format = "%d/%m/%y")
    ) %>%
    dplyr::ungroup()
}

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
      dplyr::across(.cols = c(
        "catch_name", "gear", "gear_new",
        "fish_category", "ecology"
      ), tolower)
    ) %>%
    clean_catch_names()
}


clean_catch_names <- function(data = NULL) {
  data %>%
    dplyr::mutate(catch_name = dplyr::case_when(
      catch_name == "eeelfish" ~ "eel fish",
      catch_name == "sharks" ~ "shark",
      catch_name %in% c("silvermoony", "silver moony") ~ "silver moonfish",
      catch_name %in% c("soldier", "soldierfish") ~ "soldier fish",
      catch_name %in% c("surgeoms", "surgeon", "surgeonfish") ~ "surgeon fish",
      catch_name %in% c("sweatlips", "sweetlip") ~ "sweetlips",
      catch_name %in% c("zebra", "zebrafish") ~ "zebra fish",
      TRUE ~ catch_name
    ))
}
