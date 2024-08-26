#' Preprocess Legacy Landings Data
#'
#' This function imports, preprocesses, and cleans legacy landings data from a MongoDB collection.
#' It performs various data cleaning and transformation operations, including column renaming,
#' removal of unnecessary columns, generation of unique identifiers, and data type conversions.
#' The processed data is then uploaded back to MongoDB.
#'
#' @return This function does not return a value. Instead, it processes the data and uploads
#'   the result to a MongoDB collection in the "kenya" database.
#'
#' @details
#' The function performs the following main operations:
#' 1. Pulls raw data from the "legacy_data-raw" MongoDB collection.
#' 2. Removes several unnecessary columns.
#' 3. Renames columns for clarity (e.g., 'site' to 'landing_site').
#' 4. Generates unique 'survey_id' and 'catch_id' fields.
#' 5. Converts several string fields to lowercase.
#' 6. Cleans catch names using a separate function 'clean_catch_names'.
#' 7. Uploads the processed data to the "legacy_data-preprocessed" MongoDB collection.
#'
#' @note This function requires a configuration file to be present and readable by the
#'   'read_config' function, which should provide MongoDB connection details.
#'
#' @examples
#' \dontrun{
#' preprocess_legacy_landings()
#' }
#'
#' @export

preprocess_legacy_landings <- function() {
  conf <- read_config()

  # get raw landings from mongodb
  raw_legacy_dat <- mdb_collection_pull(
    collection_name = "legacy-raw",
    db_name = "pipeline",
    connection_string = conf$storage$mongodb$connection_string
  ) |>
    dplyr::as_tibble()

  # preprocess raw landings
  processed_legacy_landings <-
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
    dplyr::group_by(.data$survey_id) %>%
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
    dplyr::ungroup() |>
    dplyr::mutate(
      dplyr::across(.cols = c(
        "catch_name", "gear", "gear_new",
        "fish_category", "ecology"
      ), tolower)
    ) %>%
    clean_catch_names()


  logger::log_info("Uploading preprocessed data to mongodb")
  # upload preprocessed landings
  mdb_collection_push(
    data = processed_legacy_landings,
    connection_string = conf$storage$mongodb$connection_string,
    collection_name = "legacy-preprocessed",
    db_name = "pipeline"
  )
}


#' Clean Catch Names
#'
#' This function standardizes catch names in the dataset by correcting common misspellings
#' and inconsistencies.
#'
#' @param data A data frame or tibble containing a column named `catch_name`.
#'
#' @return A data frame or tibble with standardized catch names in the `catch_name` column.
#'
#' @examples
#' \dontrun{
#' cleaned_data <- clean_catch_names(data)
#' }
#'
#' @keywords internal
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
