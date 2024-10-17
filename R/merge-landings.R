#' Merge Legacy and Ongoing Landings Data
#'
#' This function merges preprocessed legacy landings data with ongoing landings data
#' from separate MongoDB collections. It combines the datasets, performs minimal
#' transformations, and uploads the merged result to a new MongoDB collection.
#'
#' @param log_threshold Logging threshold level (default: logger::DEBUG)
#'
#' @return This function does not return a value. Instead, it processes the data and uploads
#'   the result to a MongoDB collection in the pipeline database.
#'
#' @details
#' The function performs the following main operations:
#' 1. Pulls preprocessed legacy data from the legacy preprocessed MongoDB collection.
#' 2. Pulls preprocessed ongoing data from the ongoing preprocessed MongoDB collection.
#' 3. Combines the two datasets using `dplyr::bind_rows()`, adding a 'version' column to distinguish the sources.
#' 4. Selects and orders relevant columns for the final merged dataset.
#' 5. Uploads the merged data to a new MongoDB collection for merged landings data.
#'
#' @note This function requires a configuration file to be present and readable by the
#'   'read_config' function, which should provide MongoDB connection details.
#'
#' @keywords workflow data-merging
#' @examples
#' \dontrun{
#' merge_landings()
#' }
#' @export
merge_landings <- function(log_threshold = logger::DEBUG) {
  conf <- read_config()

  legacy <-
    mdb_collection_pull(
      collection_name = conf$storage$mongodb$database$pipeline$collection_name$legacy$preprocessed,
      db_name = conf$storage$mongodb$database$pipeline$name,
      connection_string = conf$storage$mongodb$connection_string
    ) |>
    dplyr::as_tibble()

  ongoing <-
    mdb_collection_pull(
      collection_name = conf$storage$mongodb$database$pipeline$collection_name$ongoing$preprocessed,
      db_name = conf$storage$mongodb$database$pipeline$name,
      connection_string = conf$storage$mongodb$connection_string
    ) |>
    dplyr::as_tibble()

  merged_landings <-
    dplyr::bind_rows(legacy, ongoing, .id = "version") %>%
    dplyr::select(
      "version", "form_consent", "submission_id", "catch_id", "landing_site",
      "fishing_ground", "lat", "lon", "no_of_fishers", "n_boats", "gear", "fish_category",
      "size", "catch_kg", "total_catch", "total_catch_form"
    )

  logger::log_info("Uploading merged landings data to mongodb")
  # upload preprocessed landings
  mdb_collection_push(
    data = merged_landings,
    connection_string = conf$storage$mongodb$connection_string,
    collection_name = conf$storage$mongodb$database$pipeline$collection_name$ongoing$merged_landings,
    db_name = conf$storage$mongodb$database$pipeline$name
  )
}
