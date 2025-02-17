#' Merge Legacy and Ongoing Landings Data
#'
#' This function merges preprocessed legacy landings data with ongoing landings data
#' from Google Cloud Storage. It combines the datasets, performs minimal
#' transformations, and uploads the merged result as a Parquet file.
#'
#' @param log_threshold Logging threshold level (default: logger::DEBUG)
#'
#' @return No return value. Function processes the data and uploads the result as a
#' Parquet file to Google Cloud Storage.
#'
#' @details
#' The function performs the following main operations:
#' 1. Downloads preprocessed legacy data from Google Cloud Storage.
#' 2. Downloads preprocessed ongoing data from Google Cloud Storage.
#' 3. Combines the two datasets using `dplyr::bind_rows()`, adding a 'version' column
#'    to distinguish the sources.
#' 4. Selects and orders relevant columns for the final merged dataset.
#' 5. Uploads the merged data as a Parquet file to Google Cloud Storage.
#'
#' @note This function requires a configuration file with Google Cloud Storage
#' credentials and file prefix settings.
#'
#' @keywords workflow data-merging
#' @examples
#' \dontrun{
#' merge_landings()
#' }
#' @export
merge_landings <- function(log_threshold = logger::DEBUG) {
  conf <- read_config()

  legacy <- download_parquet_from_cloud(
    prefix = conf$surveys$catch$legacy$preprocessed$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  ongoing <-
    download_parquet_from_cloud(
      prefix = conf$surveys$catch$ongoing$preprocessed$file_prefix,
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    )

  merged_landings <-
    dplyr::bind_rows(legacy, ongoing, .id = "version") %>%
    dplyr::select(
      "version", "submission_id", "catch_id", "landing_date", "landing_site",
      "fishing_ground", "lat", "lon", "no_of_fishers", "n_boats", "gear", "fish_category",
      "size", "catch_kg", "total_catch_kg"
    )

  logger::log_info("Uploading merged landings data to google cloud storage")
  # upload preprocessed landings
  upload_parquet_to_cloud(
    data = merged_landings,
    prefix = conf$surveys$catch$ongoing$merged$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )
}

#' Merge Price Data
#'
#' This function combines and processes legacy and ongoing catch price data from MongoDB collections,
#' aggregating prices by year and uploading the results back to MongoDB.
#'
#' @param log_threshold Logging threshold level (default: logger::DEBUG)
#' @return A tibble containing the processed and combined price data
#'
#' @details
#' The function performs the following main operations:
#' 1. Pulls legacy price data from MongoDB and summarizes it yearly
#' 2. Pulls ongoing price data from MongoDB and summarizes it yearly
#' 3. Combines legacy and ongoing data
#' 4. Filters data after 1990
#' 5. Removes duplicate entries
#' 6. Uploads the processed data back to MongoDB
#'
#' @keywords workflow
#' @examples
#' \dontrun{
#' merge_prices()
#' }
#' @export
merge_prices <- function(log_threshold = logger::DEBUG) {
  conf <- read_config()

  logger::log_info("Downloading legacy price data from mongodb")

  legacy <- download_parquet_from_cloud(
    prefix = conf$surveys$catch$legacy$preprocessed$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  ) |>
    dplyr::mutate(size = NA_character_) |>
    dplyr::select("landing_date", "landing_site", "fish_category", "size", "ksh_kg") |>
    summarise_catch_price(unit = "year")


  logger::log_info("Downloading ongoing price data from mongodb")

  ongoing_price <- download_parquet_from_cloud(
    prefix = conf$surveys$price$preprocessed$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  ) |>
    dplyr::select("landing_date", "landing_site", "fish_category", "size", "ksh_kg") |>
    summarise_catch_price(unit = "year")


  price_table <-
    dplyr::bind_rows(legacy, ongoing_price) |>
    dplyr::distinct() |>
    dplyr::mutate(landing_site = dplyr::case_when(
      .data$landing_site == "rigati" ~ "rigata",
      .data$landing_site == "kiwayuu_cha_nje" ~ "kiwayuu_cha_inde",
      TRUE ~ .data$landing_site
    ))

  upload_parquet_to_cloud(
    data = price_table,
    prefix = conf$surveys$price$price_table$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )
}

#' Summarize Catch Price Data
#'
#' This function aggregates catch price data by a specified time unit,
#' calculating median prices per kilogram for each fish category at each landing site.
#'
#' @param data A tibble containing catch price data with columns: landing_date,
#'             landing_site, fish_category, and ksh_kg
#' @param unit Character string specifying the time unit for aggregation
#'             (e.g., "year", "month", "week"). Passed to lubridate::floor_date()
#'
#' @return A tibble containing summarized price data with columns:
#'         date, landing_site, fish_category, size, and median_ksh_kg
#'
#' @details
#' The function:
#' 1. Floors dates to the specified unit using lubridate
#' 2. Groups data by date, landing site, fish category and size
#' 3. Calculates median price per kilogram for each group
#'
#' @keywords helper
#' @examples
#' \dontrun{
#' summarise_catch_price(data = price_data, unit = "year")
#' summarise_catch_price(data = price_data, unit = "month")
#' }
#' @export
summarise_catch_price <- function(data = NULL, unit = NULL) {
  data |>
    dplyr::mutate(date = lubridate::floor_date(.data$landing_date, unit = unit)) |>
    dplyr::group_by(.data$date, .data$landing_site, .data$fish_category, .data$size) |>
    dplyr::summarise(
      median_ksh_kg = stats::median(.data$ksh_kg, na.rm = T)
    ) |>
    dplyr::ungroup()
}
