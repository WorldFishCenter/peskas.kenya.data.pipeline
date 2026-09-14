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
#' @keywords workflow data-merging wcs
#' @examples
#' \dontrun{
#' merge_landings()
#' }
#' @export
merge_landings <- function(log_threshold = logger::DEBUG) {
  conf <- read_config()

  versions <- c("legacy", "v1", "v2")
  data_list <- versions |>
    purrr::set_names() |>
    purrr::map(
      ~ coasts::download_parquet_from_cloud(
        prefix = conf$surveys$wcs$catch[[.x]]$preprocessed$file_prefix,
        provider = conf$storage$google$key,
        options = conf$storage$google$options_wcs
      )
    )

  merged_landings <-
    dplyr::bind_rows(
      data_list$legacy,
      data_list$v1,
      data_list$v2,
      .id = "version"
    ) %>%
    dplyr::select(
      "version",
      "submission_id",
      "catch_id",
      "landing_date",
      "landing_site",
      "fishing_ground",
      "pds",
      "boat_name",
      "lat",
      "lon",
      "fisher_id",
      "trip_cost",
      "no_of_fishers",
      "n_boats",
      "gear",
      "fish_category",
      "size",
      "catch_kg",
      "total_catch_kg"
    )

  logger::log_info("Uploading merged landings data to google cloud storage")
  # upload preprocessed landings
  coasts::upload_parquet_to_cloud(
    data = merged_landings,
    prefix = conf$surveys$wcs$catch$merged$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options_wcs
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
#' 6. Collapses the table to exactly one median price per
#'    (date, landing_site, fish_category, size) key, so the many-to-one join
#'    in [validate_landings()] cannot duplicate catch rows
#' 7. Uploads the processed data back to MongoDB
#'
#' @keywords workflow wcs
#' @examples
#' \dontrun{
#' merge_prices()
#' }
#' @export
merge_prices <- function(log_threshold = logger::DEBUG) {
  conf <- read_config()

  logger::log_info("Downloading legacy price data from mongodb")

  legacy <- coasts::download_parquet_from_cloud(
    prefix = conf$surveys$wcs$catch$legacy$preprocessed$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options_wcs
  ) |>
    dplyr::mutate(size = NA_character_) |>
    dplyr::select(
      "landing_date",
      "landing_site",
      "fish_category",
      "size",
      "ksh_kg"
    ) |>
    summarise_catch_price(unit = "year")

  logger::log_info("Downloading ongoing price data from mongodb")

  v1_price <- coasts::download_parquet_from_cloud(
    prefix = conf$surveys$wcs$price$v1$preprocessed$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options_wcs
  ) |>
    dplyr::select(
      "landing_date",
      "landing_site",
      "fish_category",
      "size",
      "ksh_kg"
    ) |>
    summarise_catch_price(unit = "year")

  v2_price <- coasts::download_parquet_from_cloud(
    prefix = conf$surveys$wcs$price$v2$preprocessed$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options_wcs
  ) |>
    dplyr::select(
      "landing_date",
      "landing_site",
      "fish_category",
      "size",
      "ksh_kg"
    ) |>
    summarise_catch_price(unit = "year")

  price_table_raw <-
    dplyr::bind_rows(legacy, v1_price, v2_price) |>
    dplyr::distinct() |>
    dplyr::mutate(
      landing_site = dplyr::case_when(
        .data$landing_site == "rigati" ~ "rigata",
        .data$landing_site == "kiwayuu_cha_nje" ~ "kiwayuu_cha_inde",
        TRUE ~ .data$landing_site
      )
    )

  # The v1 and v2 price forms both collect during 2025, and the landing_site
  # recoding above folds two more spellings into their canonical form, so the
  # same (date, landing_site, fish_category, size) key can arrive from more
  # than one source with a different median. validate_landings() joins this
  # table many-to-one, so anything left duplicated here fans out the catch
  # rows it is joined onto. Collapse to exactly one median per key.
  price_table <-
    price_table_raw |>
    dplyr::group_by(
      .data$date,
      .data$landing_site,
      .data$fish_category,
      .data$size
    ) |>
    dplyr::summarise(
      median_ksh_kg = stats::median(.data$median_ksh_kg, na.rm = TRUE),
      .groups = "drop"
    )

  logger::log_info(
    "Price table collapsed to one median per key: {nrow(price_table_raw)} rows -> {nrow(price_table)} rows ({nrow(price_table_raw) - nrow(price_table)} duplicate keys removed)"
  )

  coasts::upload_parquet_to_cloud(
    data = price_table,
    prefix = conf$surveys$wcs$price$price_table$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options_wcs
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
    dplyr::mutate(
      date = lubridate::floor_date(.data$landing_date, unit = unit)
    ) |>
    dplyr::group_by(
      .data$date,
      .data$landing_site,
      .data$fish_category,
      .data$size
    ) |>
    dplyr::summarise(
      median_ksh_kg = stats::median(.data$ksh_kg, na.rm = T)
    ) |>
    dplyr::ungroup()
}
