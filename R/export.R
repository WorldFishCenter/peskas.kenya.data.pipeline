#' Export Summarized Fishery Data
#'
#' @description
#' This function processes and exports validated fishery data by calculating various
#' summary metrics and distributions, then uploads the results to MongoDB for
#' dashboard usage.
#'
#' @details
#' The function performs the following operations:
#' 1. Pulls validated data from the "legacy-validated" MongoDB collection
#' 2. Creates three distinct summary datasets:
#'    a. Monthly summaries: Aggregates catch, effort, and CPUE metrics by BMU and month
#'    b. Gear distribution: Calculates gear usage percentages by landing site
#'    c. Fish distribution: Calculates catch composition percentages by landing site
#' 3. Uploads all three datasets to their respective MongoDB collections
#'
#' The following metrics are calculated:
#' - Effort = Number of fishers / Size of BMU in km
#' - CPUE = Total catch in kg / Effort
#' - Monthly aggregations include:
#'   * Total catch (kg)
#'   * Mean catch per trip
#'   * Mean effort
#'   * Mean CPUE
#' - Gear distribution includes:
#'   * Count of gear types used
#'   * Percentage of each gear type by landing site
#' - Fish distribution includes:
#'   * Total catch by fish category
#'   * Percentage of each fish category in total catch
#'
#' @param log_threshold Logging threshold level (default: logger::DEBUG)
#' @return
#' This function does not return a value. It processes the data and uploads
#' three separate collections to MongoDB:
#' - Monthly catch summaries
#' - Gear distribution statistics
#' - Fish distribution statistics
#'
#' @note
#' This function requires:
#' - A configuration file readable by the 'read_config' function with MongoDB connection details
#' - Access to a 'bmu_size' dataset through the 'get_metadata()' function
#' - The following R packages: dplyr, tidyr, lubridate, purrr, logger
#'
#' @keywords workflow export data-processing
#' @examples
#' \dontrun{
#' export_summaries()
#' }
#'
#' @export
export_summaries <- function(log_threshold = logger::DEBUG) {
  conf <- read_config()

  valid_data <-
    mdb_collection_pull(
      connection_string = conf$storage$mongodb$connection_string,
      collection_name = conf$storage$mongodb$database$pipeline$collection_name$ongoing$validated,
      db_name = conf$storage$mongod$database$pipeline$name
    ) |>
    dplyr::as_tibble()

  bmu_size <-
    get_metadata()$BMUs |>
    dplyr::mutate(size_km = as.numeric(.data$size_km))

  monthly_summaries <-
    valid_data |>
    dplyr::group_by(.data$submission_id) %>%
    dplyr::summarise(dplyr::across(dplyr::everything(), ~ dplyr::first(.x))) %>%
    dplyr::rename(BMU = "landing_site") |>
    dplyr::left_join(bmu_size, by = "BMU") |>
    dplyr::rowwise() |>
    dplyr::mutate(
      effort = .data$no_of_fishers / .data$size_km,
      cpue = .data$total_catch_kg / .data$effort
    ) |>
    dplyr::ungroup() %>%
    dplyr::mutate(date = lubridate::floor_date(.data$landing_date, unit = "month")) |>
    dplyr::select(.data$BMU, .data$date, .data$effort, .data$cpue, .data$total_catch_kg) %>%
    dplyr::group_by(.data$BMU, .data$date) |>
    dplyr::summarise(
      aggregated_catch_kg = sum(.data$total_catch_kg, na.rm = T),
      mean_trip_catch = mean(.data$total_catch_kg, na.rm = T),
      mean_effort = mean(.data$effort, na.rm = T),
      mean_cpue = mean(.data$cpue, na.rm = T)
    ) |>
    dplyr::ungroup()


  gear_distribution <-
    valid_data %>%
    dplyr::group_by(.data$submission_id) %>%
    dplyr::summarise(dplyr::across(dplyr::everything(), ~ dplyr::first(.x))) %>%
    dplyr::group_by(.data$landing_site) %>%
    dplyr::mutate(total_n = dplyr::n()) %>%
    dplyr::group_by(.data$landing_site, .data$gear) %>%
    dplyr::summarise(
      gear_n = dplyr::n(),
      gear_perc = .data$gear_n / dplyr::first(.data$total_n) * 100,
      .groups = "drop"
    ) %>%
    dplyr::ungroup() %>%
    tidyr::complete(.data$landing_site, .data$gear, fill = list(gear_n = 0, gear_perc = 0))


  fish_distribution <-
    valid_data %>%
    dplyr::group_by(.data$landing_site) %>% # First group by landing_site only
    dplyr::mutate(total_catch = sum(.data$catch_kg, na.rm = TRUE)) %>% # Get total catch per landing site
    dplyr::group_by(.data$landing_site, .data$fish_category) %>%
    dplyr::summarise(
      catch_kg = sum(.data$catch_kg, na.rm = TRUE),
      catch_percent = .data$catch_kg / dplyr::first(.data$total_catch) * 100,
      .groups = "drop"
    ) %>%
    dplyr::ungroup() %>%
    tidyr::complete(.data$landing_site, .data$fish_category, fill = list(catch_kg = 0, catch_percent = 0))

  # Dataframes to upload
  dataframes_to_upload <- list(
    monthly_summaries = monthly_summaries,
    gear_distribution = gear_distribution,
    fish_distribution = fish_distribution
  )

  # Collection names
  collection_names <- list(
    monthly_summaries = conf$storage$mongod$database$dashboard$collection_name$ongoing$catch_monthly,
    gear_distribution = conf$storage$mongod$database$dashboard$collection_name$ongoing$gear_distribution,
    fish_distribution = conf$storage$mongod$database$dashboard$collection_name$ongoing$fish_distribution
  )

  # Iterate over the dataframes and upload them
  purrr::walk2(
    .x = dataframes_to_upload,
    .y = collection_names,
    .f = ~ {
      logger::log_info(paste("Uploading", .y, "data to MongoDB"))
      mdb_collection_push(
        data = .x,
        connection_string = conf$storage$mongodb$connection_string,
        collection_name = .y,
        db_name = conf$storage$mongod$database$dashboard$name
      )
    }
  )
}
