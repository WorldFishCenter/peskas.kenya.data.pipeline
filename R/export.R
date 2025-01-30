#' Export Summarized Fishery Data for Dashboard Integration
#'
#' @description
#' This function processes and exports validated fishery data by calculating various
#' summary metrics and distributions, which are then uploaded to MongoDB collections
#' for usage in a dashboard.
#'
#' @details
#' The function performs the following operations:
#' 1. **Data Retrieval**: Pulls validated fishery data from the "legacy-validated" MongoDB collection.
#' 2. **Summary Dataset Generation**: Creates the following summary datasets:
#'    - **Monthly Statistics**: Aggregates metrics like catch, effort, and CPUE by BMU (Beach Management Unit) and month.
#'    - **Gear Distribution**: Calculates the percentage usage of each gear type by landing site.
#'    - **Fish Distribution**: Calculates the percentage of each fish category by landing site.
#'    - **Mapping Distribution**: Prepares a dataset of landing sites with geographic coordinates for spatial mapping.
#' 3. **Data Upload**: Uploads each of the summary datasets to its designated MongoDB collection.
#'
#' **Calculated Metrics**:
#' - **Effort** = Number of fishers / Size of BMU in km²
#' - **CPUE** = Total catch in kg / Effort
#' - **Monthly Aggregations**:
#'   * Total catch (kg)
#'   * Mean catch per trip
#'   * Mean effort
#'   * Mean CPUE (Catch Per Unit Effort)
#' - **Gear Distribution**:
#'   * Count of each gear type used
#'   * Percentage distribution of gear types by landing site
#' - **Fish Distribution**:
#'   * Total catch by fish category
#'   * Percentage of each fish category within the total catch
#'
#' @param log_threshold The logging threshold level for monitoring operations (default: `logger::DEBUG`).
#' @return
#' This function does not return a value. It uploads the following collections to MongoDB:
#' - Monthly catch summaries (`monthly_stats` and `monthly_summaries`)
#' - Gear distribution statistics (`gear_distribution`)
#' - Fish distribution statistics (`fish_distribution`)
#' - Landing site mapping data (`map_distribution`)
#'
#' @note
#' **Dependencies**:
#' - Requires a configuration file compatible with the `read_config` function, containing MongoDB connection information.
#' - Access to a `bmu_size` dataset, which provides size details of BMUs, retrieved via the `get_metadata()` function.
#'
#' @keywords workflow export
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
    dplyr::mutate(
      size_km = as.numeric(.data$size_km),
      BMU = tolower(.data$BMU)
    )

  valid_data %<>%
    dplyr::filter(!.data$landing_site %in% setdiff(valid_data$landing_site, bmu_size$BMU))

  monthly_stats <-
    valid_data |>
    dplyr::filter(!is.na(.data$landing_date)) %>%
    dplyr::mutate(date = lubridate::floor_date(.data$landing_date, unit = "month")) %>%
    dplyr::group_by(.data$submission_id) %>%
    dplyr::summarise(
      n_catches = dplyr::n(),
      dplyr::across(dplyr::everything(), ~ dplyr::first(.x))
    ) %>%
    dplyr::group_by(.data$landing_site, .data$date) %>%
    dplyr::summarise(
      tot_submissions = dplyr::n(),
      tot_fishers = sum(.data$no_of_fishers, na.rm = TRUE),
      tot_catches = sum(.data$n_catches, na.rm = TRUE),
      tot_kg = sum(.data$total_catch_kg, na.rm = TRUE)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::group_by(.data$landing_site) %>%
    dplyr::arrange(.data$landing_site, dplyr::desc(.data$date)) %>%
    dplyr::slice_head(n = 6) %>%
    dplyr::ungroup()


  # Caluclate fishers day and summarise by month the main metrics
  monthly_summaries <-
    valid_data |>
    dplyr::rename(BMU = "landing_site") |>
    dplyr::select(-c("version", "catch_id", "fishing_ground", "lat", "lon", "fish_category", "size", "catch_kg", "catch_price")) |>
    dplyr::filter(!is.na(.data$landing_date)) %>%
    dplyr::distinct() |>
    dplyr::left_join(bmu_size, by = "BMU") |>
    dplyr::group_by(.data$landing_date, .data$BMU) |>
    dplyr::summarise(
      total_fishers = sum(.data$no_of_fishers, na.rm = T),
      aggregated_catch_kg = sum(.data$total_catch_kg, na.rm = T),
      aggregated_catch_price = sum(.data$total_catch_price, na.rm = T),
      size_km = dplyr::first(.data$size_km),
      mean_trip_catch = stats::median(.data$total_catch_kg, na.rm = T)
    ) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      effort = .data$total_fishers / .data$size_km,
      cpue = .data$aggregated_catch_kg / .data$total_fishers,
      cpua = .data$aggregated_catch_kg / .data$size_km,
      rpue = .data$aggregated_catch_price / .data$total_fishers,
      rpua = .data$aggregated_catch_price / .data$size_km
    ) |>
    dplyr::mutate(
      date = lubridate::floor_date(.data$landing_date, unit = "month"),
      BMU = stringr::str_to_title(.data$BMU)
    ) |>
    dplyr::select("BMU", "date", "mean_trip_catch", "effort", "aggregated_catch_kg", "cpue", "cpua", "rpue", "rpua") %>%
    dplyr::group_by(.data$BMU, .data$date) |>
    dplyr::summarise(
      aggregated_catch_kg = sum(.data$aggregated_catch_kg, na.rm = T),
      mean_trip_catch = mean(.data$mean_trip_catch, na.rm = T),
      mean_effort = mean(.data$effort, na.rm = T),
      mean_cpue = mean(.data$cpue, na.rm = T),
      mean_cpua = mean(.data$cpua, na.rm = T),
      mean_rpue = mean(.data$rpue, na.rm = T),
      mean_rpua = mean(.data$rpua, na.rm = T)
    ) |>
    dplyr::ungroup() %>%
    tidyr::complete(
      .data$BMU,
      date = seq(min(.data$date), max(.data$date), by = "month"),
      fill = list(
        aggregated_catch_kg = NA,
        mean_trip_catch = NA,
        mean_effort = NA,
        mean_cpue = NA,
        mean_cpua = NA,
        mean_rpue = NA,
        mean_rpua = NA
      )
    )

  # Calculate gear usage percent
  gear_distribution <-
    valid_data %>%
    dplyr::group_by(.data$submission_id) %>%
    dplyr::summarise(dplyr::across(dplyr::everything(), ~ dplyr::first(.x))) %>%
    dplyr::filter(!is.na(.data$gear)) %>%
    dplyr::group_by(.data$landing_site) %>%
    dplyr::mutate(total_n = dplyr::n()) %>%
    dplyr::group_by(.data$landing_site, .data$gear) %>%
    dplyr::summarise(
      gear_n = dplyr::n(),
      gear_perc = .data$gear_n / dplyr::first(.data$total_n) * 100,
      .groups = "drop"
    ) %>%
    dplyr::ungroup() %>%
    tidyr::complete(.data$landing_site, .data$gear, fill = list(gear_n = 0, gear_perc = 0)) %>%
    dplyr::mutate(landing_site = stringr::str_to_title(.data$landing_site))


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
    tidyr::complete(.data$landing_site, .data$fish_category, fill = list(catch_kg = 0, catch_percent = 0)) %>%
    dplyr::mutate(landing_site = stringr::str_to_title(.data$landing_site))

  map_distribution <-
    valid_data %>%
    dplyr::select("submission_id", "landing_site", "lat", "lon") %>%
    dplyr::group_by(.data$submission_id) %>%
    dplyr::summarise(dplyr::across(dplyr::everything(), ~ dplyr::first(.x))) %>%
    dplyr::ungroup() %>%
    dplyr::select(-"submission_id") %>%
    dplyr::filter(!is.na(.data$lat) & !is.na(.data$landing_site)) %>%
    dplyr::mutate(landing_site = stringr::str_to_title(.data$landing_site))


  gear_summaries <-
    valid_data |>
    dplyr::filter(!is.na(.data$gear) & !is.na(.data$landing_date)) %>%
    dplyr::rename(BMU = "landing_site") |>
    dplyr::select(-c("version", "catch_id", "fishing_ground", "lat", "lon", "fish_category", "size", "catch_kg")) |>
    dplyr::distinct() |>
    dplyr::left_join(bmu_size, by = "BMU") |>
    dplyr::group_by(.data$BMU, .data$gear) |>
    dplyr::summarise(
      total_fishers = sum(.data$no_of_fishers),
      aggregated_catch_kg = sum(.data$total_catch_kg),
      total_trips = dplyr::n_distinct(.data$submission_id),
      size_km = dplyr::first(.data$size_km),
      unique_days = dplyr::n_distinct(.data$landing_date),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      mean_trip_catch = .data$aggregated_catch_kg / .data$total_trips,
      mean_effort = .data$total_fishers / (.data$size_km * .data$unique_days), # Fishers per km² per day
      mean_cpue = .data$aggregated_catch_kg / (.data$total_fishers * .data$unique_days), # kg per fisher per day
      mean_cpua = .data$aggregated_catch_kg / (.data$size_km * .data$unique_days) # kg per km² per day
    ) |>
    dplyr::select("BMU", "gear", "mean_trip_catch", "mean_effort", "mean_cpue", "mean_cpua") |>
    tidyr::complete(
      .data$BMU,
      .data$gear,
      fill = list(
        mean_trip_catch = NA,
        mean_effort = NA,
        mean_cpue = NA,
        mean_cpua = NA
      )
    ) |>
    dplyr::mutate(
      BMU = stringr::str_to_title(.data$BMU)
    )


  # Dataframes to upload
  dataframes_to_upload <- list(
    monthly_stats = monthly_stats,
    monthly_summaries = monthly_summaries,
    gear_distribution = gear_distribution,
    fish_distribution = fish_distribution,
    map_distribution = map_distribution,
    gear_summaries = gear_summaries
  )

  # Collection names
  collection_names <- list(
    monthly_stats = conf$storage$mongod$database$dashboard$collection_name$ongoing$monthly_stats,
    monthly_summaries = conf$storage$mongod$database$dashboard$collection_name$ongoing$catch_monthly,
    gear_distribution = conf$storage$mongod$database$dashboard$collection_name$ongoing$gear_distribution,
    fish_distribution = conf$storage$mongod$database$dashboard$collection_name$ongoing$fish_distribution,
    map_distribution = conf$storage$mongod$database$dashboard$collection_name$ongoing$map_distribution,
    gear_summaries = conf$storage$mongod$database$dashboard$collection_name$ongoing$gear_summaries
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
