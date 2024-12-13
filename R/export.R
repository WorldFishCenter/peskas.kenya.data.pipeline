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

  monthly_summaries <-
    valid_data |>
    dplyr::filter(!is.na(.data$landing_date)) %>%
    dplyr::group_by(.data$submission_id) %>%
    dplyr::summarise(dplyr::across(dplyr::everything(), ~ dplyr::first(.x))) %>%
    dplyr::rename(BMU = "landing_site") |>
    dplyr::left_join(bmu_size, by = "BMU") |>
    dplyr::rowwise() |>
    dplyr::mutate(
      effort = .data$no_of_fishers / .data$size_km,
      cpue = .data$total_catch_kg / .data$no_of_fishers,
      cpua = .data$total_catch_kg / .data$size_km
    ) |>
    dplyr::ungroup() %>%
    dplyr::mutate(
      date = lubridate::floor_date(.data$landing_date, unit = "month"),
      BMU = stringr::str_to_title(.data$BMU)
    ) |>
    dplyr::select("BMU", "date", "effort", "total_catch_kg", "cpue", "cpua") %>%
    dplyr::group_by(.data$BMU, .data$date) |>
    dplyr::summarise(
      aggregated_catch_kg = sum(.data$total_catch_kg, na.rm = T),
      mean_trip_catch = mean(.data$total_catch_kg, na.rm = T),
      mean_effort = mean(.data$effort, na.rm = T),
      mean_cpue = mean(.data$cpue, na.rm = T),
      mean_cpua = mean(.data$cpua, na.rm = T)
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
        mean_cpua = NA
      )
    )


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
    dplyr::filter(!is.na(.data$gear)) %>%
    dplyr::group_by(.data$submission_id) %>%
    dplyr::summarise(dplyr::across(dplyr::everything(), ~ dplyr::first(.x))) %>%
    dplyr::rename(BMU = "landing_site") |>
    dplyr::left_join(bmu_size, by = "BMU") |>
    dplyr::rowwise() |>
    dplyr::mutate(
      effort = .data$no_of_fishers / .data$size_km,
      cpue = .data$total_catch_kg / .data$no_of_fishers,
      cpua = .data$total_catch_kg / .data$size_km
    ) |>
    dplyr::ungroup() %>%
    dplyr::mutate(
      BMU = stringr::str_to_title(.data$BMU)
    ) |>
    dplyr::select("BMU", "gear", "effort", "total_catch_kg", "cpue", "cpua") %>%
    dplyr::group_by(.data$BMU, .data$gear) |>
    dplyr::summarise(
      mean_trip_catch = mean(.data$total_catch_kg, na.rm = T),
      mean_effort = mean(.data$effort, na.rm = T),
      mean_cpue = mean(.data$cpue, na.rm = T),
      mean_cpua = mean(.data$cpua, na.rm = T)
    ) |>
    dplyr::ungroup() |> 
    dplyr::mutate(dplyr::across(is.numeric, ~ ifelse(is.infinite(.x), NA_real_, .x))) %>%
  tidyr::complete(
    .data$BMU,
    .data$gear,
    fill = list(
      mean_trip_catch = NA,
      mean_effort = NA,
      mean_cpue = NA,
      mean_cpua = NA
    ))



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
