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
    download_parquet_from_cloud(
      prefix = conf$surveys$catch$ongoing$validated$file_prefix,
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    ) |>
    dplyr::filter(.data$landing_date >= "2023-01-01")

  bmu_size <-
    get_metadata()$BMUs |>
    dplyr::mutate(
      size_km = as.numeric(.data$size_km),
      BMU = tolower(.data$BMU)
    )

  valid_data %<>%
    dplyr::filter(
      !.data$landing_site %in% setdiff(valid_data$landing_site, bmu_size$BMU)
    )

  monthly_stats <-
    get_fishery_metrics(
      validated_data = valid_data,
      bmus_size_data = bmu_size
    ) |>
    dplyr::group_by(.data$BMU) %>%
    dplyr::arrange(.data$BMU, dplyr::desc(.data$date)) %>%
    dplyr::slice_head(n = 6) %>%
    dplyr::ungroup()

  # Caluclate fishers day and summarise by month the main metrics
  monthly_summaries <-
    get_fishery_metrics(
      validated_data = valid_data,
      bmus_size_data = bmu_size
    ) |>
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
    tidyr::complete(
      .data$landing_site,
      .data$gear,
      fill = list(gear_n = 0, gear_perc = 0)
    ) %>%
    dplyr::mutate(landing_site = stringr::str_to_title(.data$landing_site))

  fish_distribution <-
    valid_data %>%
    dplyr::mutate(
      date = lubridate::floor_date(.data$landing_date, unit = "month")
    ) %>%
    dplyr::group_by(.data$landing_site, .data$date, .data$fish_category) %>% # First group by landing_site only
    dplyr::summarise(total_catch_kg = sum(.data$catch_kg, na.rm = TRUE)) %>% # Get total catch per landing site
    dplyr::ungroup() %>%
    tidyr::complete(
      .data$landing_site,
      .data$date,
      .data$fish_category,
      fill = list(total_catch_kg = 0)
    ) %>%
    dplyr::mutate(
      landing_site = stringr::str_to_title(.data$landing_site),
      fish_category = stringr::str_to_title(.data$fish_category)
    )

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
    dplyr::filter(!is.na(gear) & !is.na(landing_date)) |>
    dplyr::rename(BMU = "landing_site") |>
    # Group by BMU, gear, and landing_date to get daily metrics
    dplyr::group_by(BMU, gear, landing_date) |>
    dplyr::summarise(
      daily_fishers = sum(no_of_fishers),
      daily_catch_kg = sum(total_catch_kg),
      daily_catch_price = sum(total_catch_price),
      daily_trips = dplyr::n_distinct(submission_id),
      .groups = "keep"
    ) |>
    # Join with BMU size information
    dplyr::left_join(bmu_size, by = "BMU") |>
    # Calculate daily metrics
    dplyr::mutate(
      daily_effort = daily_fishers / size_km, # fishers/km²/day
      daily_cpue = daily_catch_kg / daily_fishers, # kg/fisher/day
      daily_cpua = daily_catch_kg / size_km, # kg/km²/day
      daily_rpue = daily_catch_price / daily_fishers, # KES/fisher/day
      daily_rpua = daily_catch_price / size_km # KES/km²/day
    ) |>
    # Regroup to calculate average metrics across days
    dplyr::group_by(BMU, gear) |>
    dplyr::summarise(
      # Count number of days with data for this gear/BMU
      days_with_data = dplyr::n(),
      total_fishers = sum(daily_fishers),
      # Calculate mean metrics (averaging the daily values)
      mean_effort = mean(daily_effort, na.rm = TRUE),
      mean_cpue = mean(daily_cpue, na.rm = TRUE),
      mean_cpua = mean(daily_cpua, na.rm = TRUE),
      mean_rpue = mean(daily_rpue, na.rm = TRUE),
      mean_rpua = mean(daily_rpua, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::select(-c("days_with_data", "total_fishers")) |>
    dplyr::ungroup() |>
    # Complete the combinations and convert to title case
    tidyr::complete(
      BMU,
      gear,
      fill = list(
        mean_effort = NA,
        mean_cpue = NA,
        mean_cpua = NA,
        mean_rpue = NA,
        mean_rpua = NA
      )
    ) |>
    dplyr::mutate(
      BMU = stringr::str_to_title(BMU)
    )

  # Dataframes to upload
  dataframes_to_upload <- list(
    monthly_stats = monthly_stats,
    monthly_summaries = monthly_summaries,
    fish_distribution = fish_distribution,
    map_distribution = map_distribution,
    gear_summaries = gear_summaries
  )

  # Collection names
  collection_names <- list(
    monthly_stats = conf$storage$mongod$database$dashboard$collection_name$ongoing$monthly_stats,
    monthly_summaries = conf$storage$mongod$database$dashboard$collection_name$ongoing$catch_monthly,
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


#' Calculate Fishery Performance Metrics
#'
#' @description
#' Calculates key fishery performance metrics from validated catch data,
#' including effort, CPUE (Catch Per Unit Effort), CPUA (Catch Per Unit Area),
#' RPUE (Revenue Per Unit Effort), and RPUA (Revenue Per Unit Area).
#'
#' @param validated_data A data frame containing validated fishery data with columns:
#'   \itemize{
#'     \item landing_site: Name of the landing site (will be renamed to BMU)
#'     \item landing_date: Date of landing
#'     \item no_of_fishers: Number of fishers
#'     \item total_catch_kg: Total catch in kilograms
#'     \item total_catch_price: Total catch value in currency
#'   }
#' @param bmus_size_data A data frame containing BMU size information with columns:
#'   \itemize{
#'     \item BMU: Name of the Beach Management Unit (lowercase)
#'     \item size_km: Size of the BMU in square kilometers
#'   }
#'
#' @return A data frame with monthly aggregated metrics by BMU:
#'   \itemize{
#'     \item BMU: Name of the Beach Management Unit (title case)
#'     \item date: First day of the month
#'     \item mean_effort: Average fishers per square kilometer
#'     \item mean_cpue: Average catch (kg) per fisher
#'     \item mean_cpua: Average catch (kg) per square kilometer
#'     \item mean_rpue: Average revenue per fisher
#'     \item mean_rpua: Average revenue per square kilometer
#'   }
#'
#' @details
#' Metrics are calculated as follows:
#' - Effort = Total fishers / BMU size (km²)
#' - CPUE = Total catch (kg) / Total fishers
#' - CPUA = Total catch (kg) / BMU size (km²)
#' - RPUE = Total revenue / Total fishers
#' - RPUA = Total revenue / BMU size (km²)
#'
#' @importFrom dplyr rename select filter distinct left_join group_by summarise ungroup mutate
#' @importFrom stats median
#' @importFrom lubridate floor_date
#' @importFrom stringr str_to_title
#'
#' @keywords helper
get_fishery_metrics <- function(validated_data = NULL, bmus_size_data = NULL) {
  validated_data |>
    dplyr::rename(BMU = "landing_site") |>
    dplyr::select(
      -c(
        "version",
        "catch_id",
        "fishing_ground",
        "lat",
        "lon",
        "fish_category",
        "size",
        "catch_kg",
        "catch_price"
      )
    ) |>
    dplyr::filter(!is.na(.data$landing_date)) %>%
    dplyr::distinct() |>
    dplyr::left_join(bmus_size_data, by = "BMU") |>
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
    dplyr::select("BMU", "date", "effort", "cpue", "cpua", "rpue", "rpua") %>%
    dplyr::group_by(.data$BMU, .data$date) |>
    dplyr::summarise(
      mean_effort = mean(.data$effort, na.rm = T),
      mean_cpue = mean(.data$cpue, na.rm = T),
      mean_cpua = mean(.data$cpua, na.rm = T),
      mean_rpue = mean(.data$rpue, na.rm = T),
      mean_rpua = mean(.data$rpua, na.rm = T)
    ) |>
    dplyr::ungroup()
}
