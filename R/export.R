#' Export Summarized Fishery Data
#'
#' @description
#' This function reshapes validated legacy fishery data, calculates summary metrics
#' such as effort and CPUE (Catch Per Unit Effort), and uploads the processed data
#' to MongoDB for dashboard usage.
#'
#' @details
#' The function performs the following operations:
#' 1. Pulls validated data from the "legacy-validated" MongoDB collection.
#' 2. Reshapes the data by grouping by survey_id and summarizing catch and price.
#' 3. Joins with BMU (Beach Management Unit) size data.
#' 4. Calculates effort and CPUE.
#' 5. Selects relevant columns for the final dataset.
#' 6. Uploads the processed data to a MongoDB collection.
#'
#' Effort and CPUE are calculated as follows:
#' - Effort = Number of fishers / Size of BMU in km
#' - CPUE = Total catch in kg / Effort
#'
#' @return
#' This function does not return a value. It processes the data and uploads
#' the result to MongoDB.
#'
#' @note
#' This function requires a configuration file to be present and readable by the
#' 'read_config' function, which should provide MongoDB connection details.
#' It also assumes the existence of a 'bmu_size' dataset for joining.
#'
#' @keywords workflow export
#' @examples
#' \dontrun{
#' export_summaries()
#' }
#'
#' @export
export_summaries <- function() {
  conf <- read_config()

  valid_data <-
    mdb_collection_pull(
      connection_string = conf$storage$mongodb$connection_string,
      collection_name = conf$storage$mongod$database$pipeline$collection_name$legacy$validated,
      db_name = conf$storage$mongod$database$pipeline$name
    ) |>
    dplyr::as_tibble()

  bmu_size <-
    get_metadata()$fishing_grounds |>
    dplyr::mutate(size_km = as.numeric(.data$size_km))

  summaries <-
    valid_data |>
    dplyr::rename(BMU = "landing_site") |>
    dplyr::group_by(.data$survey_id) |>
    dplyr::summarise(dplyr::across(c("BMU":"ecology"), ~ dplyr::first(.x)),
      catch_kg = sum(.data$catch_kg, na.rm = T),
      price = mean(.data$price, na.rm = T)
    ) |>
    dplyr::ungroup() |>
    dplyr::left_join(bmu_size, by = "BMU") |>
    dplyr::rowwise() |>
    dplyr::mutate(
      effort = .data$no_of_fishers / .data$size_km,
      cpue = .data$catch_kg / .data$effort
    ) |>
    dplyr::ungroup() |>
    dplyr::select(
      "survey_id", "BMU", "landing_date",
      "gear", "catch_kg",
      "price", "effort", "cpue"
    ) |>
    dplyr::group_by(.data$BMU, .data$landing_date) |>
    dplyr::arrange(.by_group = T) |>
    dplyr::ungroup()

  monthly_summaries <-
    summaries |>
    dplyr::mutate(date = lubridate::floor_date(.data$landing_date, unit = "month")) |>
    dplyr::group_by(.data$BMU, .data$date) |>
    dplyr::summarise(
      mean_trip_catch = mean(.data$catch_kg, na.rm = T),
      mean_trip_price = mean(.data$price, na.rm = T),
      mean_effort = mean(.data$effort, na.rm = T),
      mean_cpue = mean(.data$cpue, na.rm = T)
    ) |>
    dplyr::ungroup()

  # Dataframes to upload
  dataframes_to_upload <- list(
    summaries = summaries,
    monthly_summaries = monthly_summaries
  )

  # Collection names
  collection_names <- list(
    summaries = conf$storage$mongod$database$dashboard$collection_name$legacy$fishery_metrics,
    monthly_summaries = conf$storage$mongod$database$dashboard$collection_name$legacy$fishery_metrics_monthly
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
