#' Reshape catch details from wide to long format
#'
#' Transforms catch data from a wide format (multiple columns per catch)
#' to a long format (one row per catch per submission). This function is
#' designed to work with KoBo survey data containing multiple catch details.
#'
#' @param raw_data A data frame containing submission_id and CATCH_DETAILS columns
#'   in wide format. The CATCH_DETAILS columns should follow the naming pattern
#'   CATCH_DETAILS.N.CATCH_DETAILS/variable where N is the catch number (0-based).
#'
#' @return A data frame in long format with the following columns:
#'   \describe{
#'     \item{submission_id}{Unique identifier for each submission}
#'     \item{n_catch}{Catch number (1-based indexing)}
#'     \item{species}{Marine species caught}
#'     \item{total_catch_weight}{Weight of the catch (numeric)}
#'     \item{price_per_kg}{Price per kilogram (numeric)}
#'     \item{total_value}{Total value of the catch (numeric)}
#'   }
#'
#' @examples
#' \dontrun{
#' # Load your raw KoBo survey data
#' raw_data <- read.csv("kobo_survey_data.csv")
#'
#' # Reshape to long format
#' long_data <- reshape_catch_data_v1(raw_data)
#'
#' # View the reshaped data
#' head(long_data)
#' }
#' @keywords preprocessing
#' @export
reshape_catch_data_v1 <- function(raw_data = NULL) {
  data <-
    raw_data |>
    dplyr::select("submission_id", dplyr::contains("CATCH_DETAILS"))

  # Extract all catch detail columns
  catch_cols <- names(data)[grepl("CATCH_DETAILS", names(data))]

  # Get the maximum catch number (0-based indexing in your data)
  max_catch <- max(
    as.numeric(stringr::str_extract(catch_cols, "\\d+")),
    na.rm = TRUE
  )

  # Create empty list to store reshaped data
  long_data_list <- list()

  # Loop through each catch number
  for (i in 0:max_catch) {
    # Select columns for this catch number
    current_catch_cols <- catch_cols[grepl(
      paste0("CATCH_DETAILS\\.", i, "\\."),
      catch_cols
    )]

    if (length(current_catch_cols) > 0) {
      # Extract data for this catch
      current_data <- data |>
        dplyr::select("submission_id", dplyr::all_of(current_catch_cols))

      # Rename columns to remove the prefix
      names(current_data) <- c(
        "submission_id",
        "species",
        "total_catch_weight",
        "price_per_kg",
        "total_value"
      )

      # Add catch number
      current_data$n_catch <- i + 1 # Convert to 1-based indexing

      # Filter out rows where all catch details are NA
      current_data <- current_data |>
        dplyr::filter(
          !is.na(.data$species) |
            !is.na(.data$total_catch_weight) |
            !is.na(.data$price_per_kg) |
            !is.na(.data$total_value)
        )

      # Add to list
      long_data_list[[length(long_data_list) + 1]] <- current_data
    }
  }

  # Combine all catches into one dataframe
  long_data <- dplyr::bind_rows(long_data_list)

  # Reorder columns for clarity
  long_data <- long_data |>
    dplyr::select(
      "submission_id",
      "n_catch",
      catch_taxon = "species",
      "total_catch_weight",
      "price_per_kg",
      "total_value"
    )

  # Convert numeric columns from character to numeric
  long_data <- long_data |>
    dplyr::mutate(
      total_catch_weight = as.numeric(.data$total_catch_weight),
      price_per_kg = as.numeric(.data$price_per_kg),
      total_value = as.numeric(.data$total_value)
    )

  return(long_data)
}

#' Reshape Priority Species Catch Data from Wide to Long Format
#'
#' @description
#' Transforms priority species catch data from wide to long format. Extracts columns containing
#' "PrioritySpeciesCatch", reshapes them into rows, and converts to numeric types.
#'
#' @param raw_data Data frame with priority species columns following pattern `PrioritySpeciesCatch.{i}.{field}`.
#'
#' @return Tibble with columns: submission_id, n_priority, priority_species, length_type, length_cm, weight_priority.
#'
#' @details
#' Iterates through priority species numbers (0-based in raw data, 1-based in output), reshapes each
#' group to standardized column names, filters out incomplete records, and combines into long format.
#'
#' @keywords preprocessing helper
#' @export
reshape_priority_species <- function(raw_data = NULL) {
  data <-
    raw_data |>
    dplyr::select("submission_id", dplyr::contains("PrioritySpeciesCatch"))

  # Extract all priority species columns
  priority_cols <- names(data)[grepl("PrioritySpeciesCatch", names(data))]

  # Get the maximum catch number (0-based indexing)
  max_priority <- max(
    as.numeric(stringr::str_extract(priority_cols, "\\d+")),
    na.rm = TRUE
  )

  # Create empty list to store reshaped data
  long_data_list <- list()

  # Loop through each priority species number
  for (i in 0:max_priority) {
    # Select columns for this priority number
    current_priority_cols <- priority_cols[grepl(
      paste0("PrioritySpeciesCatch\\.", i, "\\."),
      priority_cols
    )]

    if (length(current_priority_cols) > 0) {
      # Extract data for this priority species
      current_data <- data |>
        dplyr::select("submission_id", dplyr::all_of(current_priority_cols))

      # Rename columns to remove the prefix
      names(current_data) <- c(
        "submission_id",
        "priority_species",
        "length_type",
        "length_cm",
        "weight_kg"
      )

      # Add priority number (convert from 0-based to 1-based indexing)
      current_data$n_priority <- i + 1

      # Filter out rows where all priority details are NA
      current_data <- current_data |>
        dplyr::filter(
          !is.na(.data$priority_species) |
            !is.na(.data$length_type) |
            !is.na(.data$length_cm) |
            !is.na(.data$weight_kg)
        )

      # Add to list
      long_data_list[[length(long_data_list) + 1]] <- current_data
    }
  }

  # Combine all priority species into one dataframe
  long_data <- dplyr::bind_rows(long_data_list)

  # Reorder columns for clarity
  long_data <- long_data |>
    dplyr::select(
      "submission_id",
      "n_priority",
      "priority_species",
      "length_type",
      "length_cm",
      priority_weight = "weight_kg"
    )

  # Convert numeric columns from character to numeric
  long_data <- long_data |>
    dplyr::mutate(
      length_cm = as.numeric(.data$length_cm),
      priority_weight = as.numeric(.data$priority_weight)
    )

  return(long_data)
}

#' Reshape Overall Sample Weight Data from Wide to Long Format
#'
#' @description
#' Transforms overall sample weight data from wide to long format. Extracts columns containing
#' "OverallSampleWeight" (excluding calculation columns), reshapes them into rows, and converts to numeric types.
#'
#' @param raw_data Data frame with sample weight columns following pattern `OverallSampleWeight.{i}.{field}`.
#'
#' @return Tibble with columns: submission_id, n_sample, sample_species, weight_sample, price_sample.
#'
#' @details
#' Iterates through sample numbers (0-based in raw data, 1-based in output), reshapes each
#' group to standardized column names, filters out incomplete records, and combines into long format.
#'
#' @keywords preprocessing helper
#' @export
reshape_overall_sample <- function(raw_data = NULL) {
  data <-
    raw_data |>
    dplyr::select("submission_id", dplyr::contains("OverallSampleWeight")) |>
    dplyr::select(-dplyr::ends_with("calculation"))

  # Extract all overall sample columns
  sample_cols <- names(data)[grepl("OverallSampleWeight", names(data))]

  # Get the maximum sample number (0-based indexing)
  max_sample <- max(
    as.numeric(stringr::str_extract(sample_cols, "\\d+")),
    na.rm = TRUE
  )

  # Create empty list to store reshaped data
  long_data_list <- list()

  # Loop through each sample number
  for (i in 0:max_sample) {
    # Select columns for this sample number
    current_sample_cols <- sample_cols[grepl(
      paste0("OverallSampleWeight\\.", i, "\\."),
      sample_cols
    )]

    if (length(current_sample_cols) > 0) {
      # Extract data for this sample
      current_data <- data |>
        dplyr::select("submission_id", dplyr::all_of(current_sample_cols))

      # Rename columns to remove the prefix
      names(current_data) <- c(
        "submission_id",
        "species",
        "weight_sample",
        "price_sample"
      )

      # Add sample number
      current_data$n_sample <- i + 1 # Convert to 1-based indexing

      # Filter out rows where all sample details are NA
      current_data <- current_data |>
        dplyr::filter(
          !is.na(.data$species) |
            !is.na(.data$weight_sample) |
            !is.na(.data$price_sample)
        )

      # Add to list
      long_data_list[[length(long_data_list) + 1]] <- current_data
    }
  }

  # Combine all samples into one dataframe
  long_data <- dplyr::bind_rows(long_data_list)

  # Reorder columns for clarity
  long_data <- long_data |>
    dplyr::select(
      "submission_id",
      "n_sample",
      sample_species = "species",
      sample_weight = "weight_sample",
      sample_price = "price_sample"
    )

  # Convert numeric columns from character to numeric
  long_data <- long_data |>
    dplyr::mutate(
      sample_weight = as.numeric(.data$sample_weight),
      sample_price = as.numeric(.data$sample_price)
    )

  return(long_data)
}
