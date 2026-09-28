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

#' Restate fork lengths as total lengths
#'
#' @description
#' KEFS records each fish on the length type the enumerator measured: mostly
#' total length, but fork length for tunas, mackerels, jacks and some snappers.
#' Everything downstream reads `length_cm` as total length, and the size views
#' compare it with FishBase lengths at maturity that [coasts::enrich_taxa()]
#' restates as total length. So fork lengths are restated here, per fish and
#' before [summarise_priority_lengths()] averages them, with the same POPLL
#' fits: `TL = intercept + slope * FL`, from [coasts::get_tl_conversions()].
#'
#' @param priority_df Long priority-species data from [reshape_priority_species()].
#' @param taxa_mapping Airtable taxa mapping for the KEFS form, with
#'   `survey_label`, `alpha3_code` and `scientific_name`.
#' @param version FishBase / SeaLifeBase release. Keep it the release
#'   [coasts::enrich_taxa()] reads, so both sides of the size comparison use the
#'   same fits.
#'
#' @return `priority_df`, with converted fish carrying `length_type =
#'   "total_length"`.
#'
#' @details
#' A species with no fork-length fit keeps its fork lengths, and the step logs
#' a warning naming it. Carapace (lobsters, crabs) and mantle (octopus, squid)
#' lengths are left as measured: they are the standard measures for those
#' animals, and none of the lobsters, crabs, octopus or squid KEFS measures has
#' a length at maturity to compare against (2026-09).
#'
#' @keywords preprocessing helper
#' @export
convert_fork_lengths <- function(
  priority_df = NULL,
  taxa_mapping = NULL,
  version = "latest"
) {
  fork_measured <- priority_df$priority_species[
    priority_df$length_type %in% "fork_length"
  ]
  measured <- taxa_mapping |>
    dplyr::filter(.data$survey_label %in% fork_measured) |>
    dplyr::distinct(.data$survey_label, .data$alpha3_code, .data$scientific_name)
  if (nrow(measured) == 0) {
    return(priority_df)
  }

  expanded <- coasts::expand_taxonomic_info(
    dplyr::distinct(measured, .data$alpha3_code, .data$scientific_name),
    version = version
  )
  fl_to_tl <- coasts::get_tl_conversions(expanded, version = version) |>
    dplyr::filter(.data$Type == "FL") |>
    dplyr::inner_join(
      dplyr::distinct(
        expanded,
        .data$alpha3_code,
        scientific_name = .data$original_name,
        .data$SpecCode,
        .data$server
      ),
      by = c("SpecCode", "server")
    ) |>
    # A name that expands to several species takes the median of their fits.
    dplyr::group_by(.data$alpha3_code, .data$scientific_name) |>
    dplyr::summarise(
      intercept = stats::median(.data$intercept),
      slope = stats::median(.data$slope),
      .groups = "drop"
    ) |>
    dplyr::inner_join(measured, by = c("alpha3_code", "scientific_name")) |>
    dplyr::select(priority_species = "survey_label", "intercept", "slope")

  converted <- priority_df |>
    dplyr::left_join(
      fl_to_tl,
      by = "priority_species",
      relationship = "many-to-one"
    ) |>
    dplyr::mutate(
      convert = .data$length_type %in% "fork_length" & !is.na(.data$slope),
      length_cm = dplyr::if_else(
        .data$convert,
        .data$intercept + .data$slope * .data$length_cm,
        .data$length_cm
      ),
      length_type = dplyr::if_else(
        .data$convert,
        "total_length",
        .data$length_type
      )
    )

  unconverted <- converted |>
    dplyr::filter(
      .data$length_type %in% "fork_length",
      !is.na(.data$length_cm)
    ) |>
    dplyr::count(.data$priority_species)
  logger::log_info(
    "Restated {sum(converted$convert & !is.na(converted$length_cm))} fork \\
     lengths as total length"
  )
  if (nrow(unconverted) > 0) {
    logger::log_warn(
      "{sum(unconverted$n)} fork lengths have no total-length fit and stay as \\
       measured: {paste(unconverted$priority_species, collapse = '; ')}"
    )
  }

  dplyr::select(converted, -c("intercept", "slope", "convert"))
}

#' Collapse individual length measurements to the species they belong to
#'
#' @description
#' `PrioritySpeciesCatch` records one row per *measured fish*, while
#' `OverallSampleWeight` records one row per *catch item* (a species in the
#' weighed sample). The two are nested, and the cross-country API schema carries
#' a single `length_cm` per catch row, so the individuals have to be collapsed
#' onto their species before the two can be joined.
#'
#' @param priority_df Long priority-species data from [reshape_priority_species()].
#'
#' @return Tibble with one row per `submission_id` x `priority_species`:
#'   \describe{
#'     \item{submission_id}{Unique identifier for each submission}
#'     \item{priority_species}{Survey label of the measured species}
#'     \item{length_type}{Length convention used (e.g. `total_length`)}
#'     \item{length_cm}{Mean length of the measured individuals}
#'     \item{length_min_cm, length_max_cm}{Range of the measured individuals}
#'     \item{n_measured}{Number of individuals measured for that species}
#'     \item{measured_weight_kg}{Summed weight of those individuals}
#'   }
#'
#' @details
#' `length_cm` is the plain mean across individuals, which -- because Kenya
#' records one row per fish rather than per length bin -- is the same
#' individual-weighted mean the Timor pipeline publishes for the shared API
#' schema. It is a subsample statistic: `measured_weight_kg` is the weight of the
#' fish actually measured and is generally *less* than the species'
#' `sample_weight`, which covers the whole weighed sample.
#'
#' Run [convert_fork_lengths()] first, so the mean is over total lengths.
#'
#' Rows carrying no usable length are dropped, so a species measured only with
#' missing lengths contributes nothing rather than an `NaN` mean.
#'
#' @keywords preprocessing helper
#' @export
summarise_priority_lengths <- function(priority_df = NULL) {
  priority_df |>
    dplyr::filter(
      !is.na(.data$priority_species),
      !is.na(.data$length_cm)
    ) |>
    dplyr::group_by(.data$submission_id, .data$priority_species) |>
    dplyr::summarise(
      length_type = dplyr::first(stats::na.omit(.data$length_type)),
      # Order matters: `summarise()` evaluates sequentially, so the range has to
      # be taken before `length_cm` is replaced by its own mean.
      length_min_cm = min(.data$length_cm),
      length_max_cm = max(.data$length_cm),
      length_cm = mean(.data$length_cm),
      n_measured = dplyr::n(),
      measured_weight_kg = if (all(is.na(.data$priority_weight))) {
        NA_real_
      } else {
        sum(.data$priority_weight, na.rm = TRUE)
      },
      .groups = "drop"
    ) |>
    dplyr::mutate(
      submission_id = as.character(.data$submission_id)
    ) |>
    dplyr::relocate("length_cm", .before = "length_min_cm")
}
