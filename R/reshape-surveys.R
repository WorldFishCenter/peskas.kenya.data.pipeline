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

#' Coalesce ABALOBI localised-name columns into a single string
#'
#' ABALOBI localised names are stored as a map of language/locale codes to
#' strings, which flattens to several columns sharing a common prefix (e.g.
#' `monitoringSite.name.ke-en`, `monitoringSite.name.ke-sw`). This helper
#' collapses those columns row-wise into a single character vector, preferring
#' the language code that matches `prefer` (English by default).
#'
#' @param data A data frame containing the flattened ABALOBI columns.
#' @param field Character string. The shared prefix of the localised-name
#'   columns, without the trailing language code (e.g. `"monitoringSite.name"`).
#' @param prefer Character string matched (case-insensitively) against the
#'   column names to decide which language takes precedence. Defaults to
#'   `"en"`.
#'
#' @return A character vector, one value per row of `data`, or all `NA` if no
#'   matching column is present.
#'
#' @keywords preprocessing helper internal
coalesce_abalobi_localised <- function(data, field, prefer = "en") {
  cols <- names(data)[startsWith(names(data), paste0(field, "."))]
  if (length(cols) == 0) {
    return(rep(NA_character_, nrow(data)))
  }
  preferred <- cols[grepl(prefer, cols, ignore.case = TRUE)]
  ordered <- c(preferred, setdiff(cols, preferred))
  args <- lapply(ordered, function(col) as.character(data[[col]]))
  do.call(dplyr::coalesce, args)
}

#' Row-wise collapse of several columns into a single delimited string
#'
#' Internal helper used to fold the repeated columns produced when an ABALOBI
#' array is flattened (e.g. all gear options for a species) into one
#' comma-separated string of the distinct, non-missing values found in each row.
#'
#' @param data A data frame.
#' @param cols Character vector of column names to collapse.
#'
#' @return A character vector, one value per row of `data`.
#'
#' @keywords preprocessing helper internal
collapse_row_values <- function(data, cols) {
  if (length(cols) == 0 || nrow(data) == 0) {
    return(rep(NA_character_, nrow(data)))
  }
  mat <- vapply(
    cols,
    function(col) as.character(data[[col]]),
    character(nrow(data))
  )
  if (is.null(dim(mat))) mat <- matrix(mat, ncol = length(cols))
  apply(mat, 1, function(vals) {
    vals <- vals[!is.na(vals) & nzchar(vals)]
    if (length(vals) == 0) NA_character_ else paste(unique(vals), collapse = ", ")
  })
}

#' Row-wise sum of several numeric columns
#'
#' Internal helper that sums the non-missing values held across the supplied
#' columns for each row, returning `NA` where a row has no value at all (as
#' opposed to `0`).
#'
#' @param data A data frame.
#' @param cols Character vector of column names to sum.
#'
#' @return A numeric vector, one value per row of `data`.
#'
#' @keywords preprocessing helper internal
sum_row_values <- function(data, cols) {
  if (length(cols) == 0 || nrow(data) == 0) {
    return(rep(NA_real_, nrow(data)))
  }
  mat <- vapply(
    cols,
    function(col) as.numeric(data[[col]]),
    numeric(nrow(data))
  )
  if (is.null(dim(mat))) mat <- matrix(mat, ncol = length(cols))
  present <- rowSums(!is.na(mat))
  totals <- rowSums(mat, na.rm = TRUE)
  ifelse(present == 0, NA_real_, totals)
}

#' Reshape ABALOBI catch details from wide to long format
#'
#' Transforms the flattened ABALOBI `catchList` (one block of columns per
#' species, i.e. `catchList.N.field`) into a long format with one row per
#' species per activity submission. Gear options are collapsed into a single
#' string and biological samples are summarised (count of samples and total
#' individuals sampled); the full per-individual sample detail is available via
#' [reshape_abalobi_samples()].
#'
#' @param raw_data A data frame containing `submission_id` and flattened
#'   `catchList` columns, as produced by [ingest_abalobi_activities()].
#'
#' @return A tibble in long format with the following columns:
#'   \describe{
#'     \item{submission_id}{Unique identifier for each activity submission.}
#'     \item{n_catch}{Catch number (1-based).}
#'     \item{catch_id}{ABALOBI species identifier (concatenated scientific name).}
#'     \item{catch_taxon}{Localised (English-preferred) common name of the species.}
#'     \item{fao_code}{FAO 3-alpha species code (ASFIS).}
#'     \item{scientific_name}{Latin binomial name.}
#'     \item{family_name}{Taxonomic family.}
#'     \item{total_weight}{Recorded weight of the species in the catch.}
#'     \item{weight_type}{Units of \code{total_weight} (\code{kg} or \code{g}).}
#'     \item{total_catch_weight}{\code{total_weight} normalised to kilograms.}
#'     \item{count}{Number of individuals of the species.}
#'     \item{gear}{Gear option name(s) used to catch the species.}
#'     \item{gear_fao_code}{FAO gear code(s) for the gear option(s).}
#'     \item{is_other_gear}{Whether an 'Other' gear type was selected.}
#'     \item{other_gear}{Free-text description of the 'Other' gear.}
#'     \item{n_samples}{Number of biological samples taken for the species.}
#'     \item{sampled_count}{Total number of individuals across all samples.}
#'   }
#'
#' @keywords preprocessing helper
#' @export
reshape_abalobi_catch <- function(raw_data = NULL) {
  empty <- tibble::tibble(
    submission_id = character(),
    n_catch = integer(),
    catch_id = character(),
    catch_taxon = character(),
    fao_code = character(),
    scientific_name = character(),
    family_name = character(),
    total_weight = numeric(),
    weight_type = character(),
    total_catch_weight = numeric(),
    count = integer(),
    gear = character(),
    gear_fao_code = character(),
    is_other_gear = logical(),
    other_gear = character(),
    n_samples = integer(),
    sampled_count = numeric()
  )

  catch_cols <- names(raw_data)[grepl("^catchList\\.", names(raw_data))]
  if (length(catch_cols) == 0 || nrow(raw_data) == 0) {
    return(empty)
  }

  # Top-level catch indices only (ignore nested gearOptions/samples indices).
  max_catch <- max(
    as.integer(stringr::str_match(catch_cols, "^catchList\\.(\\d+)\\.")[, 2]),
    na.rm = TRUE
  )

  long_data_list <- list()

  for (i in 0:max_catch) {
    prefix <- paste0("catchList.", i, ".")
    gv <- function(suffix) {
      col <- paste0(prefix, suffix)
      if (col %in% names(raw_data)) raw_data[[col]] else NA
    }

    # Collapse the (possibly multiple) gear options, preferring English names.
    gear_name_cols <- names(raw_data)[grepl(
      paste0("^catchList\\.", i, "\\.gearOptions\\.\\d+\\.name\\."),
      names(raw_data)
    )]
    en_gear <- gear_name_cols[grepl("en", gear_name_cols, ignore.case = TRUE)]
    gear_cols <- if (length(en_gear) > 0) en_gear else gear_name_cols
    gear_fao_cols <- names(raw_data)[grepl(
      paste0("^catchList\\.", i, "\\.gearOptions\\.\\d+\\.faoCode$"),
      names(raw_data)
    )]

    # Summarise the biological samples attached to this species.
    sample_id_cols <- names(raw_data)[grepl(
      paste0("^catchList\\.", i, "\\.samples\\.\\d+\\.id$"),
      names(raw_data)
    )]
    sample_qty_cols <- names(raw_data)[grepl(
      paste0("^catchList\\.", i, "\\.samples\\.\\d+\\.quantity$"),
      names(raw_data)
    )]

    current_data <- tibble::tibble(
      submission_id = as.character(raw_data$submission_id),
      n_catch = i + 1L,
      catch_id = as.character(gv("id")),
      catch_taxon = coalesce_abalobi_localised(
        raw_data,
        paste0("catchList.", i, ".name")
      ),
      fao_code = as.character(gv("faoCode")),
      scientific_name = as.character(gv("scientificName")),
      family_name = as.character(gv("familyName")),
      total_weight = as.numeric(gv("totalWeight")),
      weight_type = as.character(gv("weightType")),
      count = as.integer(gv("count")),
      gear = collapse_row_values(raw_data, gear_cols),
      gear_fao_code = collapse_row_values(raw_data, gear_fao_cols),
      is_other_gear = as.logical(gv("isOtherGearSelected")),
      other_gear = as.character(gv("othergear")),
      n_samples = if (length(sample_id_cols) == 0) {
        0L
      } else {
        as.integer(rowSums(!is.na(raw_data[sample_id_cols])))
      },
      sampled_count = sum_row_values(raw_data, sample_qty_cols)
    )

    # Drop empty catch slots (submissions with fewer species than the maximum).
    current_data <- current_data |>
      dplyr::filter(
        !is.na(.data$catch_id) |
          !is.na(.data$scientific_name) |
          !is.na(.data$total_weight)
      )

    long_data_list[[length(long_data_list) + 1]] <- current_data
  }

  long_data <- dplyr::bind_rows(long_data_list)
  if (nrow(long_data) == 0) {
    return(empty)
  }

  # Normalise weights to kilograms for consistency with the rest of the package.
  long_data |>
    dplyr::mutate(
      total_catch_weight = dplyr::if_else(
        !is.na(.data$weight_type) & .data$weight_type == "g",
        .data$total_weight / 1000,
        .data$total_weight
      )
    ) |>
    dplyr::relocate("total_catch_weight", .after = "weight_type")
}

#' Reshape ABALOBI biological samples from wide to long format
#'
#' Transforms the flattened, doubly nested ABALOBI biological samples
#' (`catchList.N.samples.M.field`) into a long format with one row per
#' individual sample, carrying the parent species context for each measurement.
#'
#' @param raw_data A data frame containing `submission_id` and flattened
#'   `catchList` columns, as produced by [ingest_abalobi_activities()].
#'
#' @return A tibble in long format with the following columns:
#'   \describe{
#'     \item{submission_id}{Unique identifier for each activity submission.}
#'     \item{n_catch}{Catch number of the parent species (1-based).}
#'     \item{scientific_name}{Latin binomial name of the sampled species.}
#'     \item{catch_taxon}{Localised (English-preferred) common name.}
#'     \item{n_sample}{Sample number within the species (1-based).}
#'     \item{sample_id}{Unique sample identifier.}
#'     \item{quantity}{Number of individuals in the sample.}
#'     \item{weight}{Total weight of the sample.}
#'     \item{weight_type}{Units of \code{weight} (\code{kg} or \code{g}).}
#'     \item{length_sl, length_tl, length_fl}{Standard, total and fork length.}
#'     \item{length_type_sl, length_type_tl, length_type_fl}{Units of the lengths.}
#'     \item{sex}{Sex of the individual (single-item samples only).}
#'     \item{form}{Form of the individual, e.g. whole or cleaned.}
#'   }
#'
#' @keywords preprocessing helper
#' @export
reshape_abalobi_samples <- function(raw_data = NULL) {
  empty <- tibble::tibble(
    submission_id = character(),
    n_catch = integer(),
    scientific_name = character(),
    catch_taxon = character(),
    n_sample = integer(),
    sample_id = character(),
    quantity = integer(),
    weight = numeric(),
    weight_type = character(),
    length_sl = numeric(),
    length_type_sl = character(),
    length_tl = numeric(),
    length_type_tl = character(),
    length_fl = numeric(),
    length_type_fl = character(),
    sex = character(),
    form = character()
  )

  sample_cols <- names(raw_data)[grepl(
    "^catchList\\.\\d+\\.samples\\.\\d+\\.",
    names(raw_data)
  )]
  if (length(sample_cols) == 0 || nrow(raw_data) == 0) {
    return(empty)
  }

  max_catch <- max(
    as.integer(stringr::str_match(sample_cols, "^catchList\\.(\\d+)\\.")[, 2]),
    na.rm = TRUE
  )

  long_data_list <- list()

  for (i in 0:max_catch) {
    catch_sample_cols <- sample_cols[grepl(
      paste0("^catchList\\.", i, "\\.samples\\.\\d+\\."),
      sample_cols
    )]
    if (length(catch_sample_cols) == 0) next

    max_sample <- max(
      as.integer(stringr::str_match(
        catch_sample_cols,
        paste0("^catchList\\.", i, "\\.samples\\.(\\d+)\\.")
      )[, 2]),
      na.rm = TRUE
    )

    scientific_name <- {
      col <- paste0("catchList.", i, ".scientificName")
      if (col %in% names(raw_data)) {
        as.character(raw_data[[col]])
      } else {
        rep(NA_character_, nrow(raw_data))
      }
    }
    catch_taxon <- coalesce_abalobi_localised(
      raw_data,
      paste0("catchList.", i, ".name")
    )

    for (j in 0:max_sample) {
      prefix <- paste0("catchList.", i, ".samples.", j, ".")
      gv <- function(suffix) {
        col <- paste0(prefix, suffix)
        if (col %in% names(raw_data)) raw_data[[col]] else NA
      }

      current_data <- tibble::tibble(
        submission_id = as.character(raw_data$submission_id),
        n_catch = i + 1L,
        scientific_name = scientific_name,
        catch_taxon = catch_taxon,
        n_sample = j + 1L,
        sample_id = as.character(gv("id")),
        quantity = as.integer(gv("quantity")),
        weight = as.numeric(gv("weight")),
        weight_type = as.character(gv("weightType")),
        length_sl = as.numeric(gv("lengthSL")),
        length_type_sl = as.character(gv("lengthTypeSL")),
        length_tl = as.numeric(gv("lengthTL")),
        length_type_tl = as.character(gv("lengthTypeTL")),
        length_fl = as.numeric(gv("lengthFL")),
        length_type_fl = as.character(gv("lengthTypeFL")),
        sex = as.character(gv("sex")),
        form = as.character(gv("form"))
      )

      # Keep only populated sample slots.
      current_data <- current_data |>
        dplyr::filter(
          !is.na(.data$sample_id) |
            !is.na(.data$weight) |
            !is.na(.data$quantity)
        )

      long_data_list[[length(long_data_list) + 1]] <- current_data
    }
  }

  long_data <- dplyr::bind_rows(long_data_list)
  if (nrow(long_data) == 0) {
    return(empty)
  }
  long_data
}
