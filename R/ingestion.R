#' Core ingestion logic for catch survey data
#'
#' @param version Version identifier (e.g., "v1", "v2")
#' @param kobo_config Configuration object containing Kobo connection details
#' @param storage_config Configuration object containing storage details
#' @return No return value. Processes and uploads data.
#' @keywords internal
ingest_catch_survey_version <- function(version, kobo_config, storage_config) {
  logger::log_info(glue::glue(
    "Downloading WCS Fish Catch Survey Kobo data ({version})..."
  ))

  data_raw <- coasts::get_kobo_data(
    url = kobo_config$url,
    assetid = kobo_config$asset_id,
    uname = kobo_config$username,
    pwd = kobo_config$password,
    encoding = "UTF-8",
    format = "json"
  )

  logger::log_info(glue::glue(
    "Checking uniqueness of {length(data_raw)} submissions for {version}..."
  ))

  # Check that submissions are unique in case there is overlap in the pagination
  unique_ids <- dplyr::n_distinct(purrr::map_dbl(data_raw, ~ .$`_id`))
  if (unique_ids != length(data_raw)) {
    stop(glue::glue(
      "Number of submission ids ({unique_ids}) not the same as number of records ({length(data_raw)}) in {version} data"
    ))
  }

  logger::log_info(glue::glue(
    "Converting {version} Kobo data to tabular format..."
  ))

  raw_survey <- data_raw %>%
    purrr::map(flatten_row) %>%
    dplyr::bind_rows() %>%
    dplyr::rename(submission_id = "_id")

  logger::log_info(glue::glue(
    "Converted {nrow(raw_survey)} rows with {ncol(raw_survey)} columns for {version}"
  ))

  coasts::upload_parquet_to_cloud(
    data = raw_survey,
    prefix = storage_config$file_prefix,
    provider = storage_config$provider,
    options = storage_config$options
  )

  logger::log_info(glue::glue(
    "Successfully completed ingestion for {version}"
  ))
}

#' Download and Process WCS Catch Surveys from Kobotoolbox
#'
#' This function retrieves WCS survey data (v1 and v2) from Kobotoolbox,
#' processes it, and uploads the raw data as Parquet files to Google Cloud Storage.
#' This function retrieves WCS survey data (v1 and v2) from Kobotoolbox,
#' processes it, and uploads the raw data as Parquet files to Google Cloud Storage.
#'
#' @param log_threshold Logging threshold level (default: logger::DEBUG)
#'
#' @return No return value. Function downloads data, processes it, and uploads to Google Cloud Storage.
#'
#' @details
#' The function performs the following steps:
#' 1. Reads configuration settings.
#' 2. Downloads survey data from Kobotoolbox using `get_kobo_data`.
#' 3. Checks for uniqueness of submissions.
#' 4. Converts data to tabular format.
#' 5. Uploads raw data as Parquet files to Google Cloud Storage.
#' 5. Uploads raw data as Parquet files to Google Cloud Storage.
#'
#' This function processes WCS v1 (eu.kobotoolbox.org) and v2 (kf.kobotoolbox.org) catch surveys.
#' This function processes WCS v1 (eu.kobotoolbox.org) and v2 (kf.kobotoolbox.org) catch surveys.
#'
#' @keywords workflow ingestion
#' @export
#'
#' @examples
#' \dontrun{
#' ingest_wcs_surveys()
#' }
ingest_wcs_surveys <- function(log_threshold = logger::DEBUG) {
  conf <- read_config()

  # Define WCS version configurations
  version_configs <- list(
    v1 = list(
      kobo = list(
        url = "eu.kobotoolbox.org",
        asset_id = conf$ingestion$wcs$koboform$asset_id,
        username = conf$ingestion$wcs$koboform$username,
        password = conf$ingestion$wcs$koboform$password
      ),
      storage = list(
        file_prefix = conf$surveys$wcs$catch$v1$raw$file_prefix,
        provider = conf$storage$google$key,
        options = conf$storage$google$options
      )
    ),
    v2 = list(
      kobo = list(
        url = "kf.kobotoolbox.org",
        asset_id = conf$ingestion$wcs$koboform_kf$asset_id_kf,
        username = conf$ingestion$wcs$koboform_kf$username_kf,
        password = conf$ingestion$wcs$koboform_kf$password_kf
      ),
      storage = list(
        file_prefix = conf$surveys$wcs$catch$v2$raw$file_prefix,
        provider = conf$storage$google$key,
        options = conf$storage$google$options
      )
    )
  )

  # Process each WCS version
  purrr::iwalk(
    version_configs,
    ~ {
      ingest_catch_survey_version(
        version = .y,
        kobo_config = .x$kobo,
        storage_config = .x$storage
      )
    }
  )
}

#' Download and Process KEFS (BMU DAILY ARTISANAL 2025) Catch Surveys from Kobotoolbox
#'
#' This function retrieves KEFS (BMU DAILY ARTISANAL 2025) survey data from their
#' dedicated Kobo instance, processes it, and uploads the raw data as a Parquet file
#' to Google Cloud Storage.
#'
#' @param log_threshold Logging threshold level (default: logger::DEBUG)
#'
#' @return No return value. Function downloads data, processes it, and uploads to Google Cloud Storage.
#'
#' @details
#' The function performs the following steps:
#' 1. Reads configuration settings.
#' 2. Downloads survey data from KEFS Kobo instance (kf.fims.kefs.go.ke).
#' 3. Checks for uniqueness of submissions.
#' 4. Converts data to tabular format.
#' 5. Uploads raw data as a Parquet file to Google Cloud Storage.
#'
#' Note: Preprocessing, validation, and export stages for KEFS data are not yet implemented.
#'
#' @keywords workflow ingestion
#' @export
#'
#' @examples
#' \dontrun{
#' ingest_kefs_surveys_v1()
#' }
ingest_kefs_surveys_v1 <- function(log_threshold = logger::DEBUG) {
  conf <- read_config()

  # Define KEFS configuration
  kefs_config <- list(
    v1 = list(
      kobo = list(
        url = "kf.fims.kefs.go.ke",
        asset_id = conf$ingestion$kefs$koboform$asset_id_v1,
        username = conf$ingestion$kefs$koboform$username,
        password = conf$ingestion$kefs$koboform$password
      ),
      storage = list(
        file_prefix = conf$surveys$kefs$v1$raw$file_prefix,
        provider = conf$storage$google$key,
        options = conf$storage$google$options
      )
    )
  )

  # Process KEFS data
  purrr::iwalk(
    kefs_config,
    ~ {
      ingest_catch_survey_version(
        version = .y,
        kobo_config = .x$kobo,
        storage_config = .x$storage
      )
    }
  )
}

#' Download and Process KEFS (CATCH ASSESSMENT QUESTIONNAIRE) Catch Surveys from Kobotoolbox
#'
#' This function retrieves KEFS (CATCH ASSESSMENT QUESTIONNAIRE) survey data from their
#' dedicated Kobo instance, processes it, and uploads the raw data as a Parquet file
#' to Google Cloud Storage.
#'
#' @param log_threshold Logging threshold level (default: logger::DEBUG)
#'
#' @return No return value. Function downloads data, processes it, and uploads to Google Cloud Storage.
#'
#' @details
#' The function performs the following steps:
#' 1. Reads configuration settings.
#' 2. Downloads survey data from KEFS Kobo instance (kf.fims.kefs.go.ke).
#' 3. Checks for uniqueness of submissions.
#' 4. Converts data to tabular format.
#' 5. Uploads raw data as a Parquet file to Google Cloud Storage.
#'
#' Note: Preprocessing, validation, and export stages for KEFS data are not yet implemented.
#'
#' @keywords workflow ingestion
#' @export
#'
#' @examples
#' \dontrun{
#' ingest_kefs_surveys_v2()
#' }
ingest_kefs_surveys_v2 <- function(log_threshold = logger::DEBUG) {
  conf <- read_config()

  # Define KEFS configuration
  kefs_config <- list(
    v2 = list(
      kobo = list(
        url = "kf.fims.kefs.go.ke",
        asset_id = conf$ingestion$kefs$koboform$asset_id_v2,
        username = conf$ingestion$kefs$koboform$username,
        password = conf$ingestion$kefs$koboform$password
      ),
      storage = list(
        file_prefix = conf$surveys$kefs$v2$raw$file_prefix,
        provider = conf$storage$google$key,
        options = conf$storage$google$options
      )
    )
  )

  # Process KEFS data
  purrr::iwalk(
    kefs_config,
    ~ {
      ingest_catch_survey_version(
        version = .y,
        kobo_config = .x$kobo,
        storage_config = .x$storage
      )
    }
  )
}


#' Download and Process WCS Price Surveys from Kobotoolbox
#'
#' This function retrieves survey data from Kobotoolbox for a specific project,
#' processes it, and uploads the raw data as a Parquet file to Google Cloud Storage.
#' It uses the `get_kobo_data` function, which is a wrapper for `kobotools_kpi_data`
#' from the KoboconnectR package.
#'
#' @param versions Character vector of versions to process (default: c("v1", "v2"))
#' @param url The URL of Kobotoolbox (default is NULL, uses value from configuration).
#' @param project_id The asset ID of the project to download data from (default is NULL, uses value from configuration).
#' @param username Username for Kobotoolbox account (default is NULL, uses value from configuration).
#' @param psswd Password for Kobotoolbox account (default is NULL, uses value from configuration).
#' @param encoding Encoding to be used for data retrieval (default is NULL, uses "UTF-8").
#'
#' @return No return value. Function downloads data, processes it, and uploads to Google Cloud Storage.
#'
#' @details
#' The function performs the following steps:
#' 1. Reads configuration settings.
#' 2. Downloads survey data from Kobotoolbox using `get_kobo_data`.
#' 3. Checks for uniqueness of submissions.
#' 4. Converts data to tabular format.
#' 5. Uploads raw data as a Parquet file to Google Cloud Storage.
#'
#' Note that while parameters are provided for customization, the function
#' currently uses hardcoded values and configuration settings for some parameters.
#'
#' @keywords workflow ingestion
#' @export
#'
#' @examples
#' \dontrun{
#' # Process both versions
#' ingest_landings_price()
#'
#' # Process only v1
#' ingest_landings_price(versions = "v1")
#'
#' # Process specific versions
#' ingest_landings_price(versions = c("v1", "v2"))
#' }
ingest_landings_price <- function(
  versions = c("v1", "v2"),
  url = NULL,
  project_id = NULL,
  username = NULL,
  psswd = NULL,
  encoding = NULL
) {
  conf <- read_config()

  # Define version configurations
  version_configs <- list(
    v1 = list(
      kobo = list(
        url = "eu.kobotoolbox.org",
        asset_id = conf$ingestion$wcs$koboform$asset_id_price,
        username = conf$ingestion$wcs$koboform$username,
        password = conf$ingestion$wcs$koboform$password
      ),
      storage = list(
        file_prefix = conf$surveys$wcs$price$v1$raw$file_prefix,
        provider = conf$storage$google$key,
        options = conf$storage$google$options
      )
    ),
    v2 = list(
      kobo = list(
        url = "kf.kobotoolbox.org",
        asset_id = conf$ingestion$wcs$koboform_kf$asset_id_price_kf,
        username = conf$ingestion$wcs$koboform_kf$username_kf,
        password = conf$ingestion$wcs$koboform_kf$password_kf
      ),
      storage = list(
        file_prefix = conf$surveys$wcs$price$v2$raw$file_prefix,
        provider = conf$storage$google$key,
        options = conf$storage$google$options
      )
    )
  )

  # Process requested versions
  purrr::iwalk(
    version_configs[versions],
    ~ {
      ingest_price_survey_version(.y, .x$kobo, .x$storage)
    }
  )
}

#' Core ingestion logic for price survey data
#'
#' @param version Version identifier (e.g., "v1", "v2")
#' @param kobo_config Configuration object containing Kobo connection details
#' @param storage_config Configuration object containing storage details
#' @return No return value. Processes and uploads data.
#' @keywords internal
ingest_price_survey_version <- function(version, kobo_config, storage_config) {
  logger::log_info("Downloading WCS Fish Price Survey Kobo data...")

  data_raw <- coasts::get_kobo_data(
    url = kobo_config$url,
    assetid = kobo_config$asset_id,
    uname = kobo_config$username,
    pwd = kobo_config$password,
    encoding = "UTF-8",
    format = "json"
  )

  # Check that submissions are unique in case there is overlap in the pagination
  if (
    dplyr::n_distinct(purrr::map_dbl(data_raw, ~ .$`_id`)) != length(data_raw)
  ) {
    stop("Number of submission ids not the same as number of records")
  }

  logger::log_info(
    "Converting WCS Fish Price Survey Kobo data to tabular format..."
  )

  raw_survey <- data_raw %>%
    purrr::map(flatten_row) %>%
    dplyr::bind_rows() %>%
    dplyr::rename(submission_id = "_id")

  coasts::upload_parquet_to_cloud(
    data = raw_survey,
    prefix = storage_config$file_prefix,
    provider = storage_config$provider,
    options = storage_config$options
  )
}

#' Flatten a Single Row of Kobotoolbox Data
#'
#' This internal function flattens a single row of Kobotoolbox data,
#' converting nested structures into a flat tibble.
#'
#' @param x A list representing a single row of Kobotoolbox data.
#'
#' @return A flattened tibble representing the input row.
#'
#' @keywords internal
flatten_row <- function(x) {
  x %>%
    # Each row is composed of several fields
    purrr::imap(flatten_field) %>%
    rlang::squash() %>%
    tibble::as_tibble(.name_repair = "unique")
}

#' Flatten a Single Field of Kobotoolbox Data
#'
#' This internal function flattens a single field within a row of Kobotoolbox data.
#'
#' @param x The field to be flattened.
#' @param p The name of the parent field.
#'
#' @return A flattened list representing the input field.
#'
#' @keywords internal
flatten_field <- function(x, p) {
  # If the field is a simple vector do nothing but if the field is a list we
  # need more logic
  if (inherits(x, "list")) {
    if (length(x) > 0) {
      if (purrr::vec_depth(x) == 2) {
        # If the field-list has named elements is we just need to rename the list
        x <- list(x) %>%
          rlang::set_names(p) %>%
          unlist() %>%
          as.list()
      } else {
        # If the field-list is an "array" we need to iterate over its children
        x <- purrr::imap(x, rename_child, p = p)
      }
    }
  } else {
    if (is.null(x)) x <- NA
  }
  x
}

#' Rename Child Elements in Nested Kobotoolbox Data
#'
#' This internal function renames child elements in nested Kobotoolbox data structures.
#'
#' @param x The child element to be renamed.
#' @param i The index or name of the child element.
#' @param p The name of the parent element.
#'
#' @return A renamed list of child elements.
#'
#' @keywords internal
rename_child <- function(x, i, p) {
  if (length(x) == 0) {
    if (is.null(x)) {
      x <- NA
    }
    x <- list(x)
    x <- rlang::set_names(x, paste(p, i - 1, sep = "."))
  } else {
    if (inherits(i, "character")) {
      x <- rlang::set_names(x, paste(p, i, sep = "."))
    } else if (inherits(i, "integer")) {
      x <- rlang::set_names(x, paste(p, i - 1, names(x), sep = "."))
    }
  }
  x
}

#' Get All Records from Airtable with Pagination
#'
#' Retrieves ALL records from an Airtable table, handling pagination automatically.
#'
#' @param base_id Character string. The Airtable base ID.
#' @param table_name Character string. The name of the table to retrieve.
#' @param token Character string. Airtable API token for authentication.
#' @param list_handler Character string. "collapse" (default) or "count" for list fields.
#'
#' @return A tibble with all records and an 'airtable_id' column.
#'
#' @keywords ingestion
#' @export
airtable_to_df <- function(
  base_id,
  table_name,
  token,
  list_handler = "collapse"
) {
  base_url <- glue::glue(
    "https://api.airtable.com/v0/{base_id}/{URLencode(table_name)}"
  )

  all_records <- list()
  offset <- NULL
  page_count <- 0
  total_retrieved <- 0

  repeat {
    page_count <- page_count + 1
    cat("Fetching page", page_count, "...")

    # Make the request
    req <- httr2::request(base_url) %>%
      httr2::req_headers(Authorization = paste("Bearer", token))

    if (!is.null(offset)) {
      req <- req %>% httr2::req_url_query(offset = offset)
    }

    res <- req %>% httr2::req_perform()
    content <- res %>% httr2::resp_body_json()

    # Add records from this page
    page_records <- content$records
    all_records <- c(all_records, page_records)
    total_retrieved <- total_retrieved + length(page_records)

    cat(
      " retrieved",
      length(page_records),
      "records (total:",
      total_retrieved,
      ")\n"
    )

    # Check if there are more pages
    if (is.null(content$offset)) {
      cat("Retrieved all available records\n")
      break
    }

    offset <- content$offset
  }

  cat("Converting", length(all_records), "records to data frame...\n")

  # Convert all records to tibble
  df <- all_records %>%
    purrr::map_dfr(
      ~ {
        fields <- .x$fields
        fields$airtable_id <- .x$id

        # Handle lists
        if (list_handler == "collapse") {
          fields <- fields %>%
            purrr::map_if(is.list, ~ paste(.x, collapse = ", "))
        } else if (list_handler == "count") {
          fields <- fields %>%
            purrr::map_if(is.list, length)
        }

        dplyr::as_tibble(fields)
      }
    )

  cat("Successfully converted", nrow(df), "records to tibble\n")
  return(df)
}
