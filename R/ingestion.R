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

  data_raw <- get_kobo_data(
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
    stop(glue::glue(
      "Number of submission ids not the same as number of records in {version} data"
    ))
  }

  logger::log_info(glue::glue(
    "Converting WCS Fish Catch Survey Kobo data ({version}) to tabular format..."
  ))

  raw_survey <- data_raw %>%
    purrr::map(flatten_row) %>%
    dplyr::bind_rows() %>%
    dplyr::rename(submission_id = "_id")

  upload_parquet_to_cloud(
    data = raw_survey,
    prefix = storage_config$file_prefix,
    provider = storage_config$provider,
    options = storage_config$options
  )
}

#' Download and Process WCS Catch Surveys from Kobotoolbox
#'
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
#'
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

#' Download and Process KEFS Catch Surveys from Kobotoolbox
#'
#' This function retrieves KEFS (Kenya Fisheries Service) survey data from their
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
#' 2. Downloads survey data from KEFS Kobo instance (kf.fimskenya.co.ke).
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
#' ingest_kefs_surveys()
#' }
ingest_kefs_surveys <- function(log_threshold = logger::DEBUG) {
  conf <- read_config()

  # Define KEFS configuration
  kefs_config <- list(
    kefs = list(
      kobo = list(
        url = "kf.fimskenya.co.ke",
        asset_id = conf$ingestion$kefs$koboform$asset_id,
        username = conf$ingestion$kefs$koboform$username,
        password = conf$ingestion$kefs$koboform$password
      ),
      storage = list(
        file_prefix = conf$surveys$kefs$raw$file_prefix,
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

#' Download and Process All Catch Surveys (WCS + KEFS)
#'
#' @description
#' **DEPRECATED**: This function is maintained for backward compatibility but will be removed
#' in a future version. Use `ingest_wcs_surveys()` and `ingest_kefs_surveys()` separately instead.
#'
#' This function ingests both WCS and KEFS catch surveys in a single call, which creates
#' coupling between data sources. If one source fails, both fail.
#'
#' @param log_threshold Logging threshold level (default: logger::DEBUG)
#'
#' @return No return value. Function downloads data, processes it, and uploads to Google Cloud Storage.
#'
#' @keywords workflow ingestion
#' @export
#'
#' @examples
#' \dontrun{
#' # Old way (deprecated):
#' ingest_landings()
#'
#' # New way (preferred):
#' ingest_wcs_surveys()
#' ingest_kefs_surveys()
#' }
ingest_landings <- function(log_threshold = logger::DEBUG) {
  logger::log_warn(
    "ingest_landings() is deprecated. Use ingest_wcs_surveys() and ingest_kefs_surveys() separately."
  )

  # Call both functions
  ingest_wcs_surveys(log_threshold = log_threshold)
  ingest_kefs_surveys(log_threshold = log_threshold)
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

  data_raw <- get_kobo_data(
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

  upload_parquet_to_cloud(
    data = raw_survey,
    prefix = storage_config$file_prefix,
    provider = storage_config$provider,
    options = storage_config$options
  )
}

#' Retrieve Data from Kobotoolbox API
#'
#' This function retrieves survey data from Kobotoolbox API for a specific asset.
#' It supports pagination and handles both JSON and XML formats.
#'
#' @param assetid The asset ID of the Kobotoolbox form.
#' @param url The URL of Kobotoolbox (default is "eu.kobotoolbox.org").
#' @param uname Username for Kobotoolbox account.
#' @param pwd Password for Kobotoolbox account.
#' @param encoding Encoding to be used for data retrieval (default is "UTF-8").
#' @param format Format of the data to retrieve, either "json" or "xml" (default is "json").
#'
#' @return A list containing all retrieved survey results.
#' @keywords ingestion
#' @details
#' The function uses pagination to retrieve large datasets, with a limit of 30,000 records per request.
#' It continues to fetch data until all records are retrieved or an error occurs.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' kobo_data <- get_kobo_data(
#'   assetid = "your_asset_id",
#'   uname = "your_username",
#'   pwd = "your_password"
#' )
#' }
get_kobo_data <- function(
  assetid,
  url = "eu.kobotoolbox.org",
  uname = NULL,
  pwd = NULL,
  encoding = "UTF-8",
  format = "json"
) {
  if (!is.character(url)) {
    stop("URL entered is not a string")
  }
  if (!is.character(uname)) {
    stop("uname (username) entered is not a string")
  }
  if (!is.character(pwd)) {
    stop("pwd (password) entered is not a string")
  }
  if (!is.character(assetid)) {
    stop("assetid entered is not a string")
  }
  if (is.null(url) | url == "") {
    stop("URL empty")
  }
  if (is.null(uname) | uname == "") {
    stop("uname (username) empty")
  }
  if (is.null(pwd) | pwd == "") {
    stop("pwd (password) empty")
  }
  if (is.null(assetid) | assetid == "") {
    stop("assetid empty")
  }
  if (!format %in% c("json", "xml")) {
    stop("format must be either 'json' or 'xml'")
  }

  base_url <- paste0(
    "https://",
    url,
    "/api/v2/assets/",
    assetid,
    "/data.",
    format
  )

  message("Starting data retrieval from ", base_url)

  get_page <- function(url, limit = 30000, start = 0) {
    full_url <- paste0(url, "?limit=", limit, "&start=", start)

    message("Retrieving page starting at record ", start)

    respon.kpi <- tryCatch(
      expr = {
        httr2::request(full_url) |>
          httr2::req_auth_basic(uname, pwd) |>
          httr2::req_perform()
      },
      error = function(x) {
        message(
          "Error on page starting at record ",
          start,
          ". Please try again or check the input parameters."
        )
        return(NULL)
      }
    )

    if (!is.null(respon.kpi)) {
      content_type <- httr2::resp_content_type(respon.kpi)

      if (grepl("json", content_type)) {
        message("Successfully retrieved JSON data starting at record ", start)
        return(httr2::resp_body_json(respon.kpi, encoding = encoding))
      } else if (grepl("xml", content_type)) {
        message("Successfully retrieved XML data starting at record ", start)
        return(httr2::resp_body_string(respon.kpi, encoding = encoding))
      } else if (grepl("html", content_type)) {
        warning(
          "Unexpected HTML response for start ",
          start,
          ". Unable to parse."
        )
        return(NULL)
      } else {
        warning(
          "Unexpected content type: ",
          content_type,
          " for start ",
          start,
          ". Unable to parse."
        )
        return(NULL)
      }
    } else {
      return(NULL)
    }
  }

  all_results <- list()
  start <- 0
  limit <- 30000
  get_next <- TRUE

  while (get_next) {
    page_results <- get_page(base_url, limit, start)

    if (is.null(page_results)) {
      message("Error occurred. Stopping data retrieval.")
      break
    }

    new_results <- page_results$results
    all_results <- c(all_results, new_results)

    message("Total records retrieved so far: ", length(all_results))

    if (length(new_results) < limit) {
      message("Retrieved all available records.")
      get_next <- FALSE
    } else {
      start <- start + limit
    }
  }

  message(
    "Data retrieval complete. Total records retrieved: ",
    length(all_results)
  )

  # Check for unique submission IDs
  submission_ids <- sapply(all_results, function(x) x$`_id`)
  if (length(unique(submission_ids)) != length(all_results)) {
    warning(
      "Number of unique submission IDs does not match the number of records. There may be duplicates."
    )
  }

  return(all_results)
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


#' Ingest Pelagic Data Systems (PDS) Trip Data
#'
#' @description
#' This function handles the automated ingestion of GPS boat trip data from Pelagic Data Systems (PDS).
#' It performs the following operations:
#' 1. Retrieves device metadata from the configured source
#' 2. Downloads trip data from PDS API using device IMEIs
#' 3. Converts the data to parquet format
#' 4. Uploads the processed file to configured cloud storage
#'
#' @details
#' The function requires specific configuration in the `conf.yml` file with the following structure:
#'
#' ```yaml
#' pds:
#'   token: "your_pds_token"               # PDS API token
#'   secret: "your_pds_secret"             # PDS API secret
#'   pds_trips:
#'     file_prefix: "pds_trips"            # Prefix for output files
#' storage:
#'   google:                               # Storage provider name
#'     key: "google"                       # Storage provider identifier
#'     options:
#'       project: "project-id"             # Cloud project ID
#'       bucket: "bucket-name"             # Storage bucket name
#'       service_account_key: "path/to/key.json"
#' ```
#'
#' The function processes trips sequentially:
#' - Retrieves device metadata using `get_metadata()`
#' - Downloads trip data using the `get_trips()` function
#' - Converts the data to parquet format
#' - Uploads the resulting file to configured storage provider
#'
#' @param log_threshold The logging threshold to use. Default is logger::DEBUG.
#'   See `logger::log_levels` for available options.
#'
#' @return None (invisible). The function performs its operations for side effects:
#'   - Creates a parquet file locally with trip data
#'   - Uploads file to configured cloud storage
#'   - Generates logs of the process
#'
#' @examples
#' \dontrun{
#' # Run with default debug logging
#' ingest_pds_trips()
#'
#' # Run with info-level logging only
#' ingest_pds_trips(logger::INFO)
#' }
#'
#' @seealso
#' * [get_trips()] for details on the PDS trip data retrieval process
#' * [get_metadata()] for details on the device metadata retrieval
#' * [upload_cloud_file()] for details on the cloud upload process
#'
#' @keywords workflow ingestion
#' @export
ingest_pds_trips <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  pars <- read_config()

  devices_table <- airtable_to_df(
    token = pars$metadata$airtable$token,
    base_id = pars$metadata$airtable$base_id,
    table_name = "pds_devices"
  )

  # filter for Kenya
  kenya_devices <-
    devices_table |>
    dplyr::filter(stringr::str_detect(.data$customer_name, "Kenya"))

  boats_trips <- get_trips(
    token = pars$pds$token,
    secret = pars$pds$secret,
    dateFrom = "2023-01-01",
    dateTo = Sys.Date(),
    imeis = unique(kenya_devices$imei)
  )

  filename <- pars$pds$pds_trips$file_prefix %>%
    add_version(extension = "parquet")

  arrow::write_parquet(
    x = boats_trips,
    sink = filename,
    compression = "lz4",
    compression_level = 12
  )

  logger::log_info("Uploading {filename} to cloud storage")
  upload_cloud_file(
    file = filename,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
}
#' Ingest Pelagic Data Systems (PDS) Track Data
#'
#' @description
#' This function handles the automated ingestion of GPS boat track data from Pelagic Data Systems (PDS).
#' It downloads and stores only new tracks that haven't been previously uploaded to Google Cloud Storage.
#' Uses parallel processing for improved performance.
#'
#' @param log_threshold The logging threshold to use. Default is logger::DEBUG.
#' @param batch_size Optional number of tracks to process. If NULL, processes all new tracks.
#'
#' @return None (invisible). The function performs its operations for side effects.
#'
#' @keywords workflow ingestion
#' @export
ingest_pds_tracks <- function(
  log_threshold = logger::DEBUG,
  batch_size = NULL
) {
  logger::log_threshold(log_threshold)
  pars <- read_config()

  # Get trips file from cloud storage
  logger::log_info("Getting trips file from cloud storage...")
  pds_trips_parquet <- cloud_object_name(
    prefix = pars$pds$pds_trips$file_prefix,
    provider = pars$storage$google$key,
    extension = "parquet",
    version = pars$pds$pds_trips$version,
    options = pars$storage$google$options
  )

  logger::log_info("Downloading {pds_trips_parquet}")
  download_cloud_file(
    name = pds_trips_parquet,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )

  # Read trip IDs
  logger::log_info("Reading trip IDs...")
  trips_data <- arrow::read_parquet(file = pds_trips_parquet) %>%
    dplyr::pull("Trip") %>%
    unique()

  # Clean up downloaded file
  unlink(pds_trips_parquet)

  # List existing files in GCS bucket
  logger::log_info("Checking existing tracks in cloud storage...")
  existing_tracks <-
    googleCloudStorageR::gcs_list_objects(
      bucket = pars$pds_storage$google$options$bucket,
      prefix = pars$pds$pds_tracks$file_prefix
    )$name

  # Get new trip IDs
  existing_trip_ids <- extract_trip_ids_from_filenames(existing_tracks)
  new_trip_ids <- setdiff(trips_data, existing_trip_ids)

  if (length(new_trip_ids) == 0) {
    logger::log_info("No new tracks to download")
    return(invisible())
  }

  # Setup parallel processing
  workers <- parallel::detectCores() - 1
  logger::log_info("Setting up parallel processing with {workers} workers...")
  future::plan(future::multisession, workers = workers)

  # Select tracks to process
  process_ids <- if (!is.null(batch_size)) {
    new_trip_ids[1:batch_size]
  } else {
    new_trip_ids
  }
  logger::log_info("Processing {length(process_ids)} new tracks in parallel...")

  # Process tracks in parallel with progress bar
  results <- furrr::future_map(
    process_ids,
    function(trip_id) {
      tryCatch(
        {
          # Create filename for this track
          track_filename <- sprintf(
            "%s_%s.parquet",
            pars$pds$pds_tracks$file_prefix,
            trip_id
          )

          # Get track data
          track_data <- get_trip_points(
            token = pars$pds$token,
            secret = pars$pds$secret,
            id = as.character(trip_id),
            deviceInfo = TRUE
          )

          # Save to parquet
          arrow::write_parquet(
            x = track_data,
            sink = track_filename,
            compression = "lz4",
            compression_level = 12
          )

          # Upload to cloud
          logger::log_info("Uploading track for trip {trip_id}")
          upload_cloud_file(
            file = track_filename,
            provider = pars$pds_storage$google$key,
            options = pars$pds_storage$google$options
          )

          # Clean up local file
          unlink(track_filename)

          list(
            status = "success",
            trip_id = trip_id,
            message = "Successfully processed"
          )
        },
        error = function(e) {
          list(
            status = "error",
            trip_id = trip_id,
            message = e$message
          )
        }
      )
    },
    .options = furrr::furrr_options(seed = TRUE),
    .progress = TRUE
  )

  # Clean up parallel processing
  future::plan(future::sequential)

  # Summarize results
  successes <- sum(purrr::map_chr(results, "status") == "success")
  failures <- sum(purrr::map_chr(results, "status") == "error")

  logger::log_info(
    "Processing complete. Successfully processed {successes} tracks."
  )
  if (failures > 0) {
    logger::log_warn("Failed to process {failures} tracks.")
    failed_results <- results[purrr::map_chr(results, "status") == "error"]
    failed_trips <- purrr::map_chr(failed_results, "trip_id")
    failed_messages <- purrr::map_chr(failed_results, "message")

    logger::log_warn("Failed trip IDs and reasons:")
    purrr::walk2(
      failed_trips,
      failed_messages,
      ~ logger::log_warn("Trip {.x}: {.y}")
    )
  }
}

#' Extract Trip IDs from Track Filenames
#'
#' @param filenames Character vector of track filenames
#' @return Character vector of trip IDs
#' @keywords internal
extract_trip_ids_from_filenames <- function(filenames) {
  if (length(filenames) == 0) {
    return(character(0))
  }
  # Assuming filenames are in format: pds-tracks_TRIPID.parquet
  gsub(".*_([0-9]+)\\.parquet$", "\\1", filenames)
}

#' Process Single PDS Track
#'
#' @param trip_id Character. The ID of the trip to process.
#' @param pars List. Configuration parameters.
#' @return List with processing status and details.
#' @keywords internal
process_single_track <- function(trip_id, pars) {
  tryCatch(
    {
      # Create filename for this track
      track_filename <- sprintf(
        "%s_%s.parquet",
        pars$pds$pds_tracks$file_prefix,
        trip_id
      )

      # Get track data
      track_data <- get_trip_points(
        token = pars$pds$token,
        secret = pars$pds$secret,
        id = trip_id,
        deviceInfo = TRUE
      )

      # Save to parquet
      arrow::write_parquet(
        x = track_data,
        sink = track_filename,
        compression = "lz4",
        compression_level = 12
      )

      # Upload to cloud
      logger::log_info("Uploading track for trip {trip_id}")
      upload_cloud_file(
        file = track_filename,
        provider = pars$pds_storage$google$key,
        options = pars$pds_storage$google$options
      )

      # Clean up local file
      unlink(track_filename)

      list(
        status = "success",
        trip_id = trip_id,
        message = "Successfully processed"
      )
    },
    error = function(e) {
      list(
        status = "error",
        trip_id = trip_id,
        message = e$message
      )
    }
  )
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
