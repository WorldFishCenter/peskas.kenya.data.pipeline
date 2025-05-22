#' Core ingestion logic for catch survey data
#'
#' @param version Version identifier (e.g., "v1", "v2")
#' @param kobo_config Configuration object containing Kobo connection details
#' @param storage_config Configuration object containing storage details
#' @return No return value. Processes and uploads data.
#' @keywords internal
ingest_catch_survey_version <- function(version, kobo_config, storage_config) {
  logger::log_info(glue::glue("Downloading WCS Fish Catch Survey Kobo data ({version})..."))

  data_raw <- get_kobo_data(
    url = kobo_config$url,
    assetid = kobo_config$asset_id,
    uname = kobo_config$username,
    pwd = kobo_config$password,
    encoding = "UTF-8",
    format = "json"
  )

  # Check that submissions are unique in case there is overlap in the pagination
  if (dplyr::n_distinct(purrr::map_dbl(data_raw, ~ .$`_id`)) != length(data_raw)) {
    stop(glue::glue("Number of submission ids not the same as number of records in {version} data"))
  }

  logger::log_info(glue::glue("Converting WCS Fish Catch Survey Kobo data ({version}) to tabular format..."))

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
#' This function retrieves survey data from Kobotoolbox for a specific project,
#' processes it, and uploads the raw data as a Parquet file to Google Cloud Storage.
#' It uses the `get_kobo_data` function, which is a wrapper for `kobotools_kpi_data`
#' from the KoboconnectR package.
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
#' ingest_landings()
#' }
ingest_landings <- function(log_threshold = logger::DEBUG) {
  conf <- read_config()

  # Define version configurations
  version_configs <- list(
    v1 = list(
      kobo = list(
        url = "eu.kobotoolbox.org",
        asset_id = conf$ingestion$koboform$asset_id,
        username = conf$ingestion$koboform$username,
        password = conf$ingestion$koboform$password
      ),
      storage = list(
        file_prefix = conf$surveys$catch$v1$raw$file_prefix,
        provider = conf$storage$google$key,
        options = conf$storage$google$options
      )
    ),
    v2 = list(
      kobo = list(
        url = "kf.kobotoolbox.org",
        asset_id = conf$ingestion$koboform_kf$asset_id_kf,
        username = conf$ingestion$koboform_kf$username_kf,
        password = conf$ingestion$koboform_kf$password_kf
      ),
      storage = list(
        file_prefix = conf$surveys$catch$v2$raw$file_prefix,
        provider = conf$storage$google$key,
        options = conf$storage$google$options
      )
    )
  )

  # Process each version
  purrr::iwalk(version_configs, ~ {
    ingest_catch_survey_version(
      version = .y,
      kobo_config = .x$kobo,
      storage_config = .x$storage
    )
  })
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
ingest_landings_price <- function(versions = c("v1", "v2"),
                                  url = NULL,
                                  project_id = NULL,
                                  username = NULL,
                                  psswd = NULL,
                                  encoding = NULL) {
  conf <- read_config()

  # Define version configurations
  version_configs <- list(
    v1 = list(
      kobo = list(
        url = "eu.kobotoolbox.org",
        asset_id = conf$ingestion$koboform$asset_id_price,
        username = conf$ingestion$koboform$username,
        password = conf$ingestion$koboform$password
      ),
      storage = list(
        file_prefix = conf$surveys$price$v1$raw$file_prefix,
        provider = conf$storage$google$key,
        options = conf$storage$google$options
      )
    ),
    v2 = list(
      kobo = list(
        url = "kf.kobotoolbox.org",
        asset_id = conf$ingestion$koboform_kf$asset_id_price_kf,
        username = conf$ingestion$koboform_kf$username,
        password = conf$ingestion$koboform_kf$password
      ),
      storage = list(
        file_prefix = conf$surveys$price$v2$raw$file_prefix,
        provider = conf$storage$google$key,
        options = conf$storage$google$options
      )
    )
  )

  # Process requested versions
  purrr::iwalk(version_configs[versions], ~ {
    ingest_price_survey_version(.y, .x$kobo, .x$storage)
  })
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
  if (dplyr::n_distinct(purrr::map_dbl(data_raw, ~ .$`_id`)) != length(data_raw)) {
    stop("Number of submission ids not the same as number of records")
  }

  logger::log_info("Converting WCS Fish Price Survey Kobo data to tabular format...")

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
    format = "json") {
  if (!is.character(url)) stop("URL entered is not a string")
  if (!is.character(uname)) stop("uname (username) entered is not a string")
  if (!is.character(pwd)) stop("pwd (password) entered is not a string")
  if (!is.character(assetid)) stop("assetid entered is not a string")
  if (is.null(url) | url == "") stop("URL empty")
  if (is.null(uname) | uname == "") stop("uname (username) empty")
  if (is.null(pwd) | pwd == "") stop("pwd (password) empty")
  if (is.null(assetid) | assetid == "") stop("assetid empty")
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
    if (is.null(x)) x <- NA
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
