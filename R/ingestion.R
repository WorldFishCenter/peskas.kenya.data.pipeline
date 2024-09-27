#' Download WCS Surveys from Kobotoolbox
#'
#' This function retrieves survey data from Kobotoolbox for a specific project.
#' It allows users to customize the filename using a prefix, choose between CSV or RDS formats,
#' and decide whether to append versioning information to the filename.
#' The resulting files are downloaded to the working directory or specified path,
#' with paths returned as a character vector.
#'
#' @param url The URL of kobotoolbox (often referred to as 'kpi-url').
#' @param project_id Is the asset id of the asset for which the data is
#' to be downloaded.
#' @param username Username of your kobotoolbox account.
#' @param psswd Password of the account.
#' @param encoding Encoding to be used. Default is "UTF-8".
#'
#' @return A character vector with paths of the downloaded files.
#'
#' @keywords workflow ingestion
#' @export
#' @examples
#' \dontrun{
#' file_list <- retrieve_wcs_surveys(
#'   url = "kf.kobotoolbox.org",
#'   project_id = "my_project_id",
#'   username = "admin",
#'   psswd = "admin",
#'   encoding = "UTF-8"
#' )
#' }
ingest_surveys <- function(url = NULL,
                           project_id = NULL,
                           username = NULL,
                           psswd = NULL,
                           encoding = NULL) {
  conf <- read_config()

  logger::log_info("Downloading WCS Fish Catch Survey Kobo data...")
  data_raw <-
    KoboconnectR::kobotools_kpi_data(
      url = "eu.kobotoolbox.org",
      assetid = conf$ingestion$koboform$asset_id,
      uname = conf$ingestion$koboform$username,
      pwd = conf$ingestion$koboform$password,
      encoding = "UTF-8",
      format = "json"
    )$results

  # Check that submissions are unique in case there is overlap in the pagination
  if (dplyr::n_distinct(purrr::map_dbl(data_raw, ~ .$`_id`)) != length(data_raw)) {
    stop("Number of submission ids not the same as number of records")
  }

  logger::log_info("Converting WCS Fish Catch Survey Kobo data to tabular format...")
  raw_survey <-
    purrr::map(data_raw, flatten_row) %>%
    dplyr::bind_rows()

  logger::log_info("Uploading raw data to mongodb")
  mdb_collection_push(
    data = raw_survey,
    connection_string = conf$storage$mongodb$connection_string,
    collection_name = conf$storage$mongodb$database$pipeline$collection_name$ongoing$raw,
    db_name = conf$storage$mongodb$database$pipeline$name
  )
}

flatten_row <- function(x) {
  x %>%
    # Each row is composed of several fields
    purrr::imap(flatten_field) %>%
    rlang::squash() %>%
    tibble::as_tibble(.name_repair = "unique")
}

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

# Appends parent name or number to element
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
