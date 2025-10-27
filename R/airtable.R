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

#' Get Writable Fields from Airtable Table
#'
#' Returns which fields can be updated (excludes computed fields).
#'
#' @param base_id Character string. The Airtable base ID.
#' @param token Character string. Airtable API token.
#' @param table_name Character string. Name of the table.
#'
#' @return Character vector of writable field names.
#'
#' @keywords helper
#' @export
get_writable_fields <- function(base_id, token, table_name) {
  schema_url <- glue::glue(
    "https://api.airtable.com/v0/meta/bases/{base_id}/tables"
  )

  response <- httr2::request(schema_url) %>%
    httr2::req_headers(Authorization = paste("Bearer", token)) %>%
    httr2::req_perform()

  content <- response %>% httr2::resp_body_json()

  # Find the table
  target_table <- content$tables %>%
    purrr::keep(~ .x$name == table_name) %>%
    purrr::pluck(1)

  if (is.null(target_table)) {
    available_tables <- content$tables %>% purrr::map_chr("name")
    stop(
      "Table '",
      table_name,
      "' not found. Available: ",
      paste(available_tables, collapse = ", ")
    )
  }

  # Show field types
  cat("Field types in", table_name, "table:\n")
  for (field in target_table$fields) {
    status <- if (
      !is.null(field$options$isComputed) && field$options$isComputed
    ) {
      " [COMPUTED]"
    } else {
      " [WRITABLE]"
    }
    cat("- '", field$name, "' (", field$type, ")", status, "\n", sep = "")
  }

  # Return writable fields
  writable_fields <- target_table$fields %>%
    purrr::keep(~ is.null(.x$options$isComputed) || !.x$options$isComputed) %>%
    purrr::map_chr("name")

  return(writable_fields)
}

#' Update Single Airtable Record
#'
#' Updates specific fields in one record.
#'
#' @param base_id Character string. The Airtable base ID.
#' @param table_name Character string. Name of the table.
#' @param token Character string. Airtable API token.
#' @param record_id Character string. ID of the record to update.
#' @param updates Named list. Fields and values to update.
#'
#' @return httr2 response object.
#'
#' @keywords export
#' @export
update_airtable_record <- function(
  base_id,
  table_name,
  token,
  record_id,
  updates
) {
  base_url <- glue::glue(
    "https://api.airtable.com/v0/{base_id}/{URLencode(table_name)}/{record_id}"
  )
  payload <- list(fields = updates)

  tryCatch(
    {
      response <- httr2::request(base_url) %>%
        httr2::req_headers(
          Authorization = paste("Bearer", token),
          `Content-Type` = "application/json"
        ) %>%
        httr2::req_body_json(payload) %>%
        httr2::req_method("PATCH") %>%
        httr2::req_perform()
      return(response)
    },
    error = function(e) {
      if (!is.null(e$resp)) {
        error_body <- e$resp %>% httr2::resp_body_string()
        cat("Error response:\n", error_body, "\n")
      }
      stop(e)
    }
  )
}

#' Bulk Update Multiple Airtable Records
#'
#' Updates multiple records in batches of 10.
#'
#' @param base_id Character string. The Airtable base ID.
#' @param table_name Character string. Name of the table.
#' @param token Character string. Airtable API token.
#' @param updates_df Data frame with 'airtable_id' column and fields to update.
#'
#' @return List of response objects.
#'
#' @keywords export
#' @export
bulk_update_airtable <- function(base_id, table_name, token, updates_df) {
  base_url <- glue::glue(
    "https://api.airtable.com/v0/{base_id}/{URLencode(table_name)}"
  )

  # Prepare records
  records <- updates_df %>%
    purrr::pmap(function(airtable_id, ...) {
      updates <- list(...)
      updates$airtable_id <- NULL
      updates <- updates[!is.na(updates)] # Remove NAs

      list(id = airtable_id, fields = updates)
    })

  # Split into batches of 10
  batches <- split(records, ceiling(seq_along(records) / 10))

  # Send each batch
  responses <- purrr::map(batches, function(batch) {
    payload <- list(records = batch)
    cat("Updating batch of", length(batch), "records...\n")

    tryCatch(
      {
        httr2::request(base_url) %>%
          httr2::req_headers(
            Authorization = paste("Bearer", token),
            `Content-Type` = "application/json"
          ) %>%
          httr2::req_body_json(payload) %>%
          httr2::req_method("PATCH") %>%
          httr2::req_perform()
      },
      error = function(e) {
        if (!is.null(e$resp)) {
          error_body <- e$resp %>% httr2::resp_body_string()
          cat("Error response:\n", error_body, "\n")
        }
        stop(e)
      }
    )
  })

  cat("Successfully updated", nrow(updates_df), "records!\n")
  return(responses)
}

#' Create New Airtable Records
#'
#' Creates new records in batches of 10.
#'
#' @param df Data frame containing data to create.
#' @param base_id Character string. The Airtable base ID.
#' @param table_name Character string. Name of the table.
#' @param token Character string. Airtable API token.
#'
#' @return List of response objects.
#'
#' @keywords export
#' @export
df_to_airtable <- function(df, base_id, table_name, token) {
  base_url <- glue::glue(
    "https://api.airtable.com/v0/{base_id}/{URLencode(table_name)}"
  )

  # Convert to records format
  records <- df %>%
    purrr::pmap(function(...) {
      row_data <- list(...)
      row_data <- row_data[!is.na(row_data)] # Remove NAs
      list(fields = row_data)
    })

  # Split into batches of 10
  batches <- split(records, ceiling(seq_along(records) / 10))

  responses <- purrr::map(batches, function(batch) {
    payload <- list(records = batch)
    cat("Creating batch of", length(batch), "records...\n")

    tryCatch(
      {
        httr2::request(base_url) %>%
          httr2::req_headers(
            Authorization = paste("Bearer", token),
            `Content-Type` = "application/json"
          ) %>%
          httr2::req_body_json(payload) %>%
          httr2::req_method("POST") %>%
          httr2::req_perform()
      },
      error = function(e) {
        if (!is.null(e$resp)) {
          error_body <- e$resp %>% httr2::resp_body_string()
          cat("Error response:\n", error_body, "\n")
        }
        stop(e)
      }
    )
  })

  cat("Successfully created", nrow(df), "records!\n")
  return(responses)
}


#' Fetch and Filter Asset Data from Airtable
#'
#' @description
#' Retrieves data from a specified Airtable table and filters it based on form ID.
#' Handles cases where form_id column contains multiple comma-separated IDs.
#'
#' @param table_name Character. Name of the Airtable table to fetch.
#' @param select_cols Character vector. Column names to select from the table.
#' @param form Character. Form ID to filter by.
#' @param conf Configuration object from read_config().
#'
#' @return A filtered and selected data frame from Airtable containing only rows where
#'   the form_id field contains the specified form ID.
#'
#' @details
#' This function uses string detection to handle multi-valued form_id fields that may
#' contain comma-separated lists of form IDs.
#'
#' @keywords preprocessing helper
#' @export
fetch_asset <- function(
  table_name = NULL,
  select_cols = NULL,
  form = NULL,
  conf = NULL
) {
  airtable_to_df(
    base_id = conf$metadata$airtable$base_id,
    table_name = table_name,
    token = conf$metadata$airtable$token
  ) |>
    janitor::clean_names() |>
    # Filter rows where form_id contains the target ID
    dplyr::filter(stringr::str_detect(
      .data$form_id,
      stringr::fixed(form)
    )) |>
    dplyr::select(dplyr::all_of(select_cols))
}

#' Fetch Multiple Asset Tables from Airtable
#'
#' @description
#' Fetches taxa, gear, vessels, and landing sites data from Airtable filtered
#' by the specified form ID. Returns distinct records for each table.
#'
#' @param form_id Character. Form ID to filter assets by. This is passed to each
#'   individual fetch_asset call.
#' @param conf Configuration object from read_config().
#'
#' @return A named list containing four data frames:
#'   \itemize{
#'     \item \code{taxa}: Contains survey_label, alpha3_code, and scientific_name columns
#'     \item \code{gear}: Contains survey_label and standard_name columns
#'     \item \code{vessels}: Contains survey_label and standard_name columns
#'     \item \code{sites}: Contains site and site_code columns
#'   }
#'
#' @details
#' Each table is fetched separately using `fetch_asset()` and filtered to return
#' only distinct rows to avoid duplicates in the mapping tables.
#'
#' @keywords preprocessing helper
#' @export
fetch_assets <- function(form_id = NULL, conf = NULL) {
  assets_list <-
    list(
      taxa = fetch_asset(
        table_name = "taxa",
        select_cols = c("survey_label", "alpha3_code", "scientific_name"),
        form = form_id,
        conf = conf
      ),
      gear = fetch_asset(
        table_name = "gears",
        select_cols = c("survey_label", "standard_name"),
        form = form_id,
        conf = conf
      ),
      vessels = fetch_asset(
        table_name = "vessels",
        select_cols = c("survey_label", "standard_name"),
        form = form_id,
        conf = conf
      ),
      sites = fetch_asset(
        table_name = "landing_sites",
        select_cols = c("site", "site_code"),
        form = form_id,
        conf = conf
      )
    )

  purrr::map(assets_list, ~ dplyr::distinct(.x))
}

#' Get Airtable Form ID from KoBoToolbox Asset ID
#'
#' @description
#' Retrieves the Airtable record ID for a form based on its KoBoToolbox asset ID.
#'
#' @param kobo_asset_id Character. The KoBoToolbox asset ID to match.
#' @param conf Configuration object from read_config().
#'
#' @return Character. The Airtable record ID for the matching form.
#' @keywords preprocessing helper
#' @export
get_airtable_form_id <- function(kobo_asset_id = NULL, conf = NULL) {
  airtable_to_df(
    base_id = conf$metadata$airtable$base_id,
    table_name = "forms",
    token = conf$metadata$airtable$token
  ) |>
    janitor::clean_names() |>
    dplyr::filter(.data$form_id == kobo_asset_id) |>
    dplyr::pull(.data$airtable_id)
}
