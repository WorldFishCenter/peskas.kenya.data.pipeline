#' Download Parquet File from Cloud Storage
#'
#' This function handles the process of downloading a parquet file from cloud storage
#' and reading it into memory.
#'
#' @param prefix The file prefix path in cloud storage
#' @param provider The cloud storage provider key
#' @param options Cloud storage provider options
#'
#' @return A tibble containing the data from the parquet file
#'
#' @examples
#' \dontrun{
#' raw_data <- download_parquet_from_cloud(
#'   prefix = conf$ingestion$koboform$catch$legacy$raw,
#'   provider = conf$storage$google$key,
#'   options = conf$storage$google$options
#' )
#' }
#'
#' @keywords storage
#' @export
download_parquet_from_cloud <- function(prefix, provider, options) {
  # Generate cloud object name
  parquet_file <- cloud_object_name(
    prefix = prefix,
    provider = provider,
    extension = "parquet",
    options = options
  )

  # Log and download file
  logger::log_info("Retrieving {parquet_file}")
  download_cloud_file(
    name = parquet_file,
    provider = provider,
    options = options
  )

  # Read parquet file
  arrow::read_parquet(file = parquet_file)
}

#' Upload Processed Data to Cloud Storage
#'
#' This function handles the process of writing data to a parquet file and
#' uploading it to cloud storage.
#'
#' @param data The data frame or tibble to upload
#' @param prefix The file prefix path in cloud storage
#' @param provider The cloud storage provider key
#' @param options Cloud storage provider options
#' @param compression Compression algorithm to use (default: "lz4")
#' @param compression_level Compression level (default: 12)
#'
#' @return Invisible NULL
#'
#' @keywords storage
#' @examples
#' \dontrun{
#' upload_parquet_to_cloud(
#'   data = processed_data,
#'   prefix = conf$ingestion$koboform$catch$legacy$preprocessed,
#'   provider = conf$storage$google$key,
#'   options = conf$storage$google$options
#' )
#' }
#' @export
upload_parquet_to_cloud <- function(
  data,
  prefix,
  provider,
  options,
  compression = "lz4",
  compression_level = 12
) {
  # Generate filename with version
  preprocessed_filename <- prefix %>%
    add_version(extension = "parquet")

  # Write parquet file
  arrow::write_parquet(
    x = data,
    sink = preprocessed_filename,
    compression = compression,
    compression_level = compression_level
  )

  # Log and upload file
  logger::log_info("Uploading {preprocessed_filename} to cloud storage")
  upload_cloud_file(
    file = preprocessed_filename,
    provider = provider,
    options = options
  )

  invisible(NULL)
}

#' Authenticate to a Cloud Storage Provider
#'
#' This function is primarily used internally by other functions to establish authentication
#' with specified cloud providers such as Google Cloud Services (GCS) or Amazon Web Services (AWS).
#'
#' @param provider A character string specifying the cloud provider ("gcs" or "aws").
#' @param options A named list of options specific to the cloud provider (see details).
#'
#' @details For GCS, the options list must include:
#' - `service_account_key`: The contents of the authentication JSON file from your Google Project.
#'
#' This function wraps [googleCloudStorageR::gcs_auth()] to handle GCS authentication.
#'
#' @export
#' @keywords storage
#' @examples
#' \dontrun{
#' authentication_details <- readLines("path/to/json_file.json")
#' cloud_storage_authenticate("gcs", list(service_account_key = authentication_details))
#' #'
#' }
cloud_storage_authenticate <- function(provider, options) {
  if ("gcs" %in% provider) {
    # Only need to authenticate if there is no token for downstream requests
    if (isFALSE(googleAuthR::gar_has_token())) {
      service_account_key <- options$service_account_key
      temp_auth_file <- tempfile(fileext = "json")
      writeLines(service_account_key, temp_auth_file)
      googleCloudStorageR::gcs_auth(json_file = temp_auth_file)
    }
  }
}

#' Upload File to Cloud Storage
#'
#' Uploads a local file to a specified cloud storage bucket, supporting both single and multiple files.
#'
#' @param file A character vector specifying the path(s) of the file(s) to upload.
#' @param provider A character string specifying the cloud provider ("gcs" or "aws").
#' @param options A named list of provider-specific options including the bucket and authentication details.
#' @param name (Optional) The name to assign to the file in the cloud. If not specified, the local file name is used.
#'
#' @details For GCS, the options list must include:
#' - `bucket`: The name of the bucket to which files are uploaded.
#' - `service_account_key`: The authentication JSON contents, if not previously authenticated.
#'
#' This function utilizes [googleCloudStorageR::gcs_upload()] for file uploads to GCS.
#'
#' @return A list of metadata objects for the uploaded files if successful.
#' @export
#' @keywords storage
#' @examples
#' \dontrun{
#' authentication_details <- readLines("path/to/json_file.json")
#' upload_cloud_file(
#'   "path/to/local_file.csv",
#'   "gcs",
#'   list(service_account_key = authentication_details, bucket = "my-bucket")
#' )
#' }
#'
upload_cloud_file <- function(file, provider, options, name = file) {
  cloud_storage_authenticate(provider, options)

  out <- list()
  if ("gcs" %in% provider) {
    # Iterate over multiple files (and names)
    google_output <- purrr::map2(
      file,
      name,
      ~ googleCloudStorageR::gcs_upload(
        file = .x,
        bucket = options$bucket,
        name = .y,
        predefinedAcl = "bucketLevel"
      )
    )

    out <- c(out, google_output)
  }

  out
}

#' Retrieve Full Name of Versioned Cloud Object
#'
#' Gets the full name(s) of object(s) in cloud storage matching the specified prefix, version, and file extension.
#'
#' @param prefix A string indicating the object's prefix.
#' @param version A string specifying the version ("latest" or a specific version string).
#' @param extension The file extension to filter by. An empty string ("") includes all extensions.
#' @param provider A character string specifying the cloud provider ("gcs" or "aws").
#' @param exact_match A logical indicating whether to match the prefix exactly.
#' @param options A named list of provider-specific options including the bucket and authentication details.
#'
#' @details For GCS, the options list should include:
#' - `bucket`: The bucket name.
#' - `service_account_key`: The authentication JSON contents, if not previously authenticated.
#'
#' @return A vector of names of objects matching the criteria.
#' @export
#' @keywords storage
#' @examples
#' \dontrun{
#' authentication_details <- readLines("path/to/json_file.json")
#' cloud_object_name(
#'   "prefix",
#'   "latest",
#'   "json",
#'   "gcs",
#'   list(service_account_key = authentication_details, bucket = "my-bucket")
#' )
#' #'
#' }
cloud_object_name <- function(
  prefix,
  version = "latest",
  extension = "",
  provider,
  exact_match = FALSE,
  options
) {
  cloud_storage_authenticate(provider, options)

  if ("gcs" %in% provider) {
    gcs_files <- googleCloudStorageR::gcs_list_objects(
      bucket = options$bucket,
      prefix = prefix
    )

    if (nrow(gcs_files) == 0) {
      return(character(0))
    }

    gcs_files_formatted <- gcs_files %>%
      tidyr::separate(
        col = .data$name,
        into = c("base_name", "version", "ext"),
        # Version is separated with the "__" string
        sep = "__",
        remove = FALSE
      ) %>%
      dplyr::filter(stringr::str_detect(.data$ext, paste0(extension, "$"))) %>%
      dplyr::group_by(.data$base_name, .data$ext)

    if (isTRUE(exact_match)) {
      selected_rows <- gcs_files_formatted %>%
        dplyr::filter(.data$base_name == prefix)
    } else {
      selected_rows <- gcs_files_formatted
    }

    if (version == "latest") {
      selected_rows <- selected_rows %>%
        dplyr::filter(max(.data$updated) == .data$updated)
    } else {
      this_version <- version
      selected_rows <- selected_rows %>%
        dplyr::filter(.data$version == this_version)
    }

    selected_rows$name
  }
}


#' Download Object from Cloud Storage
#'
#' Downloads an object from cloud storage to a local file.
#'
#' @param name The name of the object in the storage bucket.
#' @param provider A character string specifying the cloud provider ("gcs" or "aws").
#' @param options A named list of provider-specific options including the bucket and authentication details.
#' @param file (Optional) The local path to save the downloaded object. If not specified, the object name is used.
#'
#' @details For GCS, the options list should include:
#' - `bucket`: The name of the bucket from which the object is downloaded.
#' - `service_account_key`: The authentication JSON contents, if not previously authenticated.
#'
#' @return The path to the downloaded file.
#' @export
#' @keywords storage
#' @examples
#' \dontrun{
#' authentication_details <- readLines("path/to/json_file.json")
#' download_cloud_file(
#'   "object_name.json",
#'   "gcs",
#'   list(service_account_key = authentication_details, bucket = "my-bucket"),
#'   "local_path/to/save/object.json"
#' )
#' }
#'
download_cloud_file <- function(name, provider, options, file = name) {
  cloud_storage_authenticate(provider, options)

  if ("gcs" %in% provider) {
    purrr::map2(
      name,
      file,
      ~ googleCloudStorageR::gcs_get_object(
        object_name = .x,
        bucket = options$bucket,
        saveToDisk = .y,
        overwrite = ifelse(is.null(options$overwrite), TRUE, options$overwrite)
      )
    )
  }

  file
}


#' Retrieve Data from MongoDB
#'
#' This function connects to a MongoDB database and retrieves all documents from a specified collection,
#' maintaining the original column order if available.
#'
#' @param connection_string A character string specifying the MongoDB connection URL. Default is NULL.
#' @param collection_name A character string specifying the name of the collection to query. Default is NULL.
#' @param db_name A character string specifying the name of the database. Default is NULL.
#'
#' @return A data frame containing all documents from the specified collection, with columns ordered
#'         as they were when the data was originally pushed to MongoDB.
#'
#' @keywords storage
#'
#' @examples
#' \dontrun{
#' # Retrieve data from a MongoDB collection
#' result <- mdb_collection_pull(
#'   connection_string = "mongodb://localhost:27017",
#'   collection_name = "my_collection",
#'   db_name = "my_database"
#' )
#' }
#'
#' @export
mdb_collection_pull <- function(
  connection_string = NULL,
  collection_name = NULL,
  db_name = NULL
) {
  # Connect to the MongoDB collection
  collection <- mongolite::mongo(
    collection = collection_name,
    db = db_name,
    url = connection_string
  )

  # Retrieve the metadata document
  metadata <- collection$find(query = '{"type": "metadata"}')

  # Retrieve all data documents - explicitly include _id field
  data <- collection$find(
    query = '{"type": {"$ne": "metadata"}}',
    fields = '{}'
  )

  # Alternative approach if the above doesn't work:
  # data <- collection$find(query = '{"type": {"$ne": "metadata"}}', fields = '{"_id": 1}')

  if (nrow(metadata) > 0 && "columns" %in% names(metadata)) {
    stored_columns <- metadata$columns[[1]]

    # Ensure all stored columns exist in the data
    for (col in stored_columns) {
      if (!(col %in% names(data))) {
        data[[col]] <- NA
      }
    }

    # Include _id in the column ordering (put it first)
    if ("_id" %in% names(data)) {
      stored_columns_with_id <- c("_id", stored_columns)
      # Reorder columns to include _id first, then stored columns, then any extra columns
      data <- data[, c(
        stored_columns_with_id,
        setdiff(names(data), stored_columns_with_id)
      )]
    } else {
      # Reorder columns to match stored order, and include any extra columns at the end
      data <- data[, c(stored_columns, setdiff(names(data), stored_columns))]
    }
  } else {
    # If no metadata, at least ensure _id comes first if it exists
    if ("_id" %in% names(data)) {
      other_cols <- setdiff(names(data), "_id")
      data <- data[, c("_id", other_cols)]
    }
  }

  return(data)
}
#' Upload Data to MongoDB and Overwrite Existing Content
#'
#' This function connects to a MongoDB database, removes all existing documents
#' from a specified collection, and then inserts new data. It also stores the
#' original column order to maintain data structure consistency.
#'
#' @param data A data frame containing the data to be uploaded.
#' @param connection_string A character string specifying the MongoDB connection URL.
#' @param collection_name A character string specifying the name of the collection.
#' @param db_name A character string specifying the name of the database.
#'
#' @return The number of data documents inserted into the collection (excluding the order document).
#'
#' @keywords storage
#'
#' @examples
#' \dontrun{
#' # Upload and overwrite data in a MongoDB collection
#' result <- mdb_collection_push(
#'   data = processed_legacy_landings,
#'   connection_string = "mongodb://localhost:27017",
#'   collection_name = "my_collection",
#'   db_name = "my_database"
#' )
#' }
#'
#' @export
mdb_collection_push <- function(
  data = NULL,
  connection_string = NULL,
  collection_name = NULL,
  db_name = NULL
) {
  # Connect to the MongoDB collection
  collection <- mongolite::mongo(
    collection = collection_name,
    db = db_name,
    url = connection_string
  )

  # Remove all existing documents in the collection
  collection$remove("{}")

  # Create a metadata document with column information
  metadata <- list(
    type = "metadata",
    columns = names(data),
    timestamp = Sys.time()
  )

  # Insert the metadata document first
  collection$insert(metadata)

  # Insert the new data
  collection$insert(data)

  # Return the number of documents in the collection (excluding the metadata document)
  return(collection$count() - 1)
}

#' Get metadata tables
#'
#' Get Metadata tables from Google sheets. This function downloads
#' the tables include information about the fishery.
#'
#' The parameters needed in `conf.yml` are:
#'
#' ```
#' storage:
#'   storage_name:
#'     key:
#'     options:
#'       project:
#'       bucket:
#'       service_account_key:
#' ```
#'
#' @param log_threshold The logging threshold level. Default is logger::DEBUG.
#'
#' @export
#' @keywords storage
#'
#' @examples
#' \dontrun{
#' # Ensure you have the necessary configuration in conf.yml
#' metadata_tables <- get_metadata()
#' }
get_metadata <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  pars <- read_config()

  logger::log_info("Authenticating for google drive")
  googlesheets4::gs4_auth(
    path = pars$storage$google$options$service_account_key,
    use_oob = TRUE
  )
  logger::log_info("Downloading metadata tables")

  tables <-
    pars$metadata$google_sheets$tables %>%
    rlang::set_names() %>%
    purrr::map(
      ~ googlesheets4::range_read(
        ss = pars$metadata$google_sheets$sheet_id,
        sheet = .x,
        col_types = "c"
      )
    )

  tables
}
