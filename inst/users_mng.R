conf <- read_config()
db_name <- conf$storage$mongodb$database$dashboard$name
connection_string <- conf$storage$mongodb$connection_string

# MongoDB User Management Functions
# For adding new users with nested BMU information based on treatment groups

# Required packages
library(mongolite)
library(dplyr)
library(bcrypt)
library(jsonlite)

# Function to generate random 5-character password (alphanumeric only)
generate_password <- function(length = 5) {
  # Use letters (upper and lower) and numbers, no special symbols
  chars <- c(letters, LETTERS, 0:9)
  paste0(sample(chars, length, replace = TRUE), collapse = "")
}

# Function to generate proper MongoDB ObjectId
generate_mongodb_objectid <- function() {
  # Generate a valid 24-character hex string for MongoDB ObjectId
  # Format: 4-byte timestamp + 3-byte machine + 2-byte pid + 3-byte counter

  # Current timestamp in hex (8 characters)
  timestamp <- sprintf("%08x", as.integer(Sys.time()))

  # Random machine identifier (6 characters)
  machine <- paste0(
    sample(c(0:9, letters[1:6]), 6, replace = TRUE),
    collapse = ""
  )

  # Random process id (4 characters)
  pid <- paste0(sample(c(0:9, letters[1:6]), 4, replace = TRUE), collapse = "")

  # Random counter (6 characters)
  counter <- paste0(
    sample(c(0:9, letters[1:6]), 6, replace = TRUE),
    collapse = ""
  )

  # Combine to create 24-character hex string
  object_id <- paste0(timestamp, machine, pid, counter)

  # Ensure exactly 24 characters
  object_id <- substr(object_id, 1, 24)

  return(object_id)
}

as_oid <- function(hex) list(`$oid` = unname(hex))

# Function to add new users to MongoDB collection
mdb_add_users <- function(
  treatments_df,
  connection_string,
  db_name,
  existing_users = NULL,
  existing_groups = NULL,
  existing_bmus = NULL,
  store_passwords = TRUE # Option to save plain passwords for distribution
) {
  # Connect to users collection for INSERTION (not fetching)
  users_mongo_connection <- mongolite::mongo(
    collection = "users",
    db = db_name,
    url = connection_string
  )

  # If data not provided, fetch it
  if (is.null(existing_users)) {
    existing_users <- mdb_collection_pull(
      collection = "users",
      db = db_name,
      connection_string = connection_string
    )
  }

  if (is.null(existing_groups)) {
    existing_groups <- mdb_collection_pull(
      collection = "groups",
      db = db_name,
      connection_string = connection_string
    )
  }

  if (is.null(existing_bmus)) {
    existing_bmus <- mdb_collection_pull(
      collection = "bmu",
      db = db_name,
      connection_string = connection_string
    )
  }

  # Validate data
  if (!"_id" %in% names(existing_groups)) {
    stop("Missing '_id' column in groups data")
  }

  if (!"_id" %in% names(existing_bmus)) {
    stop("Missing '_id' column in bmus data")
  }

  # Create a mapping of treatment to group ID
  treatment_to_group <- list(
    "IIA" = existing_groups$`_id`[existing_groups$name == "IIA"],
    "CIA" = existing_groups$`_id`[existing_groups$name == "CIA"],
    "WBCIA" = existing_groups$`_id`[existing_groups$name == "WBCIA"],
    "AIA" = existing_groups$`_id`[existing_groups$name == "AIA"],
    "Admin" = existing_groups$`_id`[existing_groups$name == "Admin"],
    "Control" = existing_groups$`_id`[existing_groups$name == "Control"]
  )

  # Create a mapping of BMU names to IDs
  bmu_to_id <- setNames(existing_bmus$`_id`, existing_bmus$BMU)

  # Debug: show available BMUs
  message(sprintf("Found %d BMU mappings", length(bmu_to_id)))

  # Filter out fishers that already exist
  existing_fisher_ids <- existing_users$fisherId[
    !is.na(existing_users$fisherId)
  ]
  new_treatments <- treatments_df %>%
    dplyr::filter(!fisher_id %in% existing_fisher_ids)

  if (nrow(new_treatments) == 0) {
    message("No new users to add. All fishers already exist in the database.")
    return(list(count = 0, passwords = NULL))
  }

  # Prepare new user documents for insertion
  new_users_for_insert <- list()
  passwords_record <- data.frame() # Store passwords for distribution if needed

  for (i in 1:nrow(new_treatments)) {
    fisher <- new_treatments[i, ]

    # Generate proper MongoDB ObjectId
    user_id <- generate_mongodb_objectid()

    # Generate 5-character alphanumeric password
    plain_password <- generate_password(5)

    # Get the appropriate BMUs based on treatment
    user_bmus <- get_bmus_for_treatment(
      treatment = fisher$treatment,
      user_bmu = fisher$BMU,
      bmu_to_id = bmu_to_id,
      existing_bmus = existing_bmus
    )

    # Get group ID - ensure it's not NULL
    group_id <- treatment_to_group[[fisher$treatment]]
    if (is.null(group_id) || length(group_id) == 0) {
      warning(sprintf("No group found for treatment: %s", fisher$treatment))
      next # Skip this user
    }

    # Get user BMU ID
    user_bmu_id <- bmu_to_id[[fisher$BMU]]
    if (is.null(user_bmu_id) || length(user_bmu_id) == 0) {
      warning(sprintf(
        "No BMU ID found for: '%s'. Available BMUs: %s",
        fisher$BMU,
        paste(head(names(bmu_to_id), 10), collapse = ", ")
      ))
      user_bmu_id <- NA_character_ # Use NA instead of empty string
    }

    # Prepare BMUs array - ensure it's properly formatted
    if (is.null(user_bmus) || length(user_bmus) == 0) {
      bmus_array <- list()
    } else {
      bmus_array <- as.list(user_bmus)
    }

    # Create user document with proper structure
    user_doc <- list(
      email = paste0(fisher$fisher_id, "@fisher.ke"),
      `__v` = 0L,
      bmus = if (length(bmus_array) > 0) {
        lapply(as.list(bmus_array), as_oid)
      } else {
        list()
      }, # array<ObjectId>
      created_at = jsonlite::unbox(as.POSIXct(Sys.time(), tz = "UTC")),
      fisherId = fisher$fisher_id,
      groups = lapply(list(group_id), as_oid), # array<ObjectId> (single element)
      name = fisher$fisher_id,
      password = bcrypt::hashpw(plain_password),
      status = "active",
      userBmu = if (is.na(user_bmu_id)) NULL else as_oid(user_bmu_id) # ObjectId or NULL
    )

    # Add to list for insertion
    new_users_for_insert[[i]] <- user_doc

    # Store password record for distribution
    if (store_passwords) {
      passwords_record <- rbind(
        passwords_record,
        data.frame(
          fisher_id = fisher$fisher_id,
          email = paste0(fisher$fisher_id, "@fisher.ke"),
          password = plain_password,
          treatment = fisher$treatment,
          bmu = fisher$BMU,
          created_at = Sys.time(),
          stringsAsFactors = FALSE
        )
      )
    }
  }

  # Remove any NULL entries (from skipped users)
  new_users_for_insert <- new_users_for_insert[
    !sapply(new_users_for_insert, is.null)
  ]

  # Insert new users if we have any
  if (length(new_users_for_insert) > 0) {
    # Convert list of documents to a dataframe for bulk insert
    success_count <- 0

    json_docs <- vapply(
      new_users_for_insert,
      function(doc) {
        jsonlite::toJSON(
          doc,
          auto_unbox = TRUE,
          null = "null",
          POSIXt = "mongo"
        )
      },
      FUN.VALUE = character(1)
    )

    # --- bulk insert ---
    success_count <- 0
    result <- users_mongo_connection$insert(json_docs)
    success_count <- length(json_docs)
    message(sprintf(
      "Successfully added %d new users (JSON bulk insert)",
      success_count
    ))

    if (success_count > 0) {
      message(sprintf("Successfully added %d new users", success_count))
    } else {
      warning("No users were successfully added to the database")
    }

    # Save passwords to CSV if requested
    if (store_passwords && nrow(passwords_record) > 0) {
      filename <- paste0(
        "user_passwords_",
        format(Sys.time(), "%Y%m%d_%H%M%S"),
        ".csv"
      )
      write.csv(passwords_record, filename, row.names = FALSE)
      message(sprintf("Passwords saved to: %s", filename))
      message(
        "*** IMPORTANT: Store this file securely and delete after distributing passwords ***"
      )
    }

    return(list(
      count = success_count,
      passwords = if (store_passwords) passwords_record else NULL
    ))
  }

  return(list(count = 0, passwords = NULL))
}

# Helper function to get BMUs based on treatment type
get_bmus_for_treatment <- function(
  treatment,
  user_bmu,
  bmu_to_id,
  existing_bmus
) {
  # Check if bmu_to_id is valid
  if (is.null(bmu_to_id) || length(bmu_to_id) == 0) {
    warning("BMU to ID mapping is empty")
    return(character(0))
  }

  bmus_list <- switch(
    treatment,
    "WBCIA" = {
      # WBCIA always gets these specific 5 BMUs regardless of their own BMU
      wbcia_bmu_ids <- c(
        "674ed4b0ddf733702692c65d", # Jimbo
        "674ed4b1ddf733702692c65f", # Kibuyuni
        "674ed4b1ddf733702692c660", # Mkwiro
        "674ed4b1ddf733702692c662", # Vanga
        "674ed4b1ddf733702692c664" # Wasini
      )
      selected_ids <- wbcia_bmu_ids
      if (length(selected_ids) == 0) {
        warning(sprintf(
          "No WBCIA BMUs found. Available BMUs: %s",
          paste(names(bmu_to_id)[1:min(5, length(bmu_to_id))], collapse = ", ")
        ))
      }
      as.list(selected_ids)
    },
    "CIA" = {
      # CIA gets just their own BMU
      if (!user_bmu %in% names(bmu_to_id)) {
        warning(sprintf("BMU '%s' not found in BMU list", user_bmu))
        list()
      } else {
        list(bmu_to_id[[user_bmu]])
      }
    },
    "IIA" = {
      # IIA gets just their own BMU
      if (!user_bmu %in% names(bmu_to_id)) {
        warning(sprintf("BMU '%s' not found in BMU list", user_bmu))
        list()
      } else {
        list(bmu_to_id[[user_bmu]])
      }
    },
    "AIA" = {
      # AIA gets just their own BMU
      if (!user_bmu %in% names(bmu_to_id)) {
        warning(sprintf("BMU '%s' not found in BMU list", user_bmu))
        list()
      } else {
        list(bmu_to_id[[user_bmu]])
      }
    },
    "Control" = {
      # Control group - just their own BMU
      if (!user_bmu %in% names(bmu_to_id)) {
        warning(sprintf("BMU '%s' not found in BMU list", user_bmu))
        list()
      } else {
        list(bmu_to_id[[user_bmu]])
      }
    },
    "Admin" = {
      # Admin gets all BMUs
      as.list(bmu_to_id)
    },
    {
      # Default: just the user's BMU
      if (!user_bmu %in% names(bmu_to_id)) {
        warning(sprintf(
          "BMU '%s' not found in BMU list for treatment '%s'",
          user_bmu,
          treatment
        ))
        list()
      } else {
        list(bmu_to_id[[user_bmu]])
      }
    }
  )

  # Filter out NULLs and return as character vector
  result <- unlist(bmus_list[!sapply(bmus_list, is.null)])

  # Return empty character vector if no BMUs found
  if (is.null(result)) {
    return(character(0))
  }

  return(result)
}

# Function to update existing users (if needed)
mdb_update_user <- function(
  fisher_id,
  update_fields,
  connection_string,
  db_name
) {
  users_collection <- mongolite::mongo(
    collection = "users",
    db = db_name,
    url = connection_string
  )

  # Create the update query
  query <- sprintf('{"fisherId": "%s"}', fisher_id)

  # Create the update document
  update <- jsonlite::toJSON(list(`$set` = update_fields), auto_unbox = TRUE)

  # Perform the update
  result <- users_collection$update(query, update)

  return(result)
}

# Function to validate and check for duplicates before adding
check_duplicates <- function(treatments_df, existing_users) {
  # Check for duplicate fisher_ids in the input
  duplicates_in_input <- treatments_df %>%
    dplyr::group_by(fisher_id) %>%
    dplyr::filter(dplyr::n() > 1) %>%
    dplyr::pull(fisher_id) %>%
    unique()

  if (length(duplicates_in_input) > 0) {
    warning(sprintf(
      "Found %d duplicate fisher_ids in input: %s",
      length(duplicates_in_input),
      paste(
        duplicates_in_input[1:min(5, length(duplicates_in_input))],
        collapse = ", "
      )
    ))
  }

  # Check for existing fisher_ids in database
  existing_fisher_ids <- existing_users$fisherId[
    !is.na(existing_users$fisherId)
  ]
  already_exists <- treatments_df$fisher_id[
    treatments_df$fisher_id %in% existing_fisher_ids
  ]

  if (length(already_exists) > 0) {
    message(sprintf(
      "Found %d fisher_ids that already exist in database: %s",
      length(already_exists),
      paste(already_exists[1:min(5, length(already_exists))], collapse = ", ")
    ))
  }

  return(list(
    duplicates_in_input = duplicates_in_input,
    already_exists = already_exists
  ))
}

# Main execution function
add_fishers_to_mongodb <- function(
  treatments_df,
  connection_string,
  db_name,
  dry_run = FALSE,
  store_passwords = TRUE, # Save passwords to CSV for distribution
  existing_users = NULL, # Optional: pass existing data
  existing_groups = NULL, # Optional: pass existing data
  existing_bmus = NULL # Optional: pass existing data
) {
  message("Starting user addition process...")

  # Fetch existing data if not provided
  if (
    is.null(existing_users) ||
      is.null(existing_groups) ||
      is.null(existing_bmus)
  ) {
    message("Fetching existing data from MongoDB...")

    if (is.null(existing_users)) {
      existing_users <- mdb_collection_pull(
        collection = "users",
        db = db_name,
        connection_string = connection_string
      )
    }

    if (is.null(existing_groups)) {
      existing_groups <- mdb_collection_pull(
        collection = "groups",
        db = db_name,
        connection_string = connection_string
      )
    }

    if (is.null(existing_bmus)) {
      existing_bmus <- mdb_collection_pull(
        collection = "bmus",
        db = db_name,
        connection_string = connection_string
      )
    }
  }

  message(sprintf(
    "Found %d existing users, %d groups, %d BMUs",
    nrow(existing_users),
    nrow(existing_groups),
    nrow(existing_bmus)
  ))

  # Check for duplicates
  duplicate_check <- check_duplicates(treatments_df, existing_users)

  if (dry_run) {
    message("DRY RUN MODE - No changes will be made to the database")

    # Filter out existing users
    new_fishers <- treatments_df %>%
      dplyr::filter(!fisher_id %in% duplicate_check$already_exists)

    message(sprintf("Would add %d new users", nrow(new_fishers)))

    # Show sample of what would be added
    if (nrow(new_fishers) > 0) {
      message("\nSample of users to be added:")
      sample_users <- head(new_fishers, 5)

      # Generate sample passwords to show format
      for (i in 1:nrow(sample_users)) {
        sample_users$sample_password[i] <- generate_password(5)
        sample_users$email[i] <- paste0(sample_users$fisher_id[i], "@fisher.ke")
      }
      print(sample_users)
    }

    return(invisible(NULL))
  }

  # Actually add the users
  result <- mdb_add_users(
    treatments_df = treatments_df,
    connection_string = connection_string,
    db_name = db_name,
    existing_users = existing_users,
    existing_groups = existing_groups,
    existing_bmus = existing_bmus,
    store_passwords = store_passwords
  )

  message(sprintf("\nProcess completed. Added %d new users.", result$count))

  return(result)
}

#### helpful functions ####

# conf <- read_config()

# bmus_info <-
#   get_metadata()$BMUs

# valid_data <-
#   download_parquet_from_cloud(
#     prefix = conf$surveys$catch$validated$file_prefix,
#     provider = conf$storage$google$key,
#     options = conf$storage$google$options
#   ) |>
#   dplyr::filter(.data$landing_date >= "2023-01-01")

# treatments <-
#   valid_data |>
#   dplyr::select(BMU = landing_site, fisher_id) |>
#   dplyr::mutate(BMU = stringr::str_to_title(.data$BMU)) |>
#   dplyr::filter(!is.na(.data$BMU), !is.na(.data$fisher_id)) |>
#   dplyr::distinct() |>
#   dplyr::left_join(bmus_info, by = "BMU") |>
#   dplyr::select(BMU, treatment, fisher_id) |>
#   dplyr::filter(!is.na(.data$treatment), !is.na(.data$fisher_id)) |>
#   dplyr::distinct()

# treatments_df <-
#   treatments |>
#   dplyr::filter(treatment == "WBCIA")

# mdb_add_users(
#   treatments_df = treatments_df,
#   connection_string = conf$storage$mongodb$connection_string,
#   db_name = conf$storage$mongodb$database$dashboard$name
# )

# m <- mongo(
#   collection = "users",
#   db = conf$storage$mongodb$database$dashboard$name,
#   url = conf$storage$mongodb$connection_string
# )

# # 1) Get the latest 5 _ids (newest first)
# res <- m$find(
#   query = '{}',
#   fields = '{"_id": 1}', # include _id only
#   sort = '{"_id": -1}', # newest first
#   limit = 10
# )

# # _id often comes back as a list like list("$oid"="..."):
# oid_hex <- function(x) {
#   if (is.list(x) && !is.null(x$`$oid`)) x$`$oid` else as.character(x)
# }
# ids <- vapply(res$`_id`, oid_hex, character(1))

# # 2) Delete them (exactly 5)
# invisible(lapply(ids, function(id) {
#   m$remove(sprintf('{"_id": {"$oid": "%s"}}', id), just_one = TRUE)
# }))
