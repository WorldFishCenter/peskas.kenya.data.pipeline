conf <- read_config()
db_name <- conf$storage$mongodb$cluster$dashboard$database
connection_string <- conf$storage$mongodb$cluster$dashboard$connection_string

# MongoDB User Management Functions - Version 2
# For adding users from pre-existing credentials CSV file

# Required packages
library(mongolite)
library(dplyr)
library(bcrypt)
library(jsonlite)
library(readr)

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

# Function to read and parse the credentials CSV
read_credentials_csv <- function(csv_path = "inst/current_login_list.csv") {
  # Read the CSV file
  credentials <- readr::read_csv(csv_path, show_col_types = FALSE) |>
    dplyr::mutate(password = as.character(password))

  # Clean column names and standardize
  colnames(credentials) <- c(
    "name",
    "email",
    "password",
    "fisher_id",
    "role",
    "user_bmu",
    "treatment_type"
  )

  # Filter out header row if it exists
  credentials <- credentials %>%
    dplyr::filter(!is.na(email) & email != "Email") %>%
    dplyr::mutate(
      # Standardize the role mapping
      treatment = case_when(
        role == "AIA" ~ "AIA",
        role == "IIA" ~ "IIA",
        role == "CIA" ~ "CIA",
        role == "WBCIA" ~ "WBCIA",
        role == "CCNF" ~ "Control", # Map CCNF to Control
        TRUE ~ role
      ),
      # Clean BMU names to match existing format
      BMU = stringr::str_to_title(user_bmu)
    ) %>%
    # Remove rows with empty fisher_id (BMU admin rows)
    dplyr::filter(!is.na(fisher_id) & fisher_id != "")

  message(sprintf("Loaded %d user credentials from CSV", nrow(credentials)))

  return(credentials)
}

# Updated helper function to get BMUs based on treatment type (same logic as v1)
get_bmus_for_treatment_v2 <- function(
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

# Function to validate and check for duplicates before adding
check_duplicates_v2 <- function(credentials_df, existing_users) {
  # Check for duplicate fisher_ids in the input
  duplicates_in_input <- credentials_df %>%
    dplyr::group_by(fisher_id) %>%
    dplyr::filter(dplyr::n() > 1) %>%
    dplyr::pull(fisher_id) %>%
    unique()

  if (length(duplicates_in_input) > 0) {
    warning(sprintf(
      "Found %d duplicate fisher_ids in CSV: %s",
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
  already_exists <- credentials_df$fisher_id[
    credentials_df$fisher_id %in% existing_fisher_ids
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

# Main execution function for CSV-based user addition
add_fishers_from_csv <- function(
  csv_path = "inst/current_login_list.csv",
  connection_string,
  db_name,
  dry_run = FALSE,
  hash_passwords = TRUE, # Whether to hash the passwords from CSV
  existing_users = NULL,
  existing_groups = NULL,
  existing_bmus = NULL
) {
  message("Starting CSV-based user addition process...")

  # Read credentials from CSV
  credentials_df <- read_credentials_csv(csv_path)

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
        collection = "bmu",
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
  duplicate_check <- check_duplicates_v2(credentials_df, existing_users)

  if (dry_run) {
    message("DRY RUN MODE - No changes will be made to the database")

    # Filter out existing users
    new_credentials <- credentials_df %>%
      dplyr::filter(!fisher_id %in% duplicate_check$already_exists)

    message(sprintf("Would add %d new users from CSV", nrow(new_credentials)))

    # Show sample of what would be added
    if (nrow(new_credentials) > 0) {
      message("\nSample of users to be added:")
      sample_users <- head(new_credentials, 5)
      print(sample_users %>% select(name, email, fisher_id, treatment, BMU))
    }

    # Show treatment breakdown
    treatment_summary <- new_credentials %>%
      count(treatment, name = "count")
    message("\nTreatment breakdown:")
    print(treatment_summary)

    return(invisible(NULL))
  }

  # Actually add the users
  result <- mdb_add_users_from_csv(
    csv_path = csv_path,
    connection_string = connection_string,
    db_name = db_name,
    existing_users = existing_users,
    existing_groups = existing_groups,
    existing_bmus = existing_bmus,
    hash_passwords = hash_passwords
  )

  message(sprintf(
    "\nProcess completed. Added %d new users from CSV.",
    result$count
  ))

  return(result)
}

# Function to add users from CSV to MongoDB collection
mdb_add_users_from_csv <- function(
  csv_path = "inst/current_login_list.csv",
  connection_string,
  db_name,
  existing_users = NULL,
  existing_groups = NULL,
  existing_bmus = NULL,
  hash_passwords = TRUE # Whether to hash the plain passwords from CSV
) {
  # Connect to users collection for INSERTION
  users_mongo_connection <- mongolite::mongo(
    collection = "users",
    db = db_name,
    url = connection_string
  )

  # Read credentials from CSV
  credentials_df <-
    read_credentials_csv(csv_path) |>
    dplyr::mutate(fisher_id = tolower(fisher_id)) |>
    dplyr::filter(role %in% c("CCNF"))

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
  new_credentials <- credentials_df %>%
    dplyr::filter(!fisher_id %in% existing_fisher_ids)

  if (nrow(new_credentials) == 0) {
    message("No new users to add. All fishers already exist in the database.")
    return(list(count = 0, users_added = NULL))
  }

  message(sprintf("Processing %d new users from CSV", nrow(new_credentials)))

  # new_credentials <- new_credentials %>%
  #   dplyr::filter(fisher_id %in% c("mtwa6710"))

  # Prepare new user documents for insertion
  new_users_for_insert <- list()
  users_added_record <- data.frame() # Keep record of users added

  for (i in 1:nrow(new_credentials)) {
    user_row <- new_credentials[i, ]

    # Generate proper MongoDB ObjectId
    user_id <- generate_mongodb_objectid()

    # Use password from CSV (hash it if requested)
    user_password <- if (hash_passwords) {
      bcrypt::hashpw(user_row$password)
    } else {
      user_row$password
    }

    # Get the appropriate BMUs based on treatment
    user_bmus <- get_bmus_for_treatment_v2(
      treatment = user_row$treatment,
      user_bmu = user_row$BMU,
      bmu_to_id = bmu_to_id,
      existing_bmus = existing_bmus
    )

    # Get group ID - ensure it's not NULL
    group_id <- treatment_to_group[[user_row$treatment]]
    if (is.null(group_id) || length(group_id) == 0) {
      warning(sprintf("No group found for treatment: %s", user_row$treatment))
      next # Skip this user
    }

    # Get user BMU ID
    user_bmu_id <- bmu_to_id[[user_row$BMU]]
    if (is.null(user_bmu_id) || length(user_bmu_id) == 0) {
      warning(sprintf(
        "No BMU ID found for: '%s'. Available BMUs: %s",
        user_row$BMU,
        paste(head(names(bmu_to_id), 10), collapse = ", ")
      ))
      user_bmu_id <- NA_character_
    }

    # Prepare BMUs array
    if (is.null(user_bmus) || length(user_bmus) == 0) {
      bmus_array <- list()
    } else {
      bmus_array <- as.list(user_bmus)
    }

    # Create user document with proper structure
    user_doc <- list(
      email = user_row$email,
      `__v` = 0L,
      bmus = if (length(bmus_array) > 0) {
        as.list(bmus_array) # Store as plain strings, not ObjectId wrappers
      } else {
        list()
      },
      created_at = jsonlite::unbox(as.POSIXct(Sys.time(), tz = "UTC")),
      fisherId = user_row$fisher_id,
      groups = as.list(group_id), # Store as plain strings, not ObjectId wrappers
      name = user_row$name,
      password = user_password,
      status = "active",
      userBmu = if (is.na(user_bmu_id)) NULL else user_bmu_id # Store as plain string
    )

    # Add to list for insertion
    new_users_for_insert[[i]] <- user_doc

    # Store record of user added
    users_added_record <- rbind(
      users_added_record,
      data.frame(
        name = user_row$name,
        fisher_id = user_row$fisher_id,
        email = user_row$email,
        treatment = user_row$treatment,
        bmu = user_row$BMU,
        original_password = user_row$password,
        created_at = Sys.time(),
        stringsAsFactors = FALSE
      )
    )
  }

  # Remove any NULL entries (from skipped users)
  new_users_for_insert <- new_users_for_insert[
    !sapply(new_users_for_insert, is.null)
  ]

  # Insert new users if we have any
  if (length(new_users_for_insert) > 0) {
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

    # Bulk insert
    result <- users_mongo_connection$insert(json_docs)
    success_count <- length(json_docs)

    message(sprintf("Successfully added %d new users from CSV", success_count))

    return(list(
      count = success_count,
      users_added = users_added_record
    ))
  }

  return(list(count = 0, users_added = NULL))
}

# Function to replace existing users (delete and recreate)
replace_existing_users <- function(
  csv_path = "inst/current_login_list.csv",
  connection_string,
  db_name,
  fisher_ids_to_replace = NULL, # Specific fisher IDs to replace, NULL for all
  dry_run = FALSE,
  hash_passwords = TRUE
) {
  message("Starting user replacement process...")

  # Connect to users collection
  users_mongo_connection <- mongolite::mongo(
    collection = "users",
    db = db_name,
    url = connection_string
  )

  # Read credentials from CSV
  credentials_df <- read_credentials_csv(csv_path) %>%
    dplyr::mutate(fisher_id = tolower(fisher_id)) %>%
    dplyr::filter(role %in% c("CCNF"))

  # Filter to specific fisher IDs if provided
  if (!is.null(fisher_ids_to_replace)) {
    credentials_df <- credentials_df %>%
      dplyr::filter(fisher_id %in% fisher_ids_to_replace)
    message(sprintf("Replacing %d specific users", nrow(credentials_df)))
  }

  if (nrow(credentials_df) == 0) {
    message("No users to replace")
    return(list(deleted = 0, added = 0))
  }

  # Get existing data
  existing_users <- mdb_collection_pull(
    collection = "users",
    db = db_name,
    connection_string = connection_string
  )

  existing_groups <- mdb_collection_pull(
    collection = "groups",
    db = db_name,
    connection_string = connection_string
  )

  existing_bmus <- mdb_collection_pull(
    collection = "bmu",
    db = db_name,
    connection_string = connection_string
  )

  # Find users that exist and need to be replaced
  existing_fisher_ids <- existing_users$fisherId[
    !is.na(existing_users$fisherId)
  ]
  users_to_replace <- credentials_df %>%
    dplyr::filter(fisher_id %in% existing_fisher_ids)

  message(sprintf("Found %d existing users to replace", nrow(users_to_replace)))

  if (dry_run) {
    message("DRY RUN MODE - No changes will be made")
    message("Users that would be replaced:")
    print(users_to_replace %>% select(name, fisher_id, treatment, BMU))
    return(invisible(NULL))
  }

  # Delete existing users
  deleted_count <- 0
  for (fisher_id in users_to_replace$fisher_id) {
    delete_result <- users_mongo_connection$remove(
      sprintf('{"fisherId": "%s"}', fisher_id)
    )
    # mongolite::remove() returns a simple count, not a list
    if (delete_result > 0) {
      deleted_count <- deleted_count + delete_result
      message(sprintf("Deleted user: %s", fisher_id))
    }
  }

  message(sprintf("Deleted %d existing users", deleted_count))

  # Now add the new users using the existing function
  # Create a temporary CSV with just the users we want to replace
  if (deleted_count > 0) {
    result <- mdb_add_users_from_csv(
      csv_path = csv_path,
      connection_string = connection_string,
      db_name = db_name,
      existing_users = existing_users[
        !existing_users$fisherId %in% users_to_replace$fisher_id,
      ],
      existing_groups = existing_groups,
      existing_bmus = existing_bmus,
      hash_passwords = hash_passwords
    )

    message(sprintf(
      "Replacement completed. Deleted: %d, Added: %d",
      deleted_count,
      result$count
    ))
    return(list(
      deleted = deleted_count,
      added = result$count,
      users_added = result$users_added
    ))
  } else {
    return(list(deleted = 0, added = 0))
  }
}

#### Example usage ####

# Load configuration
conf <- read_config()


readr::read_csv("inst/current_login_list.csv", show_col_types = FALSE) |>
  dplyr::filter(user_bmu == "Ngomeni") |>
  readr::write_csv("inst/current_login_list.csv")

# Run in dry-run mode first to validate
add_fishers_from_csv(
  csv_path = "inst/current_login_list.csv",
  connection_string = conf$storage$mongodb$cluster$dashboard$connection_string,
  db_name = conf$storage$mongodb$cluster$dashboard$database,
  dry_run = FALSE,
  hash_passwords = TRUE
)
