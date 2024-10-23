#' Preprocess Landings Data
#'
#' This function preprocesses raw landings data from a MongoDB collection.
#' It performs various data cleaning and transformation operations, including
#' column renaming, data pivoting, and standardization of catch names.
#'
#' @param log_threshold Logging threshold level (default: logger::DEBUG)
#'
#' @return A tibble containing the preprocessed landings data
#'
#' @details
#' The function performs the following main operations:
#' 1. Pulls raw data from the MongoDB collection
#' 2. Renames columns and selects relevant fields
#' 3. Generates unique survey IDs
#' 4. Cleans and standardizes text fields
#' 5. Pivots catch data from wide to long format
#' 6. Standardizes catch names and separates size information
#' 7. Converts data types and handles cases with no catch data
#' 7. Uploads the processed data to the preprocessed MongoDB collection.
#'
#' @keywords workflow preprocessing
#' @examples
#' \dontrun{
#' preprocessed_data <- preprocess_landings()
#' }
#' @export
preprocess_landings <- function(log_threshold = logger::DEBUG) {
  conf <- read_config()

  # get raw landings from mongodb
  raw_dat <- mdb_collection_pull(
    collection_name = conf$storage$mongodb$database$pipeline$collection_name$ongoing$raw,
    db_name = conf$storage$mongodb$database$pipeline$name,
    connection_string = conf$storage$mongodb$connection_string
  ) |>
    dplyr::as_tibble()

  # Rename columns and select relevant fields
  renamed_raw <-
    raw_dat |>
    dplyr::rename_with(~ stringr::str_remove(., "group_xp36r82/")) %>%
    dplyr::select(
      "submission_id",
      form_consent = "Good_day_my_name_is_and_I_r",
      landing_site = "Tambua_BMU",
      landing_date = "Tambua_tarehe",
      fishing_ground = "Eneo_la_uvuvi",
      gear = "Aina_ya_zana_ya_uvuvi",
      no_of_fishers = "Number_of_fishermen",
      n_boats = "Number_of_boats",
      gps = "Rekodi_GPS",
      dplyr::contains("_kg"),
      total_catch_form = "Pato_kwa_ujumla_kg"
    ) %>%
    dplyr::rowwise() %>%
    # Generate unique survey IDs and clean text fields
    dplyr::ungroup() %>%
    dplyr::mutate(
      landing_site = tolower(.data$landing_site),
      fishing_ground = tolower(.data$fishing_ground),
      landing_site = trimws(.data$landing_site),
      fishing_ground = trimws(.data$fishing_ground)
    ) %>%
    tidyr::separate(.data$gps,
      into = c("lat", "lon", "drop1", "drop2"),
      sep = " "
    ) %>%
    dplyr::select(-c("drop1", "drop2")) %>%
    # Pivot catch data from wide to long format
    tidyr::pivot_longer(
      cols = dplyr::contains("_kg"),
      names_to = "fish_category",
      values_to = "catch_kg"
    ) %>%
    # Standardize catch names and separate size information
    dplyr::mutate(
      fish_category = tolower(.data$fish_category),
      fish_category = stringr::str_remove(.data$fish_category, "_kg"),
      fish_category = dplyr::case_when(
        .data$fish_category == "samaki_wa_maji_mengi_wakubwa" ~ "pelagics_large",
        .data$fish_category == "samaki_wa_maji_mengi_wadogo" ~ "pelagics_small",
        .data$fish_category == "papa_wakubwa" ~ "shark_large",
        .data$fish_category == "papa_wadogo" ~ "shark_small",
        .data$fish_category == "taa_wakubwa" ~ "ray_large",
        .data$fish_category == "taa_wadogo" ~ "ray_small",
        .data$fish_category == "mchanganyiko_wakubwa" ~ "rest of catch_large",
        .data$fish_category == "mchanganyiko_wadogo" ~ "rest of catch_small",
        .data$fish_category == "mchanganyiko" ~ "rest of catch",
        TRUE ~ .data$fish_category
      ),
      # Standardize gear names
      gear = dplyr::case_when(
        .data$gear == "beachseine" ~ "speargun",
        .data$gear == "net" ~ "gillnet",
        .data$gear == "handline" ~ "setnet",
        .data$gear == "speargun___hook___stick" ~ "traps",
        .data$gear == "traps" ~ "handline",
        .data$gear == "ringnet" ~ "monofilament",
        .data$gear == "beachseine_1" ~ "beachseine",
        .data$gear == "ringnet_1" ~ "ringnet",
        TRUE ~ .data$gear
      ),
      # Standardize landing sites
      landing_site = dplyr::case_when(
        landing_site == "kijangwani_1" ~ "kijangwani",
        TRUE ~ .data$landing_site
      )
    ) %>%
    tidyr::separate(.data$fish_category, into = c("fish_category", "size"), sep = "_")

  # Convert data types
  field_raw <-
    renamed_raw %>%
    dplyr::mutate(
      landing_date = lubridate::as_datetime(.data$landing_date),
      submission_id = as.character(.data$submission_id),
      dplyr::across(
        c("no_of_fishers", "n_boats", "catch_kg", "lat", "lon", "total_catch_form"), ~ as.numeric(.x)
      )
    )

  # manage mismatch between caluclated total catch and total catch from form
  mismatch_total <-
    field_raw %>%
    dplyr::select("submission_id", "form_consent", "fish_category", "catch_kg", "size", "total_catch_form") %>%
    dplyr::group_by(.data$submission_id) %>%
    dplyr::mutate(total_catch = sum(.data$catch_kg, na.rm = TRUE)) %>%
    dplyr::summarise(
      form_consent = dplyr::first(.data$form_consent),
      total_catch_form = dplyr::first(.data$total_catch_form),
      total_catch = dplyr::first(.data$total_catch)
    ) %>%
    dplyr::mutate(
      mismatch_label = ifelse(.data$total_catch_form != .data$total_catch, "y", "n"),
      mismatch_value = abs(.data$total_catch_form - .data$total_catch),
      tot_catch_fixed = dplyr::case_when(
        .data$form_consent == "no" ~ NA_real_,
        # If there's a mismatch, use the total_catch sum
        .data$total_catch != .data$total_catch_form ~ .data$total_catch,
        # If fish groups sum to 0 but total catch is not 0, trust the total_catch_form
        .data$total_catch == 0 & .data$total_catch_form > 0 ~ .data$total_catch_form,
        # Otherwise, keep the original total_catch value
        TRUE ~ .data$total_catch
      )
    )

  # Handle cases with no catch data
  catch_data <-
    field_raw %>%
    dplyr::group_by(.data$submission_id) %>%
    dplyr::mutate(total_catch = sum(.data$catch_kg, na.rm = TRUE)) %>%
    dplyr::group_by(.data$submission_id) %>%
    dplyr::mutate(catch_present = ifelse(.data$total_catch > 0, TRUE, FALSE)) %>%
    dplyr::ungroup() %>%
    split(.$catch_present)

  # Process data with catch
  catch_data$`TRUE` <-
    catch_data$`TRUE` %>%
    dplyr::filter(.data$catch_kg > 0)

  # Process data without catch
  catch_data$`FALSE` <-
    catch_data$`FALSE` %>%
    dplyr::mutate(
      fish_category = NA_character_,
      size = NA_character_,
      catch_kg = 0,
    ) %>%
    dplyr::distinct()

  # Combine processed data
  refined_landings <-
    catch_data %>%
    dplyr::bind_rows() %>%
    dplyr::arrange(.data$landing_site, .data$landing_date) %>%
    dplyr::group_by(.data$submission_id) %>%
    dplyr::mutate(
      n_catch = seq(1, dplyr::n(), 1),
      catch_id = paste(.data$submission_id, .data$n_catch, sep = "-"),
      catch_kg = ifelse(.data$form_consent == "no", NA_real_, .data$catch_kg),
      total_catch = ifelse(.data$form_consent == "no", NA_real_, .data$total_catch)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select(-c("n_catch", "catch_present")) %>%
    dplyr::select("submission_id", "form_consent", "catch_id", dplyr::everything())

  # Use fixed total catch values
  preprocessed_landings <-
    mismatch_total %>%
    dplyr::select("submission_id", "tot_catch_fixed") %>%
    dplyr::right_join(refined_landings, by = "submission_id") %>%
    dplyr::select(-c("total_catch", "total_catch_form")) %>%
    dplyr::relocate(.data$tot_catch_fixed, .after = "catch_kg") %>%
    dplyr::rename(total_catch_kg = "tot_catch_fixed")

  logger::log_info("Uploading preprocessed data to mongodb")
  # upload preprocessed landings
  mdb_collection_push(
    data = preprocessed_landings,
    connection_string = conf$storage$mongodb$connection_string,
    collection_name = conf$storage$mongodb$database$pipeline$collection_name$ongoing$preprocessed,
    db_name = conf$storage$mongodb$database$pipeline$name
  )
}

#' Preprocess Legacy Landings Data
#'
#' This function imports, preprocesses, and cleans legacy landings data from a MongoDB collection.
#' It performs various data cleaning and transformation operations, including column renaming,
#' removal of unnecessary columns, generation of unique identifiers, and data type conversions.
#' The processed data is then uploaded back to MongoDB.
#'
#' @param log_threshold Logging threshold level (default: logger::DEBUG)
#'
#' @return This function does not return a value. Instead, it processes the data and uploads
#'   the result to a MongoDB collection in the pipeline database.
#'
#' @details
#' The function performs the following main operations:
#' 1. Pulls raw data from the raw data MongoDB collection.
#' 2. Removes several unnecessary columns.
#' 3. Renames columns for clarity (e.g., 'site' to 'landing_site').
#' 4. Generates unique 'survey_id' and 'catch_id' fields.
#' 5. Converts several string fields to lowercase.
#' 6. Cleans catch names using a separate function 'clean_catch_names'.
#' 7. Uploads the processed data to the preprocessed MongoDB collection.
#'
#' @note This function requires a configuration file to be present and readable by the
#'   'read_config' function, which should provide MongoDB connection details.
#'
#' @keywords workflow preprocessing
#' @examples
#' \dontrun{
#' preprocess_legacy_landings()
#' }
#' @export
preprocess_legacy_landings <- function(log_threshold = logger::DEBUG) {
  conf <- read_config()

  # get raw landings from mongodb
  raw_legacy_dat <- mdb_collection_pull(
    collection_name = conf$storage$mongodb$database$pipeline$collection_name$legacy$raw,
    db_name = conf$storage$mongodb$database$pipeline$name,
    connection_string = conf$storage$mongodb$connection_string
  ) |>
    dplyr::as_tibble()

  # preprocess raw landings
  processed_legacy_landings <-
    raw_legacy_dat %>%
    dplyr::select(-c("Months", "Year", "Day", "Month", "Management", "New_mngt", "Mngt")) %>%
    janitor::clean_names() %>%
    dplyr::rename(
      landing_date = "date",
      landing_site = "site",
      n_boats = "no_boats"
    ) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(submission_id = digest::digest(
      paste(.data$landing_date, .data$landing_site, .data$gear, .data$n_boats, .data$no_of_fishers,
        sep = "_"
      ),
      algo = "crc32"
    )) %>%
    dplyr::group_by(.data$submission_id) %>%
    dplyr::mutate(
      n_catch = seq(1, dplyr::n(), 1),
      catch_id = paste(.data$submission_id, .data$n_catch, sep = "-")
    ) %>%
    dplyr::select(
      "submission_id",
      "catch_id",
      "landing_date",
      "landing_site",
      dplyr::everything()
    ) %>%
    dplyr::select(-c(
      "n_catch", "sector", "size_km", "new_areas",
      "seascape", "new_fishing_areas", "total_catch"
    )) %>%
    dplyr::ungroup() |>
    dplyr::mutate(
      dplyr::across(.cols = c(
        "catch_name", "gear", "gear_new",
        "fish_category", "ecology",
        "landing_site"
      ), tolower),
      fixed_gear = dplyr::case_when(
        .data$gear == "gillnet" ~ "gillnet",
        .data$gear_new == "trap" ~ "traps",
        .data$gear == "monofillament" ~ "monofilament",
        .data$gear_new == "handline" ~ "handline",
        .data$gear == "longline" ~ "handline",
        .data$gear %in% c("prawnseine", "jarife", "sardinenet", "scoopnet", "net", "chachacha", "mosquitonet", "sharknet", "castnet", "squidnet", "trumnet") ~ "other_nets",
        .data$gear == "setnet" ~ "setnet",
        .data$gear == "ringnet" ~ "ringnet",
        .data$gear %in% c("spear", "harpoon") ~ "speargun",
        .data$gear == "fencetrap" ~ "fence_trap",
        .data$gear == "reefseine" ~ "reefseine",
        .data$gear == "beachseine" ~ "beachseine",
        .data$gear %in% c("stick", "gleaning") ~ "hook_and_stick",
        TRUE ~ NA_character_
      )
    ) %>%
    clean_catch_names() %>%
    dplyr::mutate(
      fish_category = dplyr::case_when(
        .data$catch_name %in% c("stringrays", "rays") ~ "ray",
        .data$catch_name %in% c("sharks", "shark") ~ "shark",
        .data$fish_category == "pelagic" ~ "pelagics",
        TRUE ~ .data$fish_category
      )
    ) %>%
    dplyr::filter(!.data$fish_category == "0") %>%
    dplyr::select(-c("gear", "gear_new", "catch_name", "ecology")) %>%
    dplyr::rename(gear = "fixed_gear") |>
    # caluclate total catch per submission
    dplyr::arrange(.data$landing_date, .data$submission_id) |>
    dplyr::group_by(.data$submission_id) |>
    dplyr::mutate(total_catch_kg = sum(.data$catch_kg, na.rm = T)) |>
    dplyr::ungroup()

  logger::log_info("Uploading preprocessed legacy data to mongodb")
  # upload preprocessed landings
  mdb_collection_push(
    data = processed_legacy_landings,
    connection_string = conf$storage$mongodb$connection_string,
    collection_name = conf$storage$mongodb$database$pipeline$collection_name$legacy$preprocessed,
    db_name = conf$storage$mongodb$database$pipeline$name
  )
}


#' Clean Catch Names
#'
#' This function standardizes catch names in the dataset by correcting common misspellings
#' and inconsistencies.
#'
#' @param data A data frame or tibble containing a column named `catch_name`.
#'
#' @return A data frame or tibble with standardized catch names in the `catch_name` column.
#'
#' @keywords preprocessing
#' @examples
#' \dontrun{
#' cleaned_data <- clean_catch_names(data)
#' }
#'
#' @export
clean_catch_names <- function(data = NULL) {
  data %>%
    dplyr::mutate(catch_name = dplyr::case_when(
      catch_name == "eeelfish" ~ "eel fish",
      catch_name == "sharks" ~ "shark",
      catch_name %in% c("silvermoony", "silver moony") ~ "silver moonfish",
      catch_name %in% c("soldier", "soldierfish") ~ "soldier fish",
      catch_name %in% c("surgeoms", "surgeon", "surgeonfish") ~ "surgeon fish",
      catch_name %in% c("sweatlips", "sweetlip") ~ "sweetlips",
      catch_name %in% c("zebra", "zebrafish") ~ "zebra fish",
      TRUE ~ catch_name
    ))
}
