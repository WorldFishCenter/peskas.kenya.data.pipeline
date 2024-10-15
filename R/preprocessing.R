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
      landing_site = "Tambua_BMU",
      landing_date = "Tambua_tarehe",
      fishing_ground = "Eneo_la_uvuvi",
      gear = "Aina_ya_zana_ya_uvuvi",
      no_of_fishers = "Number_of_fishermen",
      n_boats = "Number_of_boats",
      gps = "Rekodi_GPS",
      dplyr::contains("_kg")
    ) %>%
    dplyr::rowwise() %>%
    # Generate unique survey IDs and clean text fields
    dplyr::ungroup() %>%
    dplyr::mutate(
      landing_site = stringr::str_to_title(.data$landing_site),
      fishing_ground = stringr::str_to_title(.data$fishing_ground),
      landing_site = trimws(.data$landing_site),
      fishing_ground = trimws(.data$fishing_ground)
    ) %>%
    tidyr::separate(.data$gps,
      into = c("lat", "lon", "drop1", "drop2"),
      sep = " "
    ) %>%
    dplyr::select(-c("Pato_kwa_ujumla_kg", "drop1", "drop2")) %>%
    # Pivot catch data from wide to long format
    tidyr::pivot_longer(
      cols = dplyr::contains("_kg"),
      names_to = "catch_name",
      values_to = "catch_kg"
    ) %>%
    # Standardize catch names and separate size information
    dplyr::mutate(
      catch_name = tolower(.data$catch_name),
      catch_name = stringr::str_remove(.data$catch_name, "_kg"),
      catch_name = dplyr::case_when(
        catch_name == "samaki_wa_maji_mengi_wakubwa" ~ "freshwater fish_large",
        catch_name == "samaki_wa_maji_mengi_wadogo" ~ "freshwater fish_small",
        catch_name == "papa_wakubwa" ~ "shark_large",
        catch_name == "papa_wadogo" ~ "shark_small",
        catch_name == "taa_wakubwa" ~ "lamp_large",
        catch_name == "taa_wadogo" ~ "lamp_small",
        catch_name == "mchanganyiko_wakubwa" ~ "rest of catch_large",
        catch_name == "mchanganyiko_wadogo" ~ "rest of catch_small",
        catch_name == "mchanganyiko" ~ "rest of catch",
        TRUE ~ .data$catch_name
      )
    ) %>%
    tidyr::separate(.data$catch_name, into = c("catch_name", "size"), sep = "_")

  # Convert data types
  field_raw <-
    renamed_raw %>%
    dplyr::mutate(
      landing_date = lubridate::as_datetime(.data$landing_date),
      dplyr::across(
        c("no_of_fishers", "n_boats", "catch_kg", "lat", "lon"), ~ as.numeric(.x)
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
      catch_name = NA_character_,
      size = NA_character_,
      catch_kg = 0
    ) %>%
    dplyr::distinct()

  # Combine processed data
  preprocessed_landings <-
    catch_data %>%
    dplyr::bind_rows() %>%
    dplyr::arrange(.data$landing_site, .data$landing_date) %>%
    dplyr::group_by(.data$submission_id) %>%
    dplyr::mutate(
      n_catch = seq(1, dplyr::n(), 1),
      catch_id = paste(.data$submission_id, .data$n_catch, sep = "-")
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select(-c("n_catch", "total_catch", "catch_present")) %>%
    dplyr::select("submission_id", "catch_id", dplyr::everything())


  logger::log_info("Uploading preprocessed legacy data to mongodb")
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
    dplyr::mutate(survey_id = digest::digest(
      paste(.data$landing_date, .data$landing_site, .data$gear, .data$n_boats,
        sep = "_"
      ),
      algo = "crc32"
    )) %>%
    dplyr::group_by(.data$survey_id) %>%
    dplyr::mutate(
      n_catch = seq(1, dplyr::n(), 1),
      catch_id = paste(.data$survey_id, .data$n_catch, sep = "-")
    ) %>%
    dplyr::select(
      "survey_id",
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
        "fish_category", "ecology"
      ), tolower)
    ) %>%
    clean_catch_names()


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
