#' Preprocess Landings Data (Version 1)
#'
#' This function preprocesses raw landings data from Google Cloud Storage.
#' It performs various data cleaning and transformation operations, including
#' column renaming, data pivoting, and standardization of catch names.
#'
#' @param log_threshold Logging threshold level (default: logger::DEBUG)
#'
#' @return No return value. Function processes the data and uploads the result as a Parquet file to Google Cloud Storage.
#'
#' @details
#' The function performs the following main operations:
#' 1. Downloads raw data from Google Cloud Storage
#' 2. Renames columns and selects relevant fields
#' 3. Generates unique survey IDs
#' 4. Cleans and standardizes text fields
#' 5. Pivots catch data from wide to long format
#' 6. Standardizes catch names and separates size information
#' 7. Converts data types and handles cases with no catch data
#' 8. Uploads the processed data as a Parquet file to Google Cloud Storage
#'
#' @keywords workflow preprocessing
#' @examples
#' \dontrun{
#' preprocessed_data <- preprocess_landings_v1()
#' }
#' @export
preprocess_landings_v1 <- function(log_threshold = logger::DEBUG) {
  conf <- read_config()

  raw_dat <- download_parquet_from_cloud(
    prefix = conf$surveys$catch$v1$raw$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  preprocessed_landings <- preprocess_landings_core(
    raw_dat = raw_dat,
    gear_mapping_func = apply_gear_mapping_v1()
  )

  upload_parquet_to_cloud(
    data = preprocessed_landings,
    prefix = conf$surveys$catch$v1$preprocessed$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )
}

#' Preprocess Landings Data (Version 2)
#'
#' This function preprocesses raw landings data from Google Cloud Storage.
#' It performs various data cleaning and transformation operations, including
#' column renaming, data pivoting, and standardization of catch names.
#'
#' @param log_threshold Logging threshold level (default: logger::DEBUG)
#'
#' @return No return value. Function processes the data and uploads the result as a Parquet file to Google Cloud Storage.
#'
#' @details
#' The function performs the following main operations:
#' 1. Downloads raw data from Google Cloud Storage
#' 2. Renames columns and selects relevant fields
#' 3. Generates unique survey IDs
#' 4. Cleans and standardizes text fields
#' 5. Pivots catch data from wide to long format
#' 6. Standardizes catch names and separates size information
#' 7. Converts data types and handles cases with no catch data
#' 8. Uploads the processed data as a Parquet file to Google Cloud Storage
#'
#' @keywords workflow preprocessing
#' @examples
#' \dontrun{
#' preprocessed_data <- preprocess_landings_v2()
#' }
#' @export
preprocess_landings_v2 <- function(log_threshold = logger::DEBUG) {
  conf <- read_config()

  raw_dat <- download_parquet_from_cloud(
    prefix = conf$surveys$catch$v2$raw$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  individual_data <- get_individual_data(raw_dat)

  preprocessed_landings <- preprocess_landings_core(
    raw_dat = raw_dat,
    gear_mapping_func = apply_gear_mapping_v2()
  ) |>
    dplyr::left_join(
      individual_data,
      by = c("submission_id")
    ) |>
    dplyr::relocate(
      "fisher_id",
      .after = "no_of_fishers"
    ) |>
    dplyr::relocate(
      "trip_cost",
      .after = "fisher_id"
    )

  upload_parquet_to_cloud(
    data = preprocessed_landings,
    prefix = conf$surveys$catch$v2$preprocessed$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
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

  raw_legacy_dat <- download_parquet_from_cloud(
    prefix = conf$surveys$catch$legacy$raw$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  processed_legacy_landings <-
    raw_legacy_dat %>%
    # fix dates and times
    dplyr::mutate(
      Date = dplyr::case_when(
        .data$Date < "1990-01-01" ~
          lubridate::as_datetime(lubridate::ymd(paste(
            .data$Year,
            .data$Month,
            .data$Day,
            sep = "-"
          ))),
        TRUE ~ .data$Date
      )
    ) %>%
    dplyr::select(
      -c("Months", "Year", "Day", "Month", "Management", "New_mngt", "Mngt")
    ) %>%
    janitor::clean_names() %>%
    dplyr::rename(
      landing_date = "date",
      landing_site = "site",
      n_boats = "no_boats"
    ) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(
      submission_id = digest::digest(
        paste(
          .data$landing_date,
          .data$landing_site,
          .data$gear,
          .data$n_boats,
          .data$no_of_fishers,
          sep = "_"
        ),
        algo = "crc32"
      )
    ) %>%
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
    dplyr::select(
      -c(
        "n_catch",
        "sector",
        "size_km",
        "new_areas",
        "seascape",
        "new_fishing_areas",
        "total_catch"
      )
    ) %>%
    dplyr::ungroup() |>
    dplyr::mutate(
      dplyr::across(
        .cols = c(
          "catch_name",
          "gear",
          "gear_new",
          "fish_category",
          "ecology",
          "landing_site"
        ),
        tolower
      ),
      fixed_gear = dplyr::case_when(
        .data$gear %in%
          c(
            "gillnet",
            "net",
            "beachseine",
            "sardinenet",
            "scoopnet",
            "chachacha",
            "mosquitonet",
            "sharknet",
            "castnet",
            "squidnet",
            "trumnet",
            "prawnseine",
            "jarife",
            "setnet",
            "ringnet",
            "reefseine",
            "monofillament",
            "monofilament"
          ) ~
          "Nets",
        .data$gear %in% c("harpoon", "spear") ~ "Speargun",
        .data$gear %in% c("line") ~ "Handline",
        .data$gear_new == "trap" | .data$gear %in% c("trap") ~ "Traps",
        .data$gear %in% c("gleaning", "stick") ~ "Hook and stick",
        .data$gear %in% c("fencetrap") ~ "Fencetrap",
        .data$gear %in% c("longline") ~ "Longline",
        .data$gear_new == "handline" ~ "Handline",
        TRUE ~ NA_character_
      ),
      fixed_gear = tolower(.data$fixed_gear)
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
    dplyr::rename(
      gear = "fixed_gear",
      ksh_kg = "price"
    ) |>
    # caluclate total catch per submission
    dplyr::arrange(.data$landing_date, .data$submission_id) |>
    dplyr::group_by(.data$submission_id) |>
    dplyr::mutate(total_catch_kg = sum(.data$catch_kg, na.rm = T)) |>
    dplyr::ungroup()

  upload_parquet_to_cloud(
    data = processed_legacy_landings,
    prefix = conf$surveys$catch$legacy$preprocessed$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )
}


#' Core preprocessing logic for price data
#'
#' @param raw_dat Raw data from cloud storage
#' @param version Version identifier for logging and processing
#' @return Preprocessed price data
#' @keywords internal
preprocess_price_core <- function(raw_dat, version) {
  # Rename columns and select relevant fields
  renamed_raw <-
    raw_dat |>
    dplyr::rename_with(~ stringr::str_remove(., "group_ww6lp73/")) %>%
    dplyr::select(
      "submission_id",
      landing_site = "Tambua_BMU",
      landing_date = "Tambua_tarehe",
      dplyr::contains("_kg")
    ) %>%
    dplyr::mutate(
      landing_site = tolower(.data$landing_site),
      landing_site = trimws(.data$landing_site),
    ) %>%
    # Pivot catch data from wide to long format
    tidyr::pivot_longer(
      cols = dplyr::contains("_kg"),
      names_to = "fish_category",
      values_to = "ksh_kg"
    ) %>%
    # Standardize catch names and separate size information
    dplyr::mutate(
      fish_category = tolower(.data$fish_category),
      fish_category = stringr::str_remove(.data$fish_category, "_kg"),
      fish_category = dplyr::case_when(
        .data$fish_category == "tafi_wakubwa_ksh" ~ "rabbitfish_large",
        .data$fish_category == "tafi_wadogo_ksh" ~ "rabbitfish_small",
        .data$fish_category == "changu_wakubwa_ksh" ~ "scavengers_large",
        .data$fish_category == "changu_wadogo_ksh" ~ "scavengers_small",
        .data$fish_category == "pono_wakubwa_ksh" ~ "parrotfish_large",
        .data$fish_category == "pono_wadogo_ksh" ~ "parrotfish_small",
        .data$fish_category == "pweza_wakubwa_ksh" ~ "octopus_large",
        .data$fish_category == "pweza_wadogo_ksh" ~ "octopus_small",
        .data$fish_category == "kamba_wakubwa_ksh" ~ "lobster_large",
        .data$fish_category == "kamba_wadogo_ksh" ~ "lobster_small",
        .data$fish_category == "mchanganyiko_wakubwa_ksh" ~
          "rest of catch_large",
        .data$fish_category == "mchanganyiko_wadogo_ksh" ~
          "rest of catch_small",
        .data$fish_category == "mchanganyiko_ksh" ~ "rest of catch_small",
        .data$fish_category == "mkundaji_wakubwa_ksh" ~ "goatfish_large",
        .data$fish_category == "mkundaji_wadogo_ksh" ~ "goatfish_small",
        .data$fish_category == "samaki_wa_maji_mengi_wakubwa_ksh" ~
          "pelagics_large",
        .data$fish_category == "samaki_wa_maji_mengi_wadogo_ksh" ~
          "pelagics_small",
        .data$fish_category == "papa_wakubwa_ksh" ~ "shark_large",
        .data$fish_category == "papa_wadogo_ksh" ~ "shark_small",
        .data$fish_category == "taa_wakubwa_ksh" ~ "rabbitfish_large",
        .data$fish_category == "taa_wadogo_ksh" ~ "rabbitfish_small",
        TRUE ~ .data$fish_category # Default case to keep original if no match
      )
    ) |>
    tidyr::separate(
      .data$fish_category,
      into = c("fish_category", "size"),
      sep = "_"
    )

  # Convert data types
  preprocessed_landings_price <-
    renamed_raw %>%
    dplyr::mutate(
      landing_date = lubridate::as_datetime(.data$landing_date),
      submission_id = as.character(.data$submission_id),
      ksh_kg = as.numeric(.data$ksh_kg)
    ) |>
    dplyr::distinct()

  return(preprocessed_landings_price)
}

#' Preprocess Price Data
#'
#' This function preprocesses raw price data from Google Cloud Storage.
#' It performs various data cleaning and transformation operations, including
#' column renaming, data pivoting, and standardization of fish categories and prices.
#'
#' @param log_threshold Logging threshold level (default: logger::DEBUG)
#'
#' @return No return value. Function processes the data and uploads the result as a Parquet file to Google Cloud Storage.
#'
#' @details
#' The function performs the following main operations:
#' 1. Downloads raw price data from Google Cloud Storage
#' 2. Renames columns and selects relevant fields (submission_id, landing_site, landing_date, and price fields)
#' 3. Cleans and standardizes text fields
#' 4. Pivots price data from wide to long format
#' 5. Standardizes fish category names and separates size information
#' 6. Converts data types (datetime, character, numeric)
#' 7. Removes duplicate entries
#' 8. Uploads the processed data as a Parquet file to Google Cloud Storage
#'
#' @keywords workflow preprocessing
#' @examples
#' \dontrun{
#' preprocess_price_landings()
#' }
#' @export
preprocess_price_landings <- function(log_threshold = logger::DEBUG) {
  conf <- read_config()

  # Define version configurations
  version_configs <- list(
    v1 = list(
      raw_prefix = conf$surveys$price$v1$raw$file_prefix,
      preprocessed_prefix = conf$surveys$price$v1$preprocessed$file_prefix
    ),
    v2 = list(
      raw_prefix = conf$surveys$price$v2$raw$file_prefix,
      preprocessed_prefix = conf$surveys$price$v2$preprocessed$file_prefix
    )
  )

  # Process each version
  purrr::iwalk(
    version_configs,
    ~ {
      raw_dat <- download_parquet_from_cloud(
        prefix = .x$raw_prefix,
        provider = conf$storage$google$key,
        options = conf$storage$google$options
      )

      preprocessed_data <- preprocess_price_core(raw_dat, .y)

      upload_parquet_to_cloud(
        data = preprocessed_data,
        prefix = .x$preprocessed_prefix,
        provider = conf$storage$google$key,
        options = conf$storage$google$options
      )
    }
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
    dplyr::mutate(
      catch_name = dplyr::case_when(
        catch_name == "eeelfish" ~ "eel fish",
        catch_name == "sharks" ~ "shark",
        catch_name %in% c("silvermoony", "silver moony") ~ "silver moonfish",
        catch_name %in% c("soldier", "soldierfish") ~ "soldier fish",
        catch_name %in% c("surgeoms", "surgeon", "surgeonfish") ~
          "surgeon fish",
        catch_name %in% c("sweatlips", "sweetlip") ~ "sweetlips",
        catch_name %in% c("zebra", "zebrafish") ~ "zebra fish",
        TRUE ~ catch_name
      )
    )
}

#' Core preprocessing logic for landings data
#'
#' @param raw_dat Raw data from cloud storage
#' @param gear_mapping_func Function that handles gear standardization
#' @return Preprocessed landings data
#' @keywords internal
preprocess_landings_core <- function(raw_dat, gear_mapping_func) {
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
    tidyr::separate(
      .data$gps,
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
        .data$fish_category == "samaki_wa_maji_mengi_wakubwa" ~
          "pelagics_large",
        .data$fish_category == "samaki_wa_maji_mengi_wadogo" ~ "pelagics_small",
        .data$fish_category == "papa_wakubwa" ~ "shark_large",
        .data$fish_category == "papa_wadogo" ~ "shark_small",
        .data$fish_category == "taa_wakubwa" ~ "ray_large",
        .data$fish_category == "taa_wadogo" ~ "ray_small",
        .data$fish_category == "mchanganyiko_wakubwa" ~ "rest of catch_large",
        .data$fish_category == "mchanganyiko_wadogo" ~ "rest of catch_small",
        .data$fish_category == "mchanganyiko" ~ "rest of catch",
        TRUE ~ .data$fish_category
      )
    ) %>%
    # Apply version-specific gear mapping
    gear_mapping_func() %>%
    dplyr::mutate(
      gear = tolower(.data$gear),
      # Standardize landing sites
      landing_site = dplyr::case_when(
        landing_site == "kijangwani_1" ~ "kijangwani",
        TRUE ~ .data$landing_site
      )
    ) %>%
    tidyr::separate(
      .data$fish_category,
      into = c("fish_category", "size"),
      sep = "_"
    )

  # Convert data types
  field_raw <-
    renamed_raw %>%
    dplyr::mutate(
      landing_date = lubridate::as_datetime(.data$landing_date),
      submission_id = as.character(.data$submission_id),
      dplyr::across(
        c(
          "no_of_fishers",
          "n_boats",
          "catch_kg",
          "lat",
          "lon",
          "total_catch_form"
        ),
        ~ as.numeric(.x)
      )
    ) |>
    dplyr::distinct() %>%
    dplyr::filter(.data$form_consent == "yes") %>%
    dplyr::select(-"form_consent")

  # manage mismatch between calculated total catch and total catch from form
  mismatch_total <-
    field_raw %>%
    dplyr::select(
      "submission_id",
      "fish_category",
      "catch_kg",
      "size",
      "total_catch_form"
    ) %>%
    dplyr::group_by(.data$submission_id) %>%
    dplyr::mutate(total_catch = sum(.data$catch_kg, na.rm = TRUE)) %>%
    dplyr::summarise(
      total_catch_form = dplyr::first(.data$total_catch_form),
      total_catch = dplyr::first(.data$total_catch)
    ) %>%
    dplyr::mutate(
      mismatch_label = ifelse(
        .data$total_catch_form != .data$total_catch,
        "y",
        "n"
      ),
      mismatch_value = abs(.data$total_catch_form - .data$total_catch),
      tot_catch_fixed = dplyr::case_when(
        # If there's a mismatch, use the total_catch sum
        .data$total_catch != .data$total_catch_form ~ .data$total_catch,
        # If fish groups sum to 0 but total catch is not 0, trust the total_catch_form
        .data$total_catch == 0 & .data$total_catch_form > 0 ~
          .data$total_catch_form,
        # Otherwise, keep the original total_catch value
        TRUE ~ .data$total_catch
      )
    )

  # Handle cases with no catch data
  catch_data <-
    field_raw |>
    dplyr::left_join(
      mismatch_total |> dplyr::select("submission_id", "tot_catch_fixed"),
      by = "submission_id"
    ) |>
    dplyr::select(-c("total_catch_form")) |>
    dplyr::group_by(.data$submission_id) %>%
    dplyr::mutate(
      catch_present = ifelse(.data$tot_catch_fixed > 0, TRUE, FALSE)
    ) %>%
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
  preprocessed_landings <-
    catch_data %>%
    dplyr::bind_rows() %>%
    dplyr::arrange(.data$landing_site, .data$landing_date) %>%
    dplyr::group_by(.data$submission_id) %>%
    dplyr::mutate(
      n_catch = seq(1, dplyr::n(), 1),
      catch_id = paste(.data$submission_id, .data$n_catch, sep = "-"),
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select(-c("n_catch", "catch_present")) %>%
    dplyr::select("submission_id", "catch_id", dplyr::everything()) |>
    dplyr::rename(total_catch_kg = "tot_catch_fixed")

  return(preprocessed_landings)
}

#' Gear mapping for version 1 data
#' @keywords internal
apply_gear_mapping_v1 <- function() {
  function(data) {
    data %>%
      dplyr::mutate(
        gear = dplyr::case_when(
          .data$gear %in% c("hook_and_stick", "speargun___hook___stick") ~
            "Hook and stick",
          .data$gear %in%
            c(
              "net",
              "ringnet_1",
              "ringnet",
              "beachseine",
              "beachseine_1",
              "reefseine",
              "other_nets"
            ) ~
            "Nets",
          .data$gear %in% c("traps") ~ "Traps",
          .data$gear %in% c("handline") ~ "Handline",
          .data$gear %in% c("fence_trap") ~ "Fencetrap",
          .data$gear %in% c("mshipi_wa_kurambaza") ~ "Trollingline",
          TRUE ~ .data$gear
        )
      )
  }
}

#' Gear mapping for version 2 data
#' @keywords internal
apply_gear_mapping_v2 <- function() {
  function(data) {
    data %>%
      dplyr::mutate(
        gear = dplyr::case_when(
          .data$gear %in%
            c(
              "setnet",
              "beachseine",
              "monofilament",
              "reefseine",
              "other_nets",
              "ringnet",
              "gillnet",
              "castnet",
              "nyavu",
              "trammel_net_1",
              "scoopnet_&_castnet",
              "trammel_net",
              "Mosquito_net",
              "nyavu_ya_mbu",
              "nets"
            ) ~
            "Nets",
          .data$gear %in% c("handline") ~ "Handline",
          .data$gear %in% c("traps") ~ "Traps",
          .data$gear %in% c("speargun") ~ "Speargun",
          .data$gear %in% c("hook_and_stick") ~ "Hook and stick",
          .data$gear %in% c("fence_trap") ~ "Fencetrap",
          .data$gear %in% c("longline") ~ "Longline",
          .data$gear %in% c("trolling_line") ~ "Trollingline",
          .data$gear == "trap" ~ "Traps",
          TRUE ~ NA_character_
        )
      )
  }
}


#' Get fishers ID and trip cost
#'
#' This function extracts the fisher ID and trip cost from the raw data.
#'
#' @param raw_dat Raw data from cloud storage
#' @return A data frame with the submission ID, fisher ID, and trip cost
#' @keywords internal

get_individual_data <- function(raw_dat) {
  raw_dat |>
    dplyr::rename_with(~ stringr::str_remove(., "group_xp36r82/")) |>
    dplyr::select(
      "submission_id",
      trip_cost = "Kadiria_gharama_inay_ana_na_pato_la_mvuvi",
      dplyr::starts_with("Jina_la_mvuvi")
    ) |>
    dplyr::mutate(
      fisher_id = do.call(
        dplyr::coalesce,
        dplyr::pick(dplyr::starts_with("Jina_la_mvuvi"))
      ),
      submission_id = as.character(.data$submission_id),
      trip_cost = as.numeric(.data$trip_cost)
    ) |>
    dplyr::select("submission_id", "fisher_id", "trip_cost")
}
