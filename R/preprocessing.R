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
    prefix = conf$surveys$wcs$catch$v1$raw$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  preprocessed_landings <- preprocess_landings_core(
    raw_dat = raw_dat,
    gear_mapping_func = apply_gear_mapping_v1()
  )

  upload_parquet_to_cloud(
    data = preprocessed_landings,
    prefix = conf$surveys$wcs$catch$v1$preprocessed$file_prefix,
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
    prefix = conf$surveys$wcs$catch$v2$raw$file_prefix,
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
    prefix = conf$surveys$wcs$catch$v2$preprocessed$file_prefix,
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
    prefix = conf$surveys$wcs$catch$legacy$raw$file_prefix,
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
    prefix = conf$surveys$wcs$catch$legacy$preprocessed$file_prefix,
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
      raw_prefix = conf$surveys$wcs$price$v1$raw$file_prefix,
      preprocessed_prefix = conf$surveys$wcs$price$v1$preprocessed$file_prefix
    ),
    v2 = list(
      raw_prefix = conf$surveys$wcs$price$v2$raw$file_prefix,
      preprocessed_prefix = conf$surveys$wcs$price$v2$preprocessed$file_prefix
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
              "trammel_net"
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
          .data$gear == "handline" ~ "Handline",
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


#' Preprocess Pelagic Data Systems (PDS) Track Data
#'
#' @description
#' Downloads raw GPS tracks and creates a gridded summary of fishing activity.
#'
#' @param log_threshold The logging threshold to use. Default is logger::DEBUG.
#' @param grid_size Numeric. Size of grid cells in meters (100, 250, 500, or 1000).
#'
#' @return None (invisible). Creates and uploads preprocessed files.
#'
#' @keywords workflow preprocessing
#' @export
preprocess_pds_tracks <- function(
  log_threshold = logger::DEBUG,
  grid_size = 500
) {
  logger::log_threshold(log_threshold)
  pars <- read_config()

  # Get already preprocessed tracks
  logger::log_info("Checking existing preprocessed tracks...")
  preprocessed_filename <- cloud_object_name(
    prefix = paste0(pars$pds$pds_tracks$file_prefix, "-preprocessed"),
    provider = pars$storage$google$key,
    extension = "parquet",
    version = pars$pds$pds_tracks$version,
    options = pars$storage$google$options
  )

  # Get preprocessed trip IDs if file exists
  preprocessed_trips <- tryCatch(
    {
      download_cloud_file(
        name = preprocessed_filename,
        provider = pars$storage$google$key,
        options = pars$storage$google$options
      )
      preprocessed_data <- arrow::read_parquet(preprocessed_filename)
      unique(preprocessed_data$Trip)
    },
    error = function(e) {
      logger::log_info("No existing preprocessed tracks file found")
      character(0)
    }
  )

  # List raw tracks
  logger::log_info("Listing raw tracks...")
  raw_tracks <- googleCloudStorageR::gcs_list_objects(
    bucket = pars$pds_storage$google$options$bucket,
    prefix = pars$pds$pds_tracks$file_prefix
  )$name

  raw_trip_ids <- extract_trip_ids_from_filenames(raw_tracks)
  new_trip_ids <- setdiff(raw_trip_ids, preprocessed_trips)

  if (length(new_trip_ids) == 0) {
    logger::log_info("No new tracks to preprocess")
    return(invisible())
  }

  # Get raw tracks that need preprocessing
  new_tracks <- raw_tracks[raw_trip_ids %in% new_trip_ids]

  workers <- parallel::detectCores() - 1
  logger::log_info("Setting up parallel processing with {workers} workers...")
  future::plan(future::multisession, workers = workers)

  logger::log_info("Processing {length(new_tracks)} tracks in parallel...")
  new_processed_data <- furrr::future_map_dfr(
    new_tracks,
    function(track_file) {
      download_cloud_file(
        name = track_file,
        provider = pars$pds_storage$google$key,
        options = pars$pds_storage$google$options
      )

      track_data <- arrow::read_parquet(track_file) %>%
        preprocess_track_data(grid_size = grid_size)

      unlink(track_file)
      track_data
    },
    .options = furrr::furrr_options(seed = TRUE),
    .progress = TRUE
  )

  future::plan(future::sequential)

  # Combine with existing preprocessed data if it exists
  final_data <- if (length(preprocessed_trips) > 0) {
    dplyr::bind_rows(preprocessed_data, new_processed_data)
  } else {
    new_processed_data
  }

  output_filename <-
    paste0(pars$pds$pds_tracks$file_prefix, "-preprocessed") |>
    add_version(extension = "parquet")

  arrow::write_parquet(
    final_data,
    sink = output_filename,
    compression = "lz4",
    compression_level = 12
  )

  logger::log_info("Uploading preprocessed tracks...")
  upload_cloud_file(
    file = output_filename,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )

  unlink(output_filename)
  if (exists("preprocessed_filename")) {
    unlink(preprocessed_filename)
  }

  logger::log_success("Track preprocessing complete")

  grid_summaries <- generate_track_summaries(final_data)

  output_filename <-
    paste0(pars$pds$pds_tracks$file_prefix, "-grid_summaries") |>
    add_version(extension = "parquet")

  arrow::write_parquet(
    grid_summaries,
    sink = output_filename,
    compression = "lz4",
    compression_level = 12
  )

  logger::log_info("Uploading preprocessed tracks...")
  upload_cloud_file(
    file = output_filename,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
}


#' Preprocess Track Data into Spatial Grid Summary
#'
#' @description
#' This function processes GPS track data into a spatial grid summary, calculating time spent
#' and other metrics for each grid cell. The grid size can be specified to analyze spatial
#' patterns at different scales.
#'
#' @param data A data frame containing GPS track data with columns:
#'   - Trip: Unique trip identifier
#'   - Time: Timestamp of the GPS point
#'   - Lat: Latitude
#'   - Lng: Longitude
#'   - Speed (M/S): Speed in meters per second
#'   - Range (Meters): Range in meters
#'   - Heading: Heading in degrees
#'
#' @param grid_size Numeric. Size of grid cells in meters. Must be one of:
#'   - 100: ~100m grid cells
#'   - 250: ~250m grid cells
#'   - 500: ~500m grid cells (default)
#'   - 1000: ~1km grid cells
#'
#' @return A tibble with the following columns:
#'   - Trip: Trip identifier
#'   - lat_grid: Latitude of grid cell center
#'   - lng_grid: Longitude of grid cell center
#'   - time_spent_mins: Total time spent in grid cell in minutes
#'   - mean_speed: Average speed in grid cell (M/S)
#'   - mean_range: Average range in grid cell (Meters)
#'   - first_seen: First timestamp in grid cell
#'   - last_seen: Last timestamp in grid cell
#'   - n_points: Number of GPS points in grid cell
#'
#' @details
#' The function creates a grid by rounding coordinates based on the specified grid size.
#' Grid sizes are approximate due to the conversion from meters to degrees, with calculations
#' based on 1 degree ≈ 111km at the equator. Time spent is calculated using the time
#' differences between consecutive points.
#'
#' @keywords preprocessing
#'
#' @examples
#' \dontrun{
#' # Process tracks with 500m grid (default)
#' result_500m <- preprocess_track_data(tracks_data)
#'
#' # Use 100m grid for finer resolution
#' result_100m <- preprocess_track_data(tracks_data, grid_size = 100)
#'
#' # Use 1km grid for broader patterns
#' result_1km <- preprocess_track_data(tracks_data, grid_size = 1000)
#' }
#'
#' @keywords preprocessing
#' @export
preprocess_track_data <- function(data, grid_size = 500) {
  # Define grid size in meters to degrees (approximately)
  # 1 degree = 111km at equator
  grid_degrees <- switch(
    as.character(grid_size),
    "100" = 0.001, # ~100m
    "250" = 0.0025, # ~250m
    "500" = 0.005, # ~500m
    "1000" = 0.01, # ~1km
    stop("grid_size must be one of: 100, 250, 500, 1000")
  )

  data %>%
    dplyr::select(
      "Trip",
      "Time",
      "Lat",
      "Lng",
      "Speed (M/S)",
      "Range (Meters)",
      "Heading"
    ) %>%
    dplyr::group_by(.data$Trip) %>%
    dplyr::arrange(.data$Time) %>%
    dplyr::mutate(
      # Create grid cells based on selected size
      lat_grid = round(.data$Lat / grid_degrees, 0) * grid_degrees,
      lng_grid = round(.data$Lng / grid_degrees, 0) * grid_degrees,

      # Calculate time spent (difference with next point)
      time_diff = as.numeric(difftime(
        dplyr::lead(.data$Time),
        .data$Time,
        units = "mins"
      )),
      # For last point in series, use difference with previous point
      time_diff = dplyr::if_else(
        is.na(.data$time_diff),
        as.numeric(difftime(
          .data$Time,
          dplyr::lag(.data$Time),
          units = "mins"
        )),
        .data$time_diff
      )
    ) %>%
    # Group by trip and grid cell
    dplyr::group_by(.data$Trip, .data$lat_grid, .data$lng_grid) %>%
    dplyr::summarise(
      time_spent_mins = sum(.data$time_diff, na.rm = TRUE),
      mean_speed = mean(.data$`Speed (M/S)`, na.rm = TRUE),
      mean_range = mean(.data$`Range (Meters)`, na.rm = TRUE),
      first_seen = min(.data$Time),
      last_seen = max(.data$Time),
      n_points = dplyr::n(),
      .groups = "drop"
    ) %>%
    dplyr::filter(.data$time_spent_mins > 0) %>%
    dplyr::group_by(.data$Trip) %>%
    dplyr::arrange(.data$first_seen) %>%
    dplyr::filter(
      !(dplyr::row_number() %in% c(1, 2, dplyr::n() - 1, dplyr::n()))
    ) %>%
    dplyr::ungroup()
}

#' Generate Grid Summaries for Track Data
#'
#' @description
#' Processes GPS track data into 1km grid summaries for visualization and analysis.
#'
#' @param data Preprocessed track data
#' @param min_hours Minimum hours threshold for filtering (default: 0.15)
#' @param max_hours Maximum hours threshold for filtering (default: 10)
#'
#' @return A dataframe with grid summary statistics
#'
#' @keywords preprocessing
#' @export
generate_track_summaries <- function(data, min_hours = 0.15, max_hours = 15) {
  data %>%
    # First summarize by current grid (500m)
    dplyr::group_by(.data$lat_grid, .data$lng_grid) %>%
    dplyr::summarise(
      avg_time_mins = mean(.data$time_spent_mins),
      avg_speed = mean(.data$mean_speed),
      avg_range = mean(.data$mean_range),
      visits = dplyr::n_distinct(.data$Trip),
      total_points = sum(.data$n_points),
      .groups = "drop"
    ) %>%
    # Then regrid to 1km
    dplyr::mutate(
      lat_grid_1km = round(.data$lat_grid / 0.01) * 0.01,
      lng_grid_1km = round(.data$lng_grid / 0.01) * 0.01
    ) %>%
    dplyr::group_by(.data$lat_grid_1km, .data$lng_grid_1km) %>%
    dplyr::summarise(
      avg_time_mins = mean(.data$avg_time_mins),
      avg_speed = mean(.data$avg_speed),
      avg_range = mean(.data$avg_range),
      total_visits = sum(.data$visits),
      original_cells = dplyr::n(),
      total_points = sum(.data$total_points),
      .groups = "drop"
    ) %>%
    dplyr::mutate(
      avg_time_hours = .data$avg_time_mins / 60
    ) %>%
    dplyr::filter(
      .data$avg_time_hours >= min_hours,
      .data$avg_time_hours <= max_hours
    ) |>
    dplyr::select(-"avg_time_mins")
}


#' Calculate key fishery metrics by landing site and month in normalized long format
#'
#' Summarizes fishery data to extract main characteristics including catch rates,
#' gear usage, species composition, CPUE and RPUE by gear type. Returns data in a fully
#' normalized long format for maximum interoperability and analytical flexibility.
#'
#' @param data A dataframe containing fishery landing data with columns:
#'   submission_id, landing_date, landing_site, gear, no_of_fishers,
#'   fish_category, catch_kg, and total_catch_price
#'
#' @return A dataframe in long format with columns:
#'   \itemize{
#'     \item landing_site: Name of the landing site
#'     \item year_month: First day of the month
#'     \item metric_type: Type of metric (e.g., "avg_fishers_per_trip", "cpue", "rpue", "species_pct")
#'     \item metric_value: Numeric value of the metric
#'     \item gear_type: Type of fishing gear (for gear-specific metrics, NA for site-level metrics)
#'     \item species: Fish species name (for species-specific metrics, NA for other metrics)
#'     \item rank: Rank order (for ranked metrics like top species, NA for others)
#'   }
#'
#' @details
#' The function creates a fully normalized dataset where each row represents a single
#' metric observation. This format enables:
#' - Easy filtering by metric type, gear, or species
#' - Flexible aggregation and comparison across dimensions
#' - Database-friendly structure for storage and querying
#' - Simplified visualization and statistical analysis
#'
#' @examples
#' \dontrun{
#' fishery_metrics <- get_fishery_metrics_long(data = valid_data)
#'
#' # Filter for CPUE metrics only
#' cpue_data <- fishery_metrics %>% filter(metric_type == "cpue")
#'
#' # Filter for RPUE metrics only
#' rpue_data <- fishery_metrics %>% filter(metric_type == "rpue")
#'
#' # Get predominant gear by site
#' main_gear <- fishery_metrics %>% filter(metric_type == "predominant_gear")
#' }
#'
#' @export
get_fishery_metrics_long <- function(data = NULL) {
  # Calculate base metrics by landing site and month
  base_metrics <- data %>%
    dplyr::mutate(
      year_month = lubridate::floor_date(landing_date, "month"),
      trip_id = submission_id
    ) %>%
    dplyr::group_by(landing_site, year_month, trip_id, gear, no_of_fishers) %>%
    dplyr::summarise(
      total_catch_per_trip = sum(catch_kg, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::group_by(landing_site, year_month) %>%
    dplyr::summarise(
      avg_fishers_per_trip = round(mean(no_of_fishers, na.rm = TRUE), 2),
      avg_catch_per_trip = round(mean(total_catch_per_trip, na.rm = TRUE), 2),
      predominant_gear = names(which.max(table(gear))),
      pct_main_gear = round(
        sum(gear == predominant_gear) / dplyr::n() * 100,
        2
      ),
      .groups = "drop"
    )

  # Calculate CPUE by gear type
  cpue_metrics <- data %>%
    dplyr::mutate(year_month = lubridate::floor_date(landing_date, "month")) %>%
    dplyr::group_by(
      landing_site,
      year_month,
      gear,
      submission_id,
      no_of_fishers
    ) %>%
    dplyr::summarise(
      trip_catch = sum(catch_kg, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::mutate(cpue_per_fisher = trip_catch / no_of_fishers) %>%
    dplyr::group_by(landing_site, year_month, gear) %>%
    dplyr::summarise(
      cpue = round(mean(cpue_per_fisher, na.rm = TRUE), 2),
      .groups = "drop"
    )

  # Calculate RPUE by gear type
  rpue_metrics <- data %>%
    dplyr::mutate(year_month = lubridate::floor_date(landing_date, "month")) %>%
    dplyr::group_by(
      landing_site,
      year_month,
      gear,
      submission_id,
      no_of_fishers
    ) %>%
    dplyr::summarise(
      trip_revenue = sum(total_catch_price, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::mutate(rpue_per_fisher = trip_revenue / no_of_fishers) %>%
    dplyr::group_by(landing_site, year_month, gear) %>%
    dplyr::summarise(
      rpue = round(mean(rpue_per_fisher, na.rm = TRUE), 2),
      .groups = "drop"
    )

  # Calculate species composition metrics
  species_metrics <- data %>%
    dplyr::mutate(year_month = lubridate::floor_date(landing_date, "month")) %>%
    dplyr::group_by(landing_site, year_month, fish_category) %>%
    dplyr::summarise(
      species_catch_kg = sum(catch_kg, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::group_by(landing_site, year_month) %>%
    dplyr::mutate(
      species_pct = round(species_catch_kg / sum(species_catch_kg) * 100, 1),
      rank = rank(-species_catch_kg)
    ) %>%
    dplyr::filter(rank <= 2) %>%
    dplyr::select(
      landing_site,
      year_month,
      species = fish_category,
      species_pct,
      rank
    )

  # Convert to long format
  long_format <- dplyr::bind_rows(
    # Base site-level metrics
    base_metrics %>%
      dplyr::select(-predominant_gear) %>%
      tidyr::pivot_longer(
        cols = c(avg_fishers_per_trip, avg_catch_per_trip, pct_main_gear),
        names_to = "metric_type",
        values_to = "metric_value"
      ) %>%
      dplyr::mutate(
        gear_type = NA_character_,
        species = NA_character_,
        rank = NA_integer_
      ),

    # Predominant gear (text metric)
    base_metrics %>%
      dplyr::select(landing_site, year_month, predominant_gear) %>%
      dplyr::mutate(
        metric_type = "predominant_gear",
        metric_value = NA_real_,
        gear_type = predominant_gear,
        species = NA_character_,
        rank = NA_integer_
      ) %>%
      dplyr::select(-predominant_gear),

    # CPUE by gear type
    cpue_metrics %>%
      dplyr::mutate(
        metric_type = "cpue",
        metric_value = cpue,
        gear_type = gear,
        species = NA_character_,
        rank = NA_integer_
      ) %>%
      dplyr::select(-c(gear, cpue)),

    # RPUE by gear type
    rpue_metrics %>%
      dplyr::mutate(
        metric_type = "rpue",
        metric_value = rpue,
        gear_type = gear,
        species = NA_character_,
        rank = NA_integer_
      ) %>%
      dplyr::select(-c(gear, rpue)),

    # Species composition
    species_metrics %>%
      dplyr::mutate(
        metric_type = "species_pct",
        metric_value = species_pct,
        gear_type = NA_character_
      ) %>%
      dplyr::select(-species_pct)
  ) %>%
    dplyr::arrange(
      landing_site,
      year_month,
      metric_type,
      gear_type,
      species,
      rank
    )

  return(long_format)
}
