#' Preprocess KEFS Survey Data
#'
#' This function preprocesses raw KEFS (Kenya Fisheries Service) survey data from Google Cloud Storage.
#' It performs data cleaning, transformation, standardization of field names, type conversions,
#' and mapping to standardized taxonomic and gear names using Airtable reference tables.
#'
#' @param log_threshold Logging threshold level (default: logger::DEBUG)
#'
#' @return No return value. Function processes the data and uploads the result as a Parquet file to Google Cloud Storage.
#'
#' @details
#' The function performs the following main operations:
#' 1. **Fetches metadata assets**: Retrieves taxonomic, gear, vessel, and landing site mappings from Airtable
#'    based on the KEFS Kobo form asset ID
#' 2. **Downloads raw data**: Retrieves raw survey data from Google Cloud Storage
#' 3. **Extracts trip information**: Selects and renames relevant trip-level fields including:
#'    - Landing details (date, site, district, BMU)
#'    - Fishing ground and JCMA (Joint Community Management Area) information
#'    - Vessel details (type, name, registration, motorization, horsepower)
#'    - Trip details (crew size, start/end times, gear, mesh size, fuel)
#'    - Catch outcome indicators
#' 4. **Reshapes catch data**: Transforms catch details from wide to long format using `reshape_catch_data_v1()`
#' 5. **Type conversions and calculations**:
#'    - Converts date/time fields to proper datetime format
#'    - Calculates trip duration in hours from start and end times
#'    - Converts numeric fields (hp, fishers, mesh size, fuel) to appropriate types
#' 6. **Joins trip and catch data**: Combines trip information with catch records using full join on submission_id
#' 7. **Standardizes names**: Maps survey labels to standardized names using `map_surveys()`:
#'    - Taxonomic names to scientific names and alpha3 codes
#'    - Gear types to standardized gear names
#'    - Vessel types to standardized vessel categories
#'    - Landing site codes to full site names
#' 8. **Uploads processed data**: Saves preprocessed data as a Parquet file to Google Cloud Storage
#'
#' @section Data Structure:
#' The preprocessed output includes the following key fields:
#' \itemize{
#'   \item \strong{Trip identifiers}: submission_id
#'   \item \strong{Temporal}: landing_date, fishing_trip_start, fishing_trip_end, trip_duration
#'   \item \strong{Spatial}: district, BMU, landing_site, fishing_ground, jcma, jcma_site
#'   \item \strong{Vessel}: vessel_type, boat_name, vessel_reg_number, motorized, hp
#'   \item \strong{Crew}: captain_name, no_of_fishers
#'   \item \strong{Gear}: gear, mesh_size
#'   \item \strong{Catch}: scientific_name, alpha3_code, total_catch_weight, price_per_kg, total_value
#'   \item \strong{Operations}: fuel, catch_outcome, catch_shark
#' }
#'
#' @section Pipeline Integration:
#' This function is part of the KEFS data pipeline sequence:
#' 1. `ingest_kefs_surveys_v1()` - Downloads raw data from Kobo
#' 2. **`preprocess_kefs_surveys_v1()`** - Cleans and standardizes data (this function)
#' 3. Validation step (to be implemented)
#' 4. Export step (to be implemented)
#'
#' @keywords workflow preprocessing
#' @examples
#' \dontrun{
#' # Preprocess KEFS survey data
#' preprocess_kefs_surveys_v1()
#'
#' # Run with custom logging level
#' preprocess_kefs_surveys_v1(log_threshold = logger::INFO)
#' }
#' @export
preprocess_kefs_surveys_v1 <- function(log_threshold = logger::DEBUG) {
  conf <- read_config()

  assets <- fetch_assets(
    form_id = get_airtable_form_id(
      kobo_asset_id = conf$ingestion$kefs$koboform$asset_id_v1,
      conf = conf
    ),
    conf = conf
  )

  assets$sites <- assets$sites |>
    dplyr::mutate(
      site = stringr::str_trim(stringr::str_replace_all(.data$site, "\\n", ""))
    )

  raw_dat <- download_parquet_from_cloud(
    prefix = conf$surveys$kefs$v1$raw$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  trip_info <-
    raw_dat |>
    dplyr::select(
      "submission_id",
      enumerator_name = "Name_of_Data_Collector",
      landing_date = "date_data",
      district = "sub_county",
      "BMU",
      landing_site = "Landing_Site",
      fishing_ground = "Area_Fished",
      jcma = "Are_you_within_a_JCMA_Joint_C",
      jcma_site = "If_YES_Please_choos_from_the_list_below",
      vessel_type = "Craft_Typ",
      other_vessel = "Craft_TypOther",
      boat_name = "vessel_name",
      vessel_reg_number = "vessel_reg",
      "captain_name",
      motorized = "Motorized_or_Non",
      hp = "HP",
      no_of_fishers = "NumCrew",
      fishing_trip_start = "Enter_date_and_time_sel_left_for_fishing",
      fishing_trip_end = "Enter_date_and_time_the_vessel_landed",
      gear = "gear_type",
      mesh_size = "MESH_SIZE",
      fuel = "FUEL_USED",
      catch_outcome = "Did_you_Catch_ANY_FISH_TODAY",
      catch_shark = "Did_you_catch_any_SHARK"
    ) |>
    dplyr::left_join(
      standardize_enumerator_names(
        data = raw_dat,
        max_distance = 2
      ),
      by = "submission_id"
    ) |>
    dplyr::relocate(
      "enumerator_name_clean",
      .after = "submission_id"
    ) |>
    dplyr::select(-"enumerator_name")

  catch_data <- reshape_catch_data_v1(raw_data = raw_dat)

  # fix fields

  trip_info_preprocessed <-
    trip_info |>
    dplyr::mutate(
      submission_id = as.character(.data$submission_id),
      hp = as.integer(.data$hp),
      no_of_fishers = as.integer(.data$no_of_fishers),
      fishing_trip_start = lubridate::ymd_hms(.data$fishing_trip_start),
      fishing_trip_end = lubridate::ymd_hms(.data$fishing_trip_end),
      trip_duration = as.numeric(difftime(
        .data$fishing_trip_end,
        .data$fishing_trip_start,
        units = "hours"
      )),
      mesh_size = as.numeric(.data$mesh_size),
      fuel = as.numeric(.data$fuel)
    ) |>
    dplyr::relocate(.data$trip_duration, .after = "fishing_trip_end")

  catch_data_preprocessed <-
    catch_data |>
    dplyr::mutate(
      submission_id = as.character(.data$submission_id)
    )

  preprocessed_landings <-
    dplyr::full_join(
      trip_info_preprocessed,
      catch_data_preprocessed,
      by = "submission_id"
    )

  preprocessed_data <-
    map_surveys(
      data = preprocessed_landings,
      taxa_mapping = assets$taxa,
      gear_mapping = assets$gear,
      vessels_mapping = assets$vessels,
      sites_mapping = assets$sites,
      kefs_v2 = FALSE
    )

  # upload preprocessed landings
  upload_parquet_to_cloud(
    data = preprocessed_data,
    prefix = conf$surveys$kefs$preprocessed$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )
}
#' Preprocess KEFS (CATCH ASSESSMENT QUESTIONNAIRE) Survey Data
#'
#' This function preprocesses raw KEFS (CATCH ASSESSMENT QUESTIONNAIRE) survey data from Google Cloud Storage.
#' It performs data cleaning, transformation, standardization of field names, type conversions,
#' and mapping to standardized taxonomic and gear names using Airtable reference tables.
#'
#' @param log_threshold Logging threshold level (default: logger::DEBUG)
#'
#' @return No return value. Function processes the data and uploads the result as a Parquet file to Google Cloud Storage.
#'
#' @details
#' The function performs the following main operations:
#' 1. **Fetches metadata assets**: Retrieves taxonomic, gear, vessel, and landing site mappings from Airtable
#'    based on the KEFS Kobo form asset ID
#' 2. **Downloads raw data**: Retrieves raw survey data from Google Cloud Storage
#' 3. **Extracts trip information**: Selects and renames relevant trip-level fields including:
#'    - Landing details (date, site, district, BMU)
#'    - Fishing ground and JCMA (Joint Community Management Area) information
#'    - Vessel details (type, name, registration, motorization, horsepower)
#'    - Trip details (crew size, start/end times, gear, mesh size, fuel)
#'    - Catch outcome indicators
#' 4. **Reshapes catch data**: Transforms catch details from wide to long format using `reshape_priority_species()` and `reshape_overall_sample()`
#' 5. **Type conversions and calculations**:
#'    - Converts date/time fields to proper datetime format
#'    - Calculates trip duration in hours from start and end times
#'    - Converts numeric fields (hp, fishers, mesh size, fuel) to appropriate types
#' 6. **Joins trip and catch data**: Combines trip information with catch records using full join on submission_id
#' 7. **Standardizes names**: Maps survey labels to standardized names using `map_surveys()`:
#'    - Taxonomic names to scientific names and alpha3 codes
#'    - Gear types to standardized gear names
#'    - Vessel types to standardized vessel categories
#'    - Landing site codes to full site names
#' 8. **Uploads processed data**: Saves preprocessed data as a Parquet file to Google Cloud Storage
#'
#' @section Data Structure:
#' The preprocessed output includes the following key fields:
#' \itemize{
#'   \item \strong{Trip identifiers}: submission_id
#'   \item \strong{Temporal}: landing_date, fishing_trip_start, fishing_trip_end, trip_duration
#'   \item \strong{Spatial}: district, BMU, landing_site, fishing_ground, jcma, jcma_site
#'   \item \strong{Vessel}: vessel_type, boat_name, vessel_reg_number, motorized, hp
#'   \item \strong{Crew}: captain_name, no_of_fishers
#'   \item \strong{Gear}: gear, mesh_size
#'   \item \strong{Catch}: scientific_name, alpha3_code, total_catch_weight, price_per_kg, total_value
#'   \item \strong{Operations}: fuel, catch_outcome, catch_shark
#' }
#'
#' @section Pipeline Integration:
#' This function is part of the KEFS data pipeline sequence:
#' 1. `ingest_kefs_surveys_v2()` - Downloads raw data from Kobo
#' 2. **`preprocess_kefs_surveys_v2()`** - Cleans and standardizes data (this function)
#' 3. Validation step (to be implemented)
#' 4. Export step (to be implemented)
#'
#' @keywords workflow preprocessing
#' @examples
#' \dontrun{
#' # Preprocess KEFS survey data
#' preprocess_kefs_surveys_v2()
#'
#' # Run with custom logging level
#' preprocess_kefs_surveys_v2(log_threshold = logger::INFO)
#' }
#' @export
preprocess_kefs_surveys_v2 <- function(log_threshold = logger::DEBUG) {
  conf <- read_config()

  assets <- fetch_assets(
    form_id = get_airtable_form_id(
      kobo_asset_id = conf$ingestion$kefs$koboform$asset_id_v2,
      conf = conf
    ),
    conf = conf
  )

  assets$sites <- assets$sites |>
    dplyr::mutate(
      site = stringr::str_trim(stringr::str_replace_all(.data$site, "\\n", ""))
    )

  raw_dat <- download_parquet_from_cloud(
    prefix = conf$surveys$kefs$v2$raw$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  trip_info <-
    raw_dat |>
    dplyr::select(
      "submission_id",
      enumerator_name = "Name_of_Data_Collector",
      landing_date = "date_data",
      district = "sub_county",
      "BMU",
      landing_site = "Landing_sites",
      fishing_ground = "Area_Fished",
      vessel_type = "Craft_Typ",
      pds = "Is_a_PDS_or_CLASS_B_stalled_on_your_Boat",
      other_vessel = "Craft_TypOther",
      boat_name = "vessel_name",
      vessel_reg_number = "vessel_reg",
      "captain_name",
      motorized = "Motorized_or_Non",
      "hp",
      no_of_fishers = "NumCrew",
      fishing_trip_start = "Enter_date_and_time_sel_left_for_fishing",
      fishing_trip_end = "Enter_date_and_time_the_vessel_landed",
      gear = "gear_type",
      mesh_size = "Please_enter_the_mesh_size_in_inches",
      catch_outcome = "Did_you_CATCH_FISH_TODAY",
      length_measure = "Are_you_taking_lengt_the_priority_species",
      protected_species = "SpeciesETP",
      sample_weight = "SampleWeight",
      catch_weight = "TotalCatchWeight",
      price_kg = "PricePerKg",
      catch_price = "TValue"
    ) |>
    dplyr::left_join(
      standardize_enumerator_names(
        data = raw_dat,
        max_distance = 2
      ),
      by = "submission_id"
    ) |>
    dplyr::relocate(
      "enumerator_name_clean",
      .after = "submission_id"
    ) |>
    dplyr::select(-"enumerator_name")

  priority_df <- reshape_priority_species(raw_data = raw_dat)
  sample_df <- reshape_overall_sample(raw_data = raw_dat)

  catch_info <- dplyr::full_join(
    priority_df,
    sample_df,
    by = c("submission_id")
  ) |>
    dplyr::mutate(
      submission_id = as.character(.data$submission_id)
    ) |>
    dplyr::distinct()

  # fix fields
  trip_info_preprocessed <-
    trip_info |>
    dplyr::mutate(
      submission_id = as.character(.data$submission_id),
      hp = as.integer(.data$hp),
      no_of_fishers = as.integer(.data$no_of_fishers),
      fishing_trip_start = lubridate::ymd_hms(.data$fishing_trip_start),
      fishing_trip_end = lubridate::ymd_hms(.data$fishing_trip_end),
      trip_duration = as.numeric(difftime(
        .data$fishing_trip_end,
        .data$fishing_trip_start,
        units = "hours"
      )),
      mesh_size = as.numeric(.data$mesh_size),
      sample_weight = as.numeric(.data$sample_weight),
      catch_weight = as.numeric(.data$catch_weight),
      price_kg = as.numeric(.data$price_kg),
      catch_price = as.numeric(.data$catch_price)
    ) |>
    dplyr::relocate("trip_duration", .after = "fishing_trip_end")

  preprocessed_landings <-
    dplyr::full_join(
      trip_info_preprocessed,
      catch_info,
      by = "submission_id"
    )

  preprocessed_data <-
    map_surveys(
      data = preprocessed_landings,
      taxa_mapping = assets$taxa,
      gear_mapping = assets$gear,
      vessels_mapping = assets$vessels,
      sites_mapping = assets$sites,
      kefs_v2 = TRUE
    )

  # upload preprocessed landings
  upload_parquet_to_cloud(
    data = preprocessed_data,
    prefix = conf$surveys$kefs$preprocessed$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )
}


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
#' @keywords helper
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

#' Map Survey Labels to Standardized Taxa, Gear, and Vessel Names
#'
#' @description
#' Converts local species, gear, and vessel labels from surveys to standardized names using
#' Airtable reference tables. Handles two survey form types: the standard form which uses
#' catch_taxon, and the KEFs v2 form which uses priority_species and sample_species.
#' Replaces local names with scientific names and alpha3 codes, and standardizes gear,
#' vessel, and site names.
#'
#' @param data A data frame with preprocessed survey data. For standard surveys, must contain
#'   catch_taxon, gear, vessel_type, and landing_site columns. For KEFs v2 surveys, must
#'   contain priority_species, sample_species, gear, vessel_type, and landing_site columns.
#' @param taxa_mapping A data frame from Airtable taxa table with survey_label, alpha3_code,
#'   and scientific_name columns.
#' @param gear_mapping A data frame from Airtable gears table with survey_label and
#'   standard_name columns.
#' @param vessels_mapping A data frame from Airtable vessels table with survey_label and
#'   standard_name columns.
#' @param sites_mapping A data frame from Airtable landing_sites table with site_code and
#'   site columns.
#' @param kefs_v2 Logical. If TRUE, processes data from the KEFs v2 survey form which
#'   uses priority_species and sample_species columns. If FALSE (default), processes
#'   standard survey forms with catch_taxon column.
#'
#' @return A tibble with survey labels replaced by standardized names:
#'   \itemize{
#'     \item For standard surveys: catch_taxon replaced by scientific_name and alpha3_code
#'     \item For KEFs v2 surveys: priority_species replaced by priority_scientific_name and
#'       priority_alpha3_code; sample_species replaced by sample_scientific_name and
#'       sample_alpha3_code
#'     \item gear and vessel_type replaced by standardized names
#'     \item landing_site replaced by the full site name
#'   }
#'   Records without matches will have NA values.
#'
#' @details
#' This function is called within `preprocess_landings()` after processing raw survey data.
#' The mapping tables are retrieved from Airtable frame base and filtered by
#' form ID before being passed to this function.
#'
#' The KEFs v2 survey form captures both priority species (target catch) and sample species
#' (for biological sampling), requiring separate taxonomic mappings for each.
#'
#' @keywords preprocessing helper
#' @export
map_surveys <- function(
  data = NULL,
  taxa_mapping = NULL,
  gear_mapping = NULL,
  vessels_mapping = NULL,
  sites_mapping = NULL,
  kefs_v2 = FALSE
) {
  taxa_map <-
    if (isTRUE(kefs_v2)) {
      data |>
        dplyr::left_join(
          taxa_mapping,
          by = c("priority_species" = "survey_label")
        ) |>
        dplyr::select(-c("priority_species")) |>
        dplyr::rename(
          priority_scientific_name = "scientific_name",
          priority_alpha3_code = "alpha3_code"
        ) |>
        dplyr::relocate("priority_scientific_name", .after = "n_priority") |>
        dplyr::relocate(
          "priority_alpha3_code",
          .after = "priority_scientific_name"
        ) |>
        dplyr::left_join(
          taxa_mapping,
          by = c("sample_species" = "survey_label")
        ) |>
        dplyr::select(-c("sample_species")) |>
        dplyr::rename(
          sample_scientific_name = "scientific_name",
          sample_alpha3_code = "alpha3_code"
        ) |>
        dplyr::relocate("sample_scientific_name", .after = "n_sample") |>
        dplyr::relocate(
          "sample_alpha3_code",
          .after = "sample_scientific_name"
        )
    } else {
      data |>
        dplyr::left_join(
          taxa_mapping,
          by = c("catch_taxon" = "survey_label")
        ) |>
        dplyr::select(-c("catch_taxon")) |>
        dplyr::relocate("scientific_name", .after = "n_catch") |>
        dplyr::relocate("alpha3_code", .after = "scientific_name")
    }
  taxa_map |>
    dplyr::left_join(gear_mapping, by = c("gear" = "survey_label")) |>
    dplyr::select(-c("gear")) |>
    dplyr::relocate("standard_name", .after = "vessel_type") |>
    dplyr::rename(gear = "standard_name") |>
    dplyr::left_join(
      vessels_mapping,
      by = c("vessel_type" = "survey_label")
    ) |>
    dplyr::select(-c("vessel_type")) |>
    dplyr::relocate(
      "standard_name",
      .after = dplyr::any_of(c("vessel_type", "gear"))
    ) |>
    dplyr::rename(vessel_type = "standard_name") |>
    dplyr::left_join(
      sites_mapping,
      by = c("landing_site" = "site_code")
    ) |>
    dplyr::select(-c("landing_site")) |>
    dplyr::relocate("site", .after = "BMU") |>
    dplyr::rename(landing_site = "site")
}

#' Standardize Enumerator Names
#'
#' Cleans and standardizes enumerator names by removing special characters,
#' fixing typos, and matching similar names using string distance.
#'
#' @param data A data frame with columns 'submission_id' and 'enumerator_name'
#' @param max_distance Maximum Levenshtein distance for matching similar names.
#'   Lower values are stricter. Default is 3.
#'
#' @return A data frame with two columns: 'submission_id' and 'enumerator_name_clean'
#'
#' @details
#' The function:
#' - Removes numbers and special characters
#' - Converts to lowercase
#' - Removes extra whitespace
#' - Marks single-word entries as "undefined"
#' - Matches similar names (e.g., "john smith" and "jhon smith")
#' - Returns the shorter/alphabetically first variant as the standard name
#'
#' @keywords preprocessing helper
#' @examples
#' \dontrun{
#' clean_names <- standardize_enumerator_names(raw_dat, max_distance = 2)
#' }
#'
#' @export
standardize_enumerator_names <- function(data = NULL, max_distance = 3) {
  # Step 1: Clean the names
  cleaned_data <- data |>
    dplyr::select(
      "submission_id",
      enumerator_name = "Name_of_Data_Collector"
    ) |>
    dplyr::mutate(
      cleaned_name = stringr::str_replace_all(
        .data$enumerator_name,
        "[^a-zA-Z ]",
        ""
      ),
      cleaned_name = stringr::str_squish(.data$cleaned_name),
      cleaned_name = stringr::str_trim(.data$cleaned_name),
      cleaned_name = tolower(.data$cleaned_name),
      cleaned_name = dplyr::if_else(
        stringr::str_detect(.data$cleaned_name, "\\s"),
        .data$cleaned_name,
        "undefined"
      ),
      cleaned_name = stringr::str_replace_all(.data$cleaned_name, "\\s+", "")
    )

  # Step 2: Get unique names for matching
  unique_names <- cleaned_data |>
    dplyr::filter(.data$cleaned_name != "undefined") |>
    dplyr::distinct(.data$cleaned_name) |>
    dplyr::pull(.data$cleaned_name)

  # Step 3: Find similar names and create mapping
  dist_matrix <- stringdist::stringdistmatrix(unique_names, method = "lv")
  dist_matrix <- as.matrix(dist_matrix)
  similar_idx <- which(
    dist_matrix <= max_distance & dist_matrix > 0,
    arr.ind = TRUE
  )
  similar_idx <- similar_idx[
    similar_idx[, 1] < similar_idx[, 2],
    ,
    drop = FALSE
  ]

  # Create name mapping
  name_mapping <- data.frame(
    cleaned_name = unique_names,
    standardized_name = unique_names,
    stringsAsFactors = FALSE
  )

  # Standardize to shorter/earlier name
  if (nrow(similar_idx) > 0) {
    for (i in 1:nrow(similar_idx)) {
      idx1 <- similar_idx[i, 1]
      idx2 <- similar_idx[i, 2]

      name1 <- unique_names[idx1]
      name2 <- unique_names[idx2]

      if (
        nchar(name1) < nchar(name2) ||
          (nchar(name1) == nchar(name2) && name1 < name2)
      ) {
        name_mapping$standardized_name[
          name_mapping$cleaned_name == name2
        ] <- name1
      } else {
        name_mapping$standardized_name[
          name_mapping$cleaned_name == name1
        ] <- name2
      }
    }
  }

  # Step 4: Apply mapping and return final result
  final_data <- cleaned_data |>
    dplyr::left_join(name_mapping, by = "cleaned_name") |>
    dplyr::mutate(
      enumerator_name_clean = dplyr::coalesce(
        .data$standardized_name,
        .data$cleaned_name
      )
    ) |>
    dplyr::select("submission_id", "enumerator_name_clean")

  return(final_data)
}
