#' Pre-process WorldFish Kenya Gleaning Surveys
#'
#' Downloads raw structured gleaning survey data from cloud storage and
#' preprocesses it into a single analysis-ready data frame. The function
#' assembles three pieces and joins them on the submission:
#' \enumerate{
#'   \item \strong{General info} -- strips the Kobo group prefixes
#'         (`group_general/`, `group_trip/`, `no_fishers/`, `demographics/`,
#'         `group_gleaning_activity/`, `group_supply_chain/`), selects and
#'         renames the trip, demographic, activity and supply-chain fields,
#'         coalesces the conditional `landing_site` columns into one, and
#'         coerces dates and numeric fields.
#'   \item \strong{Catch info} -- reshapes the wide `group_catch` block into a
#'         tidy long table (one row per submission x shell group x size class)
#'         via \code{\link{reshape_gleaning_catch}}, unifying the parallel
#'         bucket/plastic container fields and applying conservative
#'         sanitisation.
#'   \item \strong{Catch totals} -- per submission, sums individuals across
#'         size classes (`total_individuals`) and reconstructs catch weight as
#'         `unit_weight_kg * n_containers` (`total_catch_kg`); the container
#'         fields are constant within a submission, hence `first()`.
#' }
#'
#' Configurations are read from `config.yml` with the following necessary
#' parameters:
#'
#' ```
#' surveys:
#'   wf_gleaning:
#'     raw:
#'       file_prefix:
#'       version:
#' storage:
#'   google:
#'     key:
#'     options:
#'       project:
#'       bucket:
#'       service_account_key:
#' ```
#'
#' The function uses logging to track progress.
#'
#' @param log_threshold Logging threshold level (default: `logger::DEBUG`).
#' @return A data frame of preprocessed gleaning surveys: one row per
#'   submission x shell group x size class, with general/demographic/activity/
#'   supply-chain fields plus `total_individuals` and `total_catch_kg`.
#' @export
#' @keywords workflow preprocessing
#' @seealso \code{\link{reshape_gleaning_catch}}
preprocess_wf_gleaning <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  raw_dat <- coasts::download_parquet_from_cloud(
    prefix = conf$surveys$wf_gleaning$raw$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options,
    version = conf$surveys$wf_gleaning$raw$version
  )

  general_info <-
    raw_dat %>%
    dplyr::rename_with(~ stringr::str_remove(., "group_general/")) |>
    dplyr::rename_with(~ stringr::str_remove(., "group_trip/")) |>
    dplyr::rename_with(~ stringr::str_remove(., "no_fishers/")) |>
    dplyr::rename_with(~ stringr::str_remove(., "demographics/")) |>
    dplyr::rename_with(~ stringr::str_remove(., "group_gleaning_activity/")) |>
    dplyr::rename_with(~ stringr::str_remove(., "group_supply_chain/")) |>
    janitor::clean_names() |>
    dplyr::select(
      # general
      "submission_id",
      #submitted_by = "submitted_by",
      submission_date = "submission_time",
      "landing_date",
      district = "county",
      dplyr::contains("landing_site"),
      "collect_data_today",
      survey_activity = "gleaners_collected",
      # demographics
      fisher_name = "gleaner_name",
      "gender",
      age = "how_old_is_the_gleaner",
      "education",
      challenges_before = "what_challenges_do_you_face_be",
      habitat_revenue = "in_which_habitat_do_h_in_monetary_terms",
      # activity
      "days_collection_week",
      start_time = "what_time_did_you_start_gleaning",
      end_time = "at_what_time_did_you_end_gleaning",
      gleaning_site_distance = "what_is_the_distance_leaning_site_km_min",
      fishing_ground = "which_area_s_did_you_glean_today",
      habitat = "where_did_you_go_to_collect_sh",
      challenges_during = "what_challenges_do_you_face_du",
      "transport",
      vessel_type = dplyr::any_of("vessel"),
      dplyr::any_of("vessel_status"),
      dplyr::any_of("vessel_cost"),
      fuel_L = dplyr::any_of("fuel"),
      propulsion_gear = dplyr::any_of("propulsion"),
      dplyr::any_of(dplyr::ends_with("_n")),
      gear = "what_kind_of_equipment_gears_did_you_use",
      catch_outcome = "collect_today",
      # supply chain
      "conservation",
      "home_consumption",
      process_food = dplyr::any_of("Do_you_process_some_of_the_spe"),
      process_what = dplyr::any_of("If_yes_which_one"),
      process_how = dplyr::any_of("How_do_you_processes_them"),
      process_why = dplyr::any_of("And_why_do_you_process"),
      "catch_use",
      "market",
      "who_selling",
      who_selling_provenance = dplyr::any_of(
        "Where_are_the_buyers_products_come_from_"
      ),
      who_selling_gener = dplyr::any_of("Which_gender_dominate_the_buyers"),
      who_selling_scale = dplyr::any_of(
        "What_is_the_scale_of_ions_of_these_buyers"
      ),
      selling_time = dplyr::any_of("How_long_does_it_tak_ies_after_harvesting"),
      challenges_selling = dplyr::any_of(
        "Did_you_experience_a_oducts_today_Yes_No"
      ),
      challenges_selling_why = dplyr::any_of("If_yes_please_select_from_the"),
      not_sold_food = dplyr::any_of("What_do_you_do_with_that_you_don_t_sell"),
      daily_income_percentage = dplyr::any_of(
        "How much of your daily income comes from gleaning?"
      ),
      catch_price = "revenue",
      happiness_rating = "happiness"
    ) %>%
    dplyr::mutate(
      landing_site = dplyr::coalesce(
        !!!dplyr::select(., dplyr::contains("landing_site"))
      ),
      start = hms::as_hms(substr(.data$start_time, 1, 8)),
      end = hms::as_hms(substr(.data$end_time, 1, 8)),
      trip_duration = as.numeric(.data$end - .data$start, units = "hours"),
      # handle any trip that crosses midnight (end < start)
      trip_duration = dplyr::if_else(
        .data$trip_duration < 0,
        .data$trip_duration + 24,
        .data$trip_duration
      )
    ) |>
    dplyr::select(
      -dplyr::contains("landing_site"),
      -"start_time",
      -"end_time",
      "start",
      "end",
      "landing_site"
    ) |>
    dplyr::relocate("landing_site", .after = "district") |>
    dplyr::relocate("trip_duration", .after = "days_collection_week") |>
    dplyr::mutate(
      landing_date = lubridate::as_date(.data$landing_date),
      submission_date = lubridate::as_date(.data$submission_date),
      dplyr::across(
        c(
          dplyr::contains("days_collection_week"),
          "trip_duration",
          "catch_price",
          dplyr::ends_with("_n")
        ),
        ~ as.double(.x)
      )
    )

  catch_info <-
    raw_dat %>%
    dplyr::rename_with(~ stringr::str_remove(., "group_general/")) |>
    dplyr::rename_with(~ stringr::str_remove(., "group_trip/")) |>
    dplyr::rename_with(~ stringr::str_remove(., "no_fishers/")) |>
    dplyr::rename_with(~ stringr::str_remove(., "demographics/")) |>
    dplyr::rename_with(~ stringr::str_remove(., "group_gleaning_activity/")) |>
    dplyr::rename_with(~ stringr::str_remove(., "group_supply_chain/")) |>
    dplyr::select(
      "submission_id",
      "collect_data_today",
      survey_activity = "gleaners_collected",
      catch_outcome = "collect_data_today",
      dplyr::starts_with("CATCH.")
    ) |>
    reshape_gleaning_catch()

  # ---- Per-submission totals -------------------------------------------------
  # total_catch_kg: per-taxon weight repeats down an instance's size-class rows,
  # so dedupe to one weight per (submission, catch_id, taxon) before summing.
  weight_per_submission <-
    catch_info |>
    dplyr::distinct(
      .data$submission_id,
      .data$catch_id,
      .data$taxon,
      .data$total_weight_kg
    ) |>
    dplyr::group_by(.data$submission_id) |>
    dplyr::summarise(
      total_catch_kg = sum(.data$total_weight_kg, na.rm = TRUE),
      .groups = "drop"
    )

  catch_totals <-
    catch_info |>
    dplyr::group_by(.data$submission_id) |>
    dplyr::summarise(
      total_individuals = sum(.data$n_individuals, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::left_join(weight_per_submission, by = "submission_id")

  gleaning <-
    general_info |>
    dplyr::left_join(
      catch_info |>
        dplyr::select(-dplyr::any_of(c("catch_outcome", "survey_activity"))),
      by = "submission_id"
    ) |>
    dplyr::left_join(catch_totals, by = "submission_id")

  # upload preprocessed landings
  coasts::upload_parquet_to_cloud(
    data = gleaning,
    prefix = conf$surveys$wf_gleaning$preprocessed$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )
}

#' Parse a Free-Text Numeric Field
#'
#' Extracts the first number from a messy free-text entry. The Kenya gleaning
#' form stores prices and some counts as text, so values arrive as `"100/-"`,
#' `" 130"` (leading space), `"I don't sell"`, etc. Returns the leading numeric
#' value (`100`, `130`) or NA when no number is present (`"I don't sell"`).
#'
#' @param x A character vector.
#' @return A double vector.
#' @keywords internal
#' @export
parse_numeric_text <- function(x) {
  as.double(stringr::str_extract(as.character(x), "[0-9]+\\.?[0-9]*"))
}


#' Reshape Kenya Gleaning Catch Data from Wide to Long Format
#'
#' Reshapes the repeated `CATCH` group of the Kenya intertidal gleaning
#' KoboToolbox survey into one tidy long table. Unlike the Zanzibar form, here:
#' \itemize{
#'   \item `CATCH` is a \strong{repeat} group: each submission can hold several
#'         catch instances (`CATCH.0`, `CATCH.1`, ...), one taxon each.
#'   \item Each instance carries its own \strong{total weight (kg)} and
#'         \strong{price/kg} (no bucket/plastic container method).
#'   \item Size bins are \strong{taxon-specific}: length classes for
#'         gastropods/bivalves/crabs/sea cucumbers, \strong{weight} classes for
#'         octopus, free \strong{text} for `others`, and \strong{none} for
#'         seaweed (weight only).
#' }
#'
#' Output grain: one row per `submission_id` x `catch_id` x `taxon` x
#' `size_class`. Per-taxon `total_weight_kg` and `price_per_kg` repeat down the
#' size-class rows of an instance. Heterogeneous bins are unified via three
#' columns: `size_class` (small/medium/large/xlarge), `size_metric`
#' (`length_cm` / `weight_kg` / `none`), and numeric `bound_min`/`bound_max`
#' (cm for length taxa, kg for octopus, NA otherwise). Seaweed yields a single
#' row (no size class); `others` keeps its raw text counts in `n_individuals_raw`
#' alongside the numeric parse in `n_individuals`. Submissions with no catch
#' detail are preserved as a single context row so none is dropped.
#'
#' Current-form fields are treated as authoritative over legacy duplicates:
#' gastropod small uses `Small_5cm_001` (not `small_5`); seaweed weight
#' coalesces `Weight_of_Seaweed_Kg` over the legacy `Weight_of_Seaweed_kg`.
#' Free-text prices/counts are parsed with [parse_numeric_text()], with the
#' original price kept in `price_per_kg_raw`.
#'
#' The taxon registry below also covers prawns and squid (defined in the form
#' but unused in this export); they activate automatically if a future export
#' contains them.
#'
#' @param data A data frame of the Kenya gleaning export, with raw Kobo repeat
#'   columns named `CATCH.{i}.CATCH/...`.
#' @param max_catch Maximum number of repeat instances to scan (default 3).
#'
#' @return A long data frame, one row per submission x catch instance x taxon x
#'   size class (plus a context row for submissions without catch detail).
#' @export
#'
#' @keywords internal
#'
#' @examples
#' \dontrun{
#' catch_long <- reshape_gleaning_catch(catch_info)
#'
#' # Per-instance catch weight (deduped: weight repeats across size rows)
#' catch_long |>
#'   dplyr::distinct(submission_id, catch_id, taxon, total_weight_kg, price_per_kg)
#' }
reshape_gleaning_catch <- function(data = NULL, max_catch = 3) {
  # Accessor for a repeat-indexed column; all-NA if absent from this export.
  pull_col <- function(i, suffix) {
    nm <- paste0("CATCH.", i, ".CATCH/", suffix)
    if (nm %in% names(data)) data[[nm]] else rep(NA_character_, nrow(data))
  }

  # ---- Taxon registry ------------------------------------------------------
  # Each entry: taxon label, shell_group code, block prefix, weight/price/photo
  # field names, species field (NA if none), size metric, and the size bins as
  # tuples c(size_class, count_field, bound_min, bound_max).
  taxa <- list(
    list(
      taxon = "gastropod",
      code = "gastropod",
      prefix = "group_length_gastropods/",
      weight = "Total_weight_of_gastropods_kgs",
      price = "How_much_do_you_sell_per_Kg_001",
      photo = "photo",
      species = "Group_Gastropod",
      metric = "length_cm",
      bins = list(
        c("small", "Small_5cm_001", "0", "5"),
        c("medium", "_5_15", "5", "15"),
        c("large", "large_15", "15", NA)
      )
    ),
    list(
      taxon = "bivalves",
      code = "bivalves",
      prefix = "group_rg0hr89/",
      weight = "Total_weight_of_Bivalves_kgs",
      price = "How_much_do_you_sell_per_Kg",
      photo = "photo_001",
      species = "Group_Bivalves",
      metric = "length_cm",
      bins = list(
        c("small", "Small_5cm", "0", "5"),
        c("medium", "_5_15_001", "5", "15"),
        c("large", "large_15_001", "15", NA)
      )
    ),
    list(
      taxon = "crab",
      code = "crustaceans",
      prefix = "group_va6sc73/",
      weight = "Total_weight_of_crabs_kgs",
      price = "How_much_do_you_sell_per_Kg_002",
      photo = "Photo_002",
      species = "Group_Crustaceans",
      metric = "length_cm",
      bins = list(
        c("small", "Small_10cm_001", "0", "10"),
        c("medium", "Medium_10_20cm", "10", "20"),
        c("large", "Large_20cm", "20", NA)
      )
    ),
    list(
      taxon = "sea_cucumber",
      code = "echinodermata",
      prefix = "group_lw2gv67/",
      weight = "Total_weight_of_sea_umber_collected_kgs",
      price = "How_much_do_you_sell_per_Kg_005",
      photo = "Photo_004",
      species = "Echinoderms_Sea_cucumbers",
      metric = "length_cm",
      bins = list(
        c("small", "Small_15cm", "0", "15"),
        c("medium", "Medium_15_30cm", "15", "30"),
        c("large", "Large_30cm", "30", NA)
      )
    ),
    list(
      taxon = "octopus",
      code = "option_6",
      prefix = "group_kp2bh56/",
      weight = "Total_weight_of_kgs_of_Octopus_Kgs",
      price = "How_much_do_you_sell_per_Kg_004",
      photo = "Photo_003",
      species = "Cephalopods",
      metric = "weight_kg",
      bins = list(
        c("small", "Small_25cm", "0", "1"),
        c("medium", "Medium_25_60cm", "1", "2"),
        c("large", "Large_60cm", "2", "3"),
        c("xlarge", "Extra_large_3_5kgs", "3", "5")
      )
    ),
    list(
      taxon = "prawn",
      code = "option_9",
      prefix = "group_zt65j40/",
      weight = "Total_weight_of_prawns_collected_Kgs",
      price = "How_much_do_you_sell_per_Kg_003",
      photo = "Photo_006",
      species = "Crustaceans_Prawns",
      metric = "length_cm",
      bins = list(
        c("small", "Small_3cm", "0", "3"),
        c("medium", "Medium_3_10cm", "3", "10"),
        c("large", "Large_10cm", "10", NA)
      )
    ),
    list(
      taxon = "squid",
      code = "option_10",
      prefix = "group_ah6rj08/",
      weight = "Total_weight_of_squids_collected_kgs",
      price = "How_much_do_you_sell_per_Kg_006",
      photo = "Photo_005",
      species = "Cephalopods_Squids",
      metric = "length_cm",
      bins = list(
        c("small", "Small_25cm_001", "0", "25"),
        c("medium", "Medium_25_60cm_001", "25", "60"),
        c("large", "Large_60cm_001", "60", NA)
      )
    ),
    list(
      taxon = "seaweed",
      code = "algae__seaweeds",
      prefix = "group_ej35k64/",
      weight = NA,
      price = "How_much_do_you_sell_per_Kg_007",
      photo = "Photos",
      species = "Algae_Seaweeds",
      metric = "none",
      bins = list()
    ),
    list(
      taxon = "others",
      code = "others",
      prefix = "group_aj5rv34/",
      weight = "Total_weight_other_species_group",
      price = "How_much_do_you_sell_per_Kg_008",
      photo = "Photo_007",
      species = NA,
      metric = "none",
      bins = list(
        c("small", "Small_No_of_individual_and_length", NA, NA),
        c("medium", "Medium_No_of_individual_and_length", NA, NA),
        c("large", "Larger_No_of_individual_and_length", NA, NA)
      )
    )
  )

  # ---- Build one taxon block within one repeat instance --------------------
  build_block <- function(i, sp) {
    sg <- pull_col(i, "shell_group")
    keep <- which(sg == sp$code)
    if (length(keep) == 0) {
      return(NULL)
    }
    n <- length(keep)

    weight <- if (sp$taxon == "seaweed") {
      dplyr::coalesce(
        parse_numeric_text(pull_col(
          i,
          paste0(sp$prefix, "Weight_of_Seaweed_Kg")
        )[keep]),
        parse_numeric_text(pull_col(
          i,
          paste0(sp$prefix, "Weight_of_Seaweed_kg")
        )[keep])
      )
    } else {
      parse_numeric_text(pull_col(i, paste0(sp$prefix, sp$weight))[keep])
    }

    price_raw <- pull_col(i, paste0(sp$prefix, sp$price))[keep]
    species <- if (sp$taxon == "others") {
      pull_col(i, "others")[keep]
    } else if (is.na(sp$species)) {
      rep(NA_character_, n)
    } else {
      pull_col(i, sp$species)[keep]
    }
    local_name <- dplyr::coalesce(
      pull_col(i, "Give_the_local_name_es_if_you_don_t_know")[keep],
      pull_col(i, "Give_the_local_name_es_if_you_don_t_know_001")[keep]
    )

    base <- tibble::tibble(
      submission_id = data$submission_id[keep],
      catch_id = i,
      shell_group = sp$code,
      taxon = sp$taxon,
      species_codes = species,
      local_name = local_name,
      total_weight_kg = weight,
      price_per_kg = parse_numeric_text(price_raw),
      price_per_kg_raw = price_raw,
      size_metric = sp$metric,
      quantity_changed = pull_col(
        i,
        "Has_the_quantity_of_es_changed_over_time"
      )[keep],
      change_reasons = pull_col(i, "If_the_quantity_chan_the_specific_reasons")[
        keep
      ],
      length_photo = pull_col(i, paste0(sp$prefix, sp$photo))[keep]
    )

    if (length(sp$bins) == 0) {
      # seaweed: single row, no size class
      base |>
        dplyr::mutate(
          size_class = NA_character_,
          bound_min = NA_real_,
          bound_max = NA_real_,
          n_individuals = NA_real_,
          n_individuals_raw = NA_character_
        )
    } else {
      purrr::map_dfr(sp$bins, function(b) {
        cnt_raw <- pull_col(i, paste0(sp$prefix, b[[2]]))[keep]
        base |>
          dplyr::mutate(
            size_class = b[[1]],
            bound_min = suppressWarnings(as.double(b[[3]])),
            bound_max = suppressWarnings(as.double(b[[4]])),
            n_individuals = parse_numeric_text(cnt_raw),
            n_individuals_raw = if (sp$taxon == "others") {
              cnt_raw
            } else {
              NA_character_
            }
          )
      })
    }
  }

  # ---- Assemble across instances x taxa ------------------------------------
  long <- purrr::map_dfr(seq.int(0, max_catch - 1), function(i) {
    purrr::map_dfr(taxa, function(sp) build_block(i, sp))
  })

  # Submissions with no catch detail in any instance -> single context row.
  no_detail <- data |>
    dplyr::filter(!.data$submission_id %in% long$submission_id) |>
    dplyr::transmute(
      submission_id = .data$submission_id,
      catch_id = 0L,
      shell_group = NA_character_,
      taxon = NA_character_,
      species_codes = NA_character_,
      local_name = NA_character_,
      total_weight_kg = NA_real_,
      price_per_kg = NA_real_,
      price_per_kg_raw = NA_character_,
      size_metric = NA_character_,
      quantity_changed = NA_character_,
      change_reasons = NA_character_,
      length_photo = NA_character_,
      size_class = NA_character_,
      bound_min = NA_real_,
      bound_max = NA_real_,
      n_individuals = NA_real_,
      n_individuals_raw = NA_character_
    )

  size_levels <- c("small", "medium", "large", "xlarge")

  dplyr::bind_rows(long, no_detail) |>
    dplyr::mutate(
      n_species = stringr::str_count(.data$species_codes, "\\S+"),
      size_class = factor(
        .data$size_class,
        levels = size_levels,
        ordered = TRUE
      )
    ) |>
    dplyr::left_join(
      dplyr::select(
        data,
        dplyr::any_of(
          c("submission_id", "catch_outcome", "survey_activity")
        )
      ),
      by = "submission_id"
    ) |>
    dplyr::select(
      "submission_id",
      dplyr::any_of(c("catch_outcome", "survey_activity")),
      "catch_id",
      "shell_group",
      "taxon",
      "species_codes",
      "n_species",
      "local_name",
      "total_weight_kg",
      "price_per_kg",
      "price_per_kg_raw",
      "size_class",
      "size_metric",
      "bound_min",
      "bound_max",
      "n_individuals",
      "n_individuals_raw",
      "quantity_changed",
      "change_reasons",
      "length_photo"
    ) |>
    dplyr::arrange(
      .data$submission_id,
      .data$catch_id,
      .data$taxon,
      .data$size_class
    )
}
