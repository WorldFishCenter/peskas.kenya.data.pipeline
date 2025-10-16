conf <- read_config()

raw_dat <- download_parquet_from_cloud(
  prefix = conf$surveys$kefs$raw$file_prefix,
  provider = conf$storage$google$key,
  options = conf$storage$google$options
)

raw <- raw_dat |>
  dplyr::select(
    "submission_id",
    landing_date = "date_data",
    "BMU",
    landing_site = "Landing_Site",
    fishing_ground = "Area_Fished",
    jcma = "Are_you_within_a_JCMA_Joint_C",
    jcma_site = "If_YES_Please_choos_from_the_list_below",
    vessel_type = "Craft_Typ",
    boat_name = "vessel_name",
    vessel_reg_number = "vessel_reg",
    "captain_name",
    motorized = "Motorized_or_Non",
    hp = "HP",
    no_of_fishers = "NumCrew",
    fishing_trip_start = "Enter_date_and_time_sel_left_for_fishing",
    fishing_trip_end = "Enter_date_and_time_the_vessel_landed",
    "gear_category",
    gear = "gear_type",
    mesh_size = "MESH_SIZE",
    fuel = "FUEL_USED",
    catch_outcome = "Did_you_Catch_ANY_FISH_TODAY",
    catch_taxon = `CATCH_DETAILS.1.CATCH_DETAILS/marinespecies`,
    catch_kg = `CATCH_DETAILS.0.CATCH_DETAILS/TotalCatchWeight`,
    price_kg = `CATCH_DETAILS.0.CATCH_DETAILS/PricePerKg`,
    catch_price = `CATCH_DETAILS.0.CATCH_DETAILS/TValue`,
    catch_shark = "Did_you_catch_any_SHARK"
  )


unique(raw_dat$Landing_Site)
