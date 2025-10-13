#' @importFrom utils globalVariables
#' @importFrom stats lag time
#' @importFrom dplyr n n_distinct arrange row_number
#' @importFrom rlang sym
utils::globalVariables(c(
  ".",
  # Track processing variables
  "Boat", "Community", "Lat", "Lng", "Time", "Trip",
  "activity", "activity_refined", "circle_stat",
  "code_engin", "dist_meters", "filename", "geometry",
  "no_trajet", "prev_geom", "prev_time", "speed_ms",
  "time_diff", "X", "Y",
  # Fishery metrics variables
  "landing_date", "submission_id", "landing_site",
  "year_month", "trip_id", "gear", "no_of_fishers",
  "catch_kg", "total_catch_per_trip", "predominant_gear",
  "trip_catch", "cpue_per_fisher", "total_catch_price",
  "trip_revenue", "rpue_per_fisher", "fish_category",
  "species_catch_kg", "species_pct", "avg_fishers_per_trip",
  "avg_catch_per_trip", "pct_main_gear", "cpue", "rpue",
  "metric_type", "gear_type", "species", "size",
  # Spatial analysis variables
  "grid_id", "fishing_events", "fishing_points",
  "total_points", "n_points"
))
