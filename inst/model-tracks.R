#' Process GPS tracks for fishing effort analysis
#'
#' @description Main function to process GPS tracking data from fishing vessels,
#' identify fishing activities, and calculate effort metrics using speed-based
#' classification and spatial clustering algorithms.
#'
#' @param tracks_file Path to parquet file containing vessel tracking data.
#'   Must include columns: Time, Boat, Trip, Lat, Lng, Community
#' @param ports_df Data frame with port locations containing columns:
#'   BMU (port name), lat (latitude), lon (longitude)
#' @param port_buffer_m Buffer radius around ports in meters to exclude
#'   non-fishing activities (default 500m)
#' @param trip_gap_hours Time gap in hours to split tracks into separate trips
#'   (default 2 hours)
#' @param standardize_interval Seconds between standardized position intervals.
#'   Set to NULL to keep original temporal resolution (default 300 seconds)
#' @param fishing_speed_ms Maximum speed threshold for fishing activity in m/s
#'   (default 1.5 m/s, approximately 3 knots)
#' @param circle_radius_m Radius in meters for spatial clustering analysis
#'   to identify fishing grounds (default 2000m)
#' @param circle_time_window Number of time steps to consider for clustering
#'   (default 20)
#'
#' @return List containing:
#'   \item{processed_data}{SF object with classified fishing activities}
#'   \item{effort_by_trip}{Summary of fishing hours per trip}
#'   \item{spatial_effort}{Gridded spatial fishing effort}
#'   \item{overall_summary}{Overall fishing effort statistics}
#'   \item{parameters}{Parameters used in processing}
#'
#' @examples
#' \dontrun{
#' # Define port locations
#' ports <- data.frame(
#'   BMU = c("Port1", "Port2"),
#'   lat = c(-4.5, -3.8),
#'   lon = c(39.2, 39.8)
#' )
#'
#' # Process tracks with default parameters
#' results <- process_fishing_tracks(
#'   tracks_file = "vessel_tracks.parquet",
#'   ports_df = ports
#' )
#'
#' # Process with custom parameters for slower vessels
#' results <- process_fishing_tracks(
#'   tracks_file = "vessel_tracks.parquet",
#'   ports_df = ports,
#'   fishing_speed_ms = 1.0,  # Lower speed threshold
#'   standardize_interval = NULL  # Keep original resolution
#' )
#'
#' # Visualize results
#' plot <- visualize_fishing_track(results$processed_data)
#' print(plot)
#' }
#'
#' @keywords preprocessing
#' @export
process_fishing_tracks <- function(
  tracks_file,
  ports_df,
  port_buffer_m = 500,
  trip_gap_hours = 2,
  standardize_interval = 300,
  fishing_speed_ms = 1.5,
  circle_radius_m = 2000,
  circle_time_window = 20
) {
  # Check for required packages
  if (!requireNamespace("GPSMonitoring", quietly = TRUE)) {
    stop("Package 'GPSMonitoring' is required but not installed.")
  }
  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("Package 'arrow' is required but not installed.")
  }

  # Set s2 to FALSE for stability
  sf::sf_use_s2(FALSE)

  # Load tracks
  message("Loading tracks data...")
  tracks <- arrow::read_parquet(tracks_file)

  # Step 1: Prepare data
  message("Preparing GPS data...")
  gps_data <- prepare_gps_data(tracks)

  # Step 2: Create exclusion zones
  message("Creating exclusion zones...")
  exclude <- create_exclusion_zones(ports_df, buffer_m = port_buffer_m)
  pol.extent <- create_extent_polygon(gps_data)

  # Step 3: Curate data
  message("Curating GPS data...")
  gps_curated <- GPSMonitoring::GPS.curation(
    gps_data,
    extent = pol.extent,
    exclude = exclude
  )

  # Step 4: Process trajectories with proper speed calculation
  message("Processing trajectories...")
  standardize <- !is.null(standardize_interval)
  gps_processed <- process_trajectories_with_speed(
    gps_curated,
    trip_gap_seconds = trip_gap_hours * 3600,
    standardize = standardize,
    interval_seconds = ifelse(standardize, standardize_interval, NA)
  )

  # Step 5: Classify fishing activity
  message("Classifying fishing activities...")
  gps_classified <- classify_fishing_activity(
    gps_processed,
    speed_threshold = fishing_speed_ms,
    use_circles = TRUE,
    circle_radius = circle_radius_m,
    circle_window = circle_time_window
  )

  # Step 6: Calculate summaries
  message("Calculating summaries...")
  interval <- ifelse(
    standardize,
    standardize_interval,
    median(gps_processed$time_diff, na.rm = TRUE)
  )
  summaries <- calculate_fishing_summaries(gps_classified, interval)

  # Return everything
  return(list(
    processed_data = gps_classified,
    effort_by_trip = summaries$effort_by_trip,
    spatial_effort = summaries$spatial_effort,
    overall_summary = summaries$overall_summary,
    ports = ports_df,
    parameters = list(
      port_buffer_m = port_buffer_m,
      trip_gap_hours = trip_gap_hours,
      standardize_interval = standardize_interval,
      fishing_speed_ms = fishing_speed_ms
    )
  ))
}

#' Prepare GPS data in GPSMonitoring format
#'
#' @description Converts raw tracking data into format required by GPSMonitoring package
#' @param tracks Data frame with vessel tracking data
#' @return SF object with formatted GPS data
#' @keywords internal
prepare_gps_data <- function(tracks) {
  tracks %>%
    dplyr::mutate(
      filename = paste0("Boat_", Boat, "_Trip_", Trip, ".gpx"),
      code_village = Community,
      code_engin = Boat,
      code_pecheur = Boat,
      no_trajet = Trip,
      latitude = Lat,
      longitude = Lng,
      time = Time,
      date_heure = Time,
      track_fid = as.numeric(factor(Boat)),
      track_seg_id = as.numeric(factor(Boat))
    ) %>%
    dplyr::arrange(Boat, Time) %>%
    dplyr::group_by(filename) %>%
    dplyr::mutate(track_seg_point_id = dplyr::row_number()) %>%
    dplyr::ungroup() %>%
    sf::st_as_sf(coords = c("Lng", "Lat"), crs = 4326, remove = FALSE)
}

#' Create port exclusion zones
#'
#' @description Creates buffer zones around ports to exclude anchored vessels
#' @param ports_df Data frame with port locations
#' @param buffer_m Buffer radius in meters
#' @return SF object with unified exclusion zone
#' @keywords internal
create_exclusion_zones <- function(ports_df, buffer_m = 500) {
  # Convert buffer from meters to approximate degrees
  buffer_deg <- buffer_m / 111000 # rough conversion at equator

  ports_sf <- sf::st_as_sf(ports_df, coords = c("lon", "lat"), crs = 4326)
  port_buffers <- sf::st_buffer(ports_sf, dist = buffer_deg)
  exclude_geom <- sf::st_union(port_buffers)
  exclude_geom <- sf::st_make_valid(exclude_geom)
  sf::st_sf(id = 1, geometry = exclude_geom)
}

#' Create study area extent
#'
#' @description Creates bounding polygon for study area
#' @param gps_data SF object with GPS positions
#' @return SF polygon defining study extent
#' @keywords internal
create_extent_polygon <- function(gps_data) {
  bbox <- sf::st_bbox(gps_data)
  emprise <- matrix(
    c(
      bbox["xmin"] - 0.1,
      bbox["ymax"] + 0.1,
      bbox["xmax"] + 0.1,
      bbox["ymax"] + 0.1,
      bbox["xmax"] + 0.1,
      bbox["ymin"] - 0.1,
      bbox["xmin"] - 0.1,
      bbox["ymin"] - 0.1,
      bbox["xmin"] - 0.1,
      bbox["ymax"] + 0.1
    ),
    ncol = 2,
    byrow = TRUE
  )
  sf::st_sf(
    id = 1,
    geometry = sf::st_sfc(sf::st_polygon(list(emprise)), crs = 4326)
  )
}

#' Calculate vessel speeds from GPS positions
#'
#' @description Processes trajectories and calculates speed between consecutive points
#' @param gps_data SF object with GPS data
#' @param trip_gap_seconds Seconds between pings to define new trip
#' @param standardize Whether to standardize time intervals
#' @param interval_seconds Target interval in seconds if standardizing
#' @return SF object with speed calculations
#' @keywords internal
process_trajectories_with_speed <- function(
  gps_data,
  trip_gap_seconds = 7200,
  standardize = TRUE,
  interval_seconds = 300
) {
  # Add coordinates
  gps_with_coords <- gps_data %>%
    dplyr::mutate(
      x = sf::st_coordinates(.)[, 1],
      y = sf::st_coordinates(.)[, 2]
    )

  if (standardize) {
    # Standardize time intervals using Redis.traj
    message("  Standardizing time intervals...")
    gps_redis <- GPSMonitoring::Redis.traj(
      GPS.data = sf::st_drop_geometry(gps_with_coords),
      step = interval_seconds,
      silent = TRUE
    )

    # Convert back to sf and calculate proper distances
    gps_processed <- sf::st_as_sf(
      gps_redis,
      coords = c("x", "y"),
      crs = 4326
    ) %>%
      dplyr::arrange(no_trajet, date) %>%
      dplyr::group_by(no_trajet) %>%
      dplyr::mutate(
        prev_geom = dplyr::lag(geometry),
        dist_meters = ifelse(
          is.na(prev_geom),
          0,
          as.numeric(sf::st_distance(geometry, prev_geom, by_element = TRUE))
        ),
        speed_ms = dist_meters / interval_seconds,
        speed_kmh = speed_ms * 3.6,
        speed_knots = speed_ms * 1.94384,
        time_diff = interval_seconds
      ) %>%
      dplyr::ungroup() %>%
      dplyr::select(-prev_geom)
  } else {
    # Don't standardize, just calculate speeds from original data
    gps_processed <- gps_with_coords %>%
      dplyr::arrange(code_engin, time) %>%
      dplyr::group_by(code_engin, no_trajet) %>%
      dplyr::mutate(
        prev_geom = dplyr::lag(geometry),
        prev_time = dplyr::lag(time),
        time_diff = as.numeric(difftime(time, prev_time, units = "secs")),
        dist_meters = ifelse(
          is.na(prev_geom),
          0,
          as.numeric(sf::st_distance(geometry, prev_geom, by_element = TRUE))
        ),
        speed_ms = ifelse(time_diff > 0, dist_meters / time_diff, 0),
        speed_kmh = speed_ms * 3.6,
        speed_knots = speed_ms * 1.94384,
        date = time # Add date column for compatibility
      ) %>%
      dplyr::ungroup() %>%
      dplyr::select(-prev_geom, -prev_time)
  }

  return(gps_processed)
}

#' Classify fishing vs transit activities
#'
#' @description Uses speed thresholds and spatial clustering to identify fishing
#' @param gps_data SF object with speed calculations
#' @param speed_threshold Maximum speed for fishing (m/s)
#' @param use_circles Whether to use spatial clustering refinement
#' @param circle_radius Radius for clustering analysis (meters)
#' @param circle_window Time window for clustering
#' @return SF object with activity classification
#' @keywords internal
classify_fishing_activity <- function(
  gps_data,
  speed_threshold = 1.5,
  use_circles = TRUE,
  circle_radius = 2000,
  circle_window = 20
) {
  # Initial classification based on speed
  gps_classified <- gps_data %>%
    dplyr::mutate(
      activity = dplyr::case_when(
        is.na(speed_ms) ~ "UK",
        speed_ms < speed_threshold ~ "active",
        TRUE ~ "UK"
      ),
      id = dplyr::row_number()
    )

  if (use_circles) {
    message("  Adding circular statistics...")

    # Add circular statistics
    gps_with_circles <- GPSMonitoring::all.add.nb.point(
      gps_classified,
      r = circle_radius,
      temp_windows = circle_window
    )

    # Refine classification
    column_name <- paste0("circle", circle_radius)
    gps_classified <- gps_with_circles %>%
      dplyr::mutate(
        circle_stat = !!rlang::sym(column_name),
        # Create refined activity classification
        activity_refined = dplyr::case_when(
          # Definitely fishing: low speed + high point density
          !is.na(circle_stat) & circle_stat > 10 & speed_ms < 1.0 ~ "active",
          # Definitely transit: high speed + low point density
          !is.na(circle_stat) & circle_stat < 3 & speed_ms > 2.5 ~ "UK",
          # Medium density + very low speed = likely fishing
          !is.na(circle_stat) & circle_stat > 5 & speed_ms < speed_threshold ~
            "active",
          # Otherwise keep speed-based classification
          TRUE ~ activity
        ),
        # Use refined classification as main activity
        activity = activity_refined
      ) %>%
      dplyr::select(-activity_refined, -circle_stat)
  }

  return(gps_classified)
}

#' Calculate fishing effort metrics
#'
#' @description Summarizes fishing effort by trip and spatial grid
#' @param gps_data SF object with classified activities
#' @param interval_seconds Time interval between positions
#' @return List with effort summaries
#' @keywords internal
calculate_fishing_summaries <- function(gps_data, interval_seconds = 300) {
  # Effort by trip
  effort_by_trip <- gps_data %>%
    sf::st_drop_geometry() %>%
    dplyr::filter(activity == "active") %>%
    dplyr::group_by(no_trajet) %>%
    dplyr::summarise(
      n_points = dplyr::n(),
      fishing_hours = n_points * interval_seconds / 3600,
      start_fishing = min(date),
      end_fishing = max(date),
      .groups = "drop"
    )

  # Spatial effort
  grid_size <- 0.01 # ~1km
  grid <- sf::st_make_grid(
    gps_data,
    cellsize = c(grid_size, grid_size),
    square = TRUE
  )
  grid <- sf::st_sf(grid_id = 1:length(grid), geometry = grid)

  spatial_effort <- sf::st_join(
    grid,
    dplyr::filter(gps_data, activity == "active"),
    left = FALSE
  ) %>%
    dplyr::group_by(grid_id) %>%
    dplyr::summarise(
      fishing_events = dplyr::n(),
      total_hours = fishing_events * interval_seconds / 3600,
      .groups = "drop"
    )

  # Overall summary
  overall_summary <- gps_data %>%
    sf::st_drop_geometry() %>%
    dplyr::summarise(
      total_trips = dplyr::n_distinct(no_trajet),
      total_points = dplyr::n(),
      fishing_points = sum(activity == "active", na.rm = TRUE),
      transit_points = sum(activity == "UK", na.rm = TRUE),
      percent_fishing = 100 * fishing_points / total_points,
      total_fishing_hours = fishing_points * interval_seconds / 3600
    )

  return(list(
    effort_by_trip = effort_by_trip,
    spatial_effort = spatial_effort,
    overall_summary = overall_summary
  ))
}

#' Visualize fishing track with activity classification
#'
#' @description Creates map showing fishing vs transit activities for a track
#' @param processed_data SF object with classified activities
#' @param track_id Specific track ID to plot (NULL for random selection)
#' @return ggplot object
#'
#' @examples
#' \dontrun{
#' # Visualize random track
#' plot <- visualize_fishing_track(results$processed_data)
#'
#' # Visualize specific track
#' plot <- visualize_fishing_track(results$processed_data, track_id = 12345)
#' }
#'
#' @keywords helper
#' @export
visualize_fishing_track <- function(processed_data, track_id = NULL) {
  if (is.null(track_id)) {
    track_id <- sample(unique(processed_data$no_trajet), 1)
  }

  single_track <- dplyr::filter(processed_data, no_trajet == track_id)

  p <- ggplot2::ggplot(single_track) +
    ggplot2::geom_path(
      data = sf::st_coordinates(single_track) %>% as.data.frame(),
      ggplot2::aes(x = X, y = Y),
      alpha = 0.3,
      size = 0.5
    ) +
    ggplot2::geom_sf(ggplot2::aes(color = activity), size = 2, alpha = 0.8) +
    ggplot2::scale_color_manual(
      values = c("active" = "red", "UK" = "blue"),
      labels = c("active" = "Fishing", "UK" = "Transit"),
      name = "Activity"
    ) +
    ggplot2::theme_minimal() +
    ggplot2::labs(
      title = paste("Track", track_id, "- Fishing Activity"),
      x = "Longitude",
      y = "Latitude"
    ) +
    ggplot2::theme(
      plot.title = ggplot2::element_text(hjust = 0.5),
      legend.position = "bottom"
    )

  return(p)
}


ports <- data.frame(
  BMU = c(
    "Bureni",
    "Chale",
    "Gazi",
    "Jimbo",
    "Kanamai",
    "Kenyatta",
    "Kibuyuni",
    "Kijangwani",
    "Kuruwitu",
    "Marina",
    "Mgwani",
    "Mkwiro",
    "Msumarini",
    "Mtwapa",
    "Mvuleni",
    "Mwaepe",
    "Mwanyaza",
    "Nyali",
    "Reef",
    "Shimoni",
    "Tradewinds",
    "Vanga",
    "Vipingo",
    "Wasini"
  ),
  lat = c(
    -3.83217,
    -4.437990256,
    -4.428604,
    -4.674807,
    -3.928567972,
    -4.005982386,
    -4.638997,
    -3.780836113,
    -3.792731815,
    -3.9601083,
    -4.3833826,
    -4.6621,
    -3.873126543,
    -3.95076,
    -4.358683442,
    -4.343164,
    -4.369472,
    -4.0524885,
    -4.030643153,
    -4.647653,
    -4.304135,
    -4.662195,
    -3.814323429,
    -4.657433
  ),
  lon = c(
    39.825659,
    39.53588791,
    39.511569,
    39.215278,
    39.78030992,
    39.72705319,
    39.341125,
    39.84024926,
    39.83678678,
    39.7560211,
    39.5531845,
    39.395194,
    39.80664847,
    39.745094,
    39.56181226,
    39.566298,
    39.558043,
    39.70611691,
    39.72067205,
    39.380669,
    39.58267,
    39.228392,
    39.83032521,
    39.364912
  )
)
