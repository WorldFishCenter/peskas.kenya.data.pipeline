test_that("get_catch_flags bounds each fish by its species' FishBase lengths", {
  rows <- tibble::tibble(
    submission_id = as.character(1:6),
    catch_outcome = "yes",
    total_sample_weight = 5,
    total_catch_weight = 10,
    n_sample = 1,
    sample_scientific_name = c(
      "Siganus sutor", "Siganus sutor", "Scomberomorus commerson",
      "Scylla serrata", "Siganus sutor", "Unknown fish"
    ),
    sample_alpha3_code = c("IUU", "IUU", "COM", "MUD", "IUU", "XXX"),
    sample_weight = 1,
    sample_price = 100,
    length_type = c(
      "total_length", "total_length", "total_length",
      "carapace_length", "carapace_length", "total_length"
    ),
    length_min_cm = c(20, 0.2, 150, 15, 312, 501),
    length_max_cm = c(30, 25, 150, 15, 312, 501)
  )
  species_lengths <- tibble::tibble(
    alpha3_code = c("IUU", "COM", "MUD"),
    original_name = c("Siganus sutor", "Scomberomorus commerson", "Scylla serrata"),
    server = c("fishbase", "fishbase", "sealifebase"),
    order = c("Perciformes", "Scombriformes", NA),
    length_max_tl = c(54.9, 266, 9.15),
    length_common_tl = c(NA, 120, NA)
  )

  flags <- get_catch_flags(rows, species_lengths)
  codes <- stats::setNames(flags$alert_flag_catch, flags$submission_id)

  expect_equal(
    unname(codes[as.character(1:6)]),
    c(NA, "5.4", NA, NA, "5.3", "5.3")
  )
  where <- flags$alert_catches[[which(flags$submission_id == "5")]]
  expect_equal(where$catch_number, 1L)
  expect_equal(where$value, "312 cm")
})
