# One KEFS trip with four composition rows: a measured species, a second grade
# of it (its fish sit on the first row), an unmeasured species, and a measured
# species with one fish weight missing.
kefs <- tibble::tibble(
  submission_id = "1",
  landing_date = as.Date("2026-01-02"),
  gaul_1_code = "1",
  gaul_1_name = "Kilifi",
  gaul_2_code = "2",
  gaul_2_name = "Kilifi North",
  landing_site = "Takaungu",
  no_of_fishers = 3L,
  trip_duration = 5,
  gear = "Gillnet",
  vessel_type = "Canoe",
  habitat = "Reef",
  catch_outcome = "yes",
  n_sample = c(1, 2, 3, 4),
  sample_alpha3_code = c("SIU", "SIU", "OCT", "LUJ"),
  sample_scientific_name = c("Siganus sutor", "Siganus sutor", "Octopus", "Lutjanus"),
  sample_weight = c(6, 2, 1, 4),
  sample_price = 100,
  length_cm = c(26.7, 26.7, NA, 30),
  n_measured = c(3L, 3L, NA, 2L),
  total_catch_weight = 20,
  total_catch_price = 2000
)
fish <- tibble::tibble(
  submission_id = "1",
  n_sample = c(1, 1, 1, 4, 4),
  length_cm = c(20, 30, 30, 28, 32),
  priority_weight = c(1, 2, 3, NA, 1)
)
conf <- list(ingestion = list(kefs = list(koboform = list(asset_id_v2 = "a"))))

api <- format_api_kefs(kefs, fish, conf)

test_that("each measured length is a row, and no weight or value is lost", {
  expect_equal(sum(api$catch_kg), sum(kefs$sample_weight))
  expect_equal(sum(api$catch_price), sum(kefs$sample_weight * kefs$sample_price))
  first <- api[api$n_catch == 1, ]
  expect_equal(first$length_cm, c(20, 30))
  expect_equal(first$n_fish, c(1L, 2L))
  # 6 kg shared by the fish's own weights, 1 : (2 + 3)
  expect_equal(first$catch_kg, c(1, 5))
})

test_that("a missing fish weight shares the row by number of fish", {
  expect_equal(api$catch_kg[api$n_catch == 4], c(2, 2))
})

test_that("rows without measured fish keep their weight and no length", {
  rest <- api[api$n_catch %in% 2:3, ]
  expect_equal(rest$catch_kg, c(2, 1))
  expect_true(all(is.na(rest$length_cm)))
  expect_true(all(is.na(rest$n_fish)))
})
