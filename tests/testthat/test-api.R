# One landing with two catches, each spread over length bins, plus a landing
# with no catch at all. `bin` values mirror the real table: 5 cm-wide bins, most
# of them empty.
bins <- tidyr::tibble(
  submission_id = c(1L, 1L, 1L, 1L, 1L, 2L),
  survey_version = "v3",
  landing_date = as.Date("2026-01-02"),
  gaul_1_code = "3089", gaul_1_name = "Dili",
  gaul_2_code = "131705", gaul_2_name = "Atauro",
  landing_site = "Beloi",
  n_fishers = 3,
  trip_duration = 4.5,
  gear = "Gill Net",
  vessel_type = "Canoes",
  catch_habitat = "Reef",
  catch_outcome = "1",
  n_catch = c(1L, 1L, 1L, 2L, 2L, 1L),
  catch_taxon = c("SDX", "SDX", "SDX", "TUN", "TUN", "0"),
  scientific_name = c(rep("Decapterus spp", 3), "Thunnini", "Thunnini", NA),
  length = c(7.5, 12.5, 17.5, 22.5, 27.5, NA),
  n_individuals = c(2, 0, 8, 1, NA, NA),
  catch_kg = c(0.5, NA, 4, 2, NA, NA),
  catch_price = c(rep(30, 5), 0)
)

conf <- list(
  ingestion = list(landings = list(v3 = list(asset_id = "asset3")))
)

api <- peskas.timor.data.pipeline:::api_trips(bins, conf)

test_that("the emitted schema is the cross-country contract", {
  expect_equal(
    names(api),
    c(
      "survey_id", "trip_id", "landing_date", "gaul_1_code", "gaul_1_name",
      "gaul_2_code", "gaul_2_name", "landing_site", "n_fishers",
      "trip_duration_hrs", "gear", "vessel_type", "catch_habitat",
      "catch_outcome", "n_catch", "catch_taxon", "scientific_name",
      "length_cm", "catch_kg", "catch_price", "tot_catch_kg", "tot_catch_price"
    )
  )
  expect_type(api$n_catch, "integer")
  expect_s3_class(api$landing_date, "Date")
  expect_equal(api$survey_id, rep("asset3", 3))
  expect_equal(api$trip_id, c("TRIP_1", "TRIP_1", "TRIP_2"))
})

test_that("length bins collapse to one row per catch without losing weight", {
  expect_equal(nrow(api), 3L)
  expect_equal(sum(api$catch_kg, na.rm = TRUE), sum(bins$catch_kg, na.rm = TRUE))
  # catch 1: 0.5 + 4, the empty bin contributing nothing
  expect_equal(api$catch_kg[which(api$catch_taxon == "SDX")], 4.5)
})

test_that("length_cm is the individuals-weighted mean of the bin midpoints", {
  # catch 1: (2 * 7.5 + 0 * 12.5 + 8 * 17.5) / 10
  expect_equal(api$length_cm[which(api$catch_taxon == "SDX")], 15.5)
  # catch 2: the 27.5 bin has no count, so only the 22.5 bin is weighed
  expect_equal(api$length_cm[which(api$catch_taxon == "TUN")], 22.5)
  # a landing with no catch has no length
  expect_true(is.na(api$length_cm[is.na(api$catch_taxon)]))
})

test_that("nothing weighable becomes NA rather than zero", {
  expect_true(is.na(api$catch_kg[is.na(api$catch_taxon)]))
  expect_true(is.na(api$tot_catch_kg[is.na(api$catch_taxon)]))
})

test_that("the landing's revenue is tot_catch_price and catch_price is NA", {
  expect_true(all(is.na(api$catch_price)))
  expect_equal(api$tot_catch_price, c(30, 30, 0))
  # tot_catch_kg is the landing's total, repeated on each of its catches
  expect_equal(api$tot_catch_kg[api$trip_id == "TRIP_1"], c(6.5, 6.5))
})

test_that("the no-catch sentinel becomes NA, as in the other countries", {
  expect_true(is.na(api$catch_taxon[api$trip_id == "TRIP_2"]))
})

test_that("nesting the validated catch ignores the columns the API added", {
  # `tidyr::nest()` groups on every column it is not nesting, so the columns
  # `rename_validated_catch()` gained for the API export would silently change
  # the artefact the portal reads. `nest_landing_catch()` selects first.
  catch <- tidyr::tibble(
    submission_id = 1L,
    n_catch = c(1L, 1L),
    catch_taxon = "SDX",
    catch_use = "food",
    length_type = NA_character_,
    length = c(7.5, 12.5),
    number_of_fish = c(2, 3),
    catch = c(0.5, 1),
    Zinc_mu = c(0.1, 0.2)
  )
  nested <- peskas.timor.data.pipeline:::nest_landing_catch(catch)

  expect_equal(
    nested,
    peskas.timor.data.pipeline:::nest_landing_catch(
      dplyr::mutate(catch, catch_outcome = c("1", "0"), scientific_name = "x")
    )
  )
  expect_equal(nrow(nested$landing_catch[[1]]), 1L)
})
