library(peskas.timor.data.pipeline)

logger::log_threshold(logger::ERROR)
# Local runs read their credentials from `.env`; CI supplies them as real
# environment variables, where this is a no-op. (Until migration Phase 5 this
# was a `setwd("../..")`, which never worked from an installed package —
# tinytest sets the working directory to the test file's own directory, so
# `../..` landed inside the R library.)
if (file.exists(".env")) dotenv::load_dot_env()
conf <- peskas.timor.data.pipeline::read_config()

validated_landings <- peskas.timor.data.pipeline:::get_validated_landings(conf)
metadata <- peskas.timor.data.pipeline:::get_preprocessed_sheets(conf)
assets <- peskas.timor.data.pipeline::get_assets(conf)

# Function to check if there are negative values in a vector
any_negative <- . %>% magrittr::is_less_than(0) %>% any() %>% isTRUE()

catch <- validated_landings %>%
  tidyr::unnest(landing_catch) %>%
  tidyr::unnest(length_frequency)

# Landing columns ---------------------------------------------------------

# NOTE: four of these assertions named columns the validated artefact has never
# had — `trip_duration`, `landing_value`, `catch_purpose` and `individuals`, a
# schema that was never shipped. Reading a missing column returns NULL, so they
# passed vacuously (and `catch_purpose` failed outright on the empty compare)
# while warning "Unknown or uninitialised column". Pointed at the real columns
# in migration Phase 5. No assertion was dropped or weakened: `trip_length`,
# `catch_price`, `catch_use` and `number_of_fish` are the same quantities under
# the names `format_public_data()` reads.

expect_false(
  any_negative(na.omit(validated_landings$trip_length)),
  "Negative trip durations in landings")

expect_false(
  any_negative(na.omit(validated_landings$catch_price)),
  "Negative values in landings")

expect_false(
  any(na.omit(validated_landings$landing_date) > (lubridate::with_tz(Sys.Date() + 1, "Asia/Dili"))),
  "Landing dates larger than current date + 1")

expect_false(
  any(na.omit(validated_landings$landing_date) <
        lubridate::with_tz("2017-01-01", "Asia/Dili")),
  "Landing dates prior to 2017")

expect_true(
  all(nchar(na.omit(validated_landings$tracker_imei)) == 15),
  "IMEIs of not 15 characters"
)

# Catch columns -----------------------------------------------------------

expect_false(
  any_negative(na.omit(catch$length)),
  "Negative catch lengths")

expect_equal(
  sort(unique(na.omit(catch$catch_use))),
  c("both", "food", "sale"),
  info = "Catch purpose has unepected values"
)

expect_true(
  {
    landing_codes <- na.omit(catch$catch_taxon)
    valid_codes <- c(na.omit(assets$taxa$alpha3_code), "0")
    all(landing_codes %in% valid_codes)
  },
  "Catch codes has unepected values"
)

expect_false(
  any_negative(na.omit(catch$number_of_fish)),
  "Negative catch numbers")

expect_false(
  anyNA(catch$catch_taxon),
  "NA values in catch taxon")
