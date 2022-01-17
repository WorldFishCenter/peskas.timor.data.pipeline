library(peskas.timor.data.pipeline)

logger::log_threshold(logger::ERROR)
# Adjust the working directory so that when running localy the authentication
# details in the config file load properly
setwd("../..")
pars <- peskas.timor.data.pipeline::read_config()

validated_landings <- peskas.timor.data.pipeline:::get_validated_landings(pars)
metadata <- peskas.timor.data.pipeline:::get_preprocessed_metadata(pars)

# Function to check if there are negative values in a vector
any_negative <- . %>% magrittr::is_less_than(0) %>% any() %>% isTRUE()

catch <- validated_landings %>%
  tidyr::unnest(landing_catch) %>%
  tidyr::unnest(length_frequency)

# Landing columns ---------------------------------------------------------

expect_false(
  any_negative(na.omit(validated_landings$trip_duration)),
  "Negative trip durations in landings")

expect_false(
  any_negative(na.omit(validated_landings$landing_value)),
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
  sort(unique(na.omit(catch$catch_purpose))),
  c("both", "food", "sale"),
  info = "Catch purpose has unepected values"
)

expect_true(
  {
    landing_codes <- na.omit(catch$catch_taxon)
    valid_codes <- c(na.omit(metadata$catch_types$interagency_code), "0")
    all(landing_codes %in% valid_codes)
  },
  "Catch codes has unepected values"
)

expect_false(
  any_negative(na.omit(catch$individuals)),
  "Negative catch numbers")

expect_false(
  anyNA(catch$catch_taxon),
  "NA values in catch taxon")
