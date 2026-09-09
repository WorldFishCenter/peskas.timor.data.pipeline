library(peskas.timor.data.pipeline)

logger::log_threshold(logger::ERROR)
# Local runs read their credentials from `.env`; CI supplies them as real
# environment variables, where this is a no-op. Not a `setwd("../..")`:
# tinytest sets the working directory to the test file's own directory, so
# that lands inside the R library.
if (file.exists(".env")) dotenv::load_dot_env()
conf <- peskas.timor.data.pipeline::read_config()

validated_landings <- peskas.timor.data.pipeline:::get_validated_landings(conf)
assets <- peskas.timor.data.pipeline::get_assets(conf)

# Submissions carrying alert 4, "landing date after submission date". The alert
# string is a hyphen-separated list, so split it rather than matching "4" inside
# "14" or "24".
flagged_dates <-
  coasts::download_parquet_from_cloud(
    prefix = conf$surveys$landings$validation$flags$file_prefix,
    provider = conf$storage$google$key,
    options = coasts::resolve_storage_opts(conf, "country")
  ) |>
  subset(vapply(strsplit(alert, "-"), function(x) "4" %in% x, logical(1))) |>
  getElement("submission_id") |>
  as.integer()

# Function to check if there are negative values in a vector
any_negative <- . %>% magrittr::is_less_than(0) %>% any() %>% isTRUE()

catch <- validated_landings %>%
  tidyr::unnest(landing_catch) %>%
  tidyr::unnest(length_frequency)

# Landing columns ---------------------------------------------------------

# Name the columns the artefact actually has: reading a missing one returns
# NULL, so an assertion over it passes vacuously while warning "Unknown or
# uninitialised column".

expect_false(
  any_negative(na.omit(validated_landings$trip_length)),
  "Negative trip durations in landings")

expect_false(
  any_negative(na.omit(validated_landings$catch_price)),
  "Negative values in landings")

# A landing date after the submission date is flagged (alert 4) and kept, not
# blanked: `landing_date` is the merge key and every time aggregation reads it,
# so dropping the row would hide a correctable typo. The guarantee is therefore
# that no future date escapes *unflagged*, which is what this asserts.
expect_false(
  any(
    !is.na(validated_landings$landing_date) &
      validated_landings$landing_date >
        as.Date(lubridate::with_tz(Sys.time(), "Asia/Dili")) + 1 &
      !validated_landings$landing_id %in% flagged_dates
  ),
  "Unflagged landing dates larger than current date + 1")

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
