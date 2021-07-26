library(peskas.timor.data.pipeline)

logger::log_threshold(logger::ERROR)
# Adjust the working directory so that when running localy the authentication
# details in the config file load properly
setwd("../..")
pars <- peskas.timor.data.pipeline::read_config()

merged_trips <- peskas.timor.data.pipeline:::get_merged_trips(pars)

expect_equal(
  length(na.omit(merged_trips$landing_id)),
  dplyr::n_distinct(na.omit(merged_trips$landing_id)),
  info = "Landing ids might be duplicated"
)

expect_equal(
  length(na.omit(merged_trips$tracker_trip_id)),
  dplyr::n_distinct(na.omit(merged_trips$tracker_trip_id)),
  info = "Tracking trip ids might be duplicated"
)



