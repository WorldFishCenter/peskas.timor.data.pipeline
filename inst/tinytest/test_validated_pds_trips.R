library(peskas.timor.data.pipeline)

logger::log_threshold(logger::ERROR)
# Adjust the working directory so that when running localy the authentication
# details in the config file load properly
setwd("../..")
pars <- peskas.timor.data.pipeline::read_config()

pds_trips <- peskas.timor.data.pipeline:::get_validated_pds_trips(pars)

trips <-
  pds_trips %>%
  dplyr::mutate(duration = difftime(pds_trips$tracker_trip_end, pds_trips$tracker_trip_start, units = "sec"))

expect_true(
  all(na.omit(pds_trips$tracker_trip_start) < Sys.time()),
  "Trip starts are in the future")

expect_true(
  all(na.omit(pds_trips$tracker_trip_end) < Sys.time()),
  "Trip ends are in the future")

expect_true(
  all(na.omit(pds_trips$tracker_trip_start) > lubridate::ymd("2017-01-01")),
  "Trip starts are prior to 2017")

expect_true(
  all(na.omit(pds_trips$tracker_trip_end) > lubridate::ymd("2017-01-01")),
  "Trip ends are prior to 2017")

expect_true(
  all(na.omit(pds_trips$tracker_trip_start < pds_trips$tracker_trip_end)),
  "Trip start time is after trip end"
)

expect_true(
  all(na.omit(as.numeric(trips$duration) == trips$tracker_trip_duration)),
  "Indicated and calculated trip duration differ"
)

# Skipping overlap test for now as it takes way too long
# do_intervals_overlap <- function(x){
#   if (length(x) <= 1) return(FALSE)
#   x %>% combn(2, simplify = F) %>%
#     purrr::map_lgl(~ lubridate::int_overlaps(.[1], .[2])) %>%
#     any()
# }
#
# interval_overlaps <- trips %>%
#   dplyr::filter(!is.na(tracker_trip_start)) %>%
#   dplyr::mutate(week = lubridate::floor_date(tracker_trip_start, "day")) %>%
#   dplyr::group_by(tracker_imei, week) %>%
#   dplyr::summarise(overlap = do_intervals_overlap(interval))
#
# expect_false(
#   any(interval_overlaps$overlap),
#   "Trips from a same IMEI overlap in time"
# )

expect_true(
  all(na.omit(trips$tracker_last_seen > trips$tracker_trip_start)) &
    all(na.omit(trips$tracker_last_seen > trips$tracker_trip_end)),
  "The last seen date-time is earlier than the trip start or end time"
)
