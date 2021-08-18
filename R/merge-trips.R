#'Merge trips
#'
#'Merges validated landings and the validated pds trips into one single data
#'frame. Merging is done only when there is a single landing and tracked trip
#'per day. Days are determined using the end of the tracking trip.
#'
#'The parameters needed in the config file are those required for
#'`validate_landings()`, and `validate_pds_trips()`combined, as well as
#'merged_trips.
#'
#'@param log_threshold
#'@inheritParams ingest_landings
#'@keywords workflow
#'@return no outputs. This function is used for it's side effects
#'
#'@importFrom rlang .data
#'@export
#'
merge_trips <- function(){

  pars <- read_config()
  logger::log_info("Retrieving validated landings...")
  validated_landings <- get_validated_landings(pars)
  logger::log_info("Retrieving validated pds trips...")
  validated_pds_trips <- get_validated_pds_trips(pars)

  logger::log_info("Preparing datasets...")
  landings <- validated_landings %>%
    dplyr::group_by(.data$landing_date, .data$tracker_imei) %>%
    dplyr::mutate(unique_trip_per_day = dplyr::n() == 1) %>%
    dplyr::ungroup() %>%
    split(.$unique_trip_per_day)

  pds <- validated_pds_trips %>%
    # We assume the landing date to be the same as the date when the trip ended
    dplyr::mutate(landing_date = lubridate::as_date(.data$tracker_trip_end)) %>%
    dplyr::group_by(.data$landing_date, .data$tracker_imei) %>%
    dplyr::mutate(unique_trip_per_day = dplyr::n() == 1) %>%
    dplyr::ungroup() %>%
    split(.$unique_trip_per_day)

  logger::log_info("Merging datasets datasets...")
  # Only join when we have one landing and one tracking per day, otherwise we
  # cannot do guarantee that the landing corresponds to a trip
  merged_trips <- dplyr::full_join(
    landings$`TRUE`,
    pds$`TRUE`,
    by = c("landing_date", "tracker_imei", "unique_trip_per_day")) %>%
    dplyr::bind_rows(landings$`FALSE`) %>%
    dplyr::bind_rows(pds$`FALSE`) %>%
    dplyr::select(-.data$unique_trip_per_day)

  merged_trips_filename <- pars$merged_trips$file_prefix %>%
    add_version(extension = "rds")

  readr::write_rds(x = merged_trips,
                   file = merged_trips_filename,
                   compress = "gz")
  logger::log_info("Uploading {merged_trips_filename} to cloud storage")
  upload_cloud_file(file = merged_trips_filename,
                    provider = pars$storage$google$key,
                    options = pars$storage$google$options)
}
